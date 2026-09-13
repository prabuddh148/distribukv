package io.distribukv.cluster;

import io.distribukv.config.ClusterProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static membership plus heartbeat-based failure detection.
 *
 * <p>With Redis enabled, nodes publish TTL heartbeat keys and read everyone's liveness in one
 * round trip; a missing key means DOWN. Without Redis (or while Redis is unreachable, so Redis is
 * never a single point of failure) every node pings every peer over HTTP and marks a peer DOWN
 * after {@code kv.failure-timeout-ms} of silence. Coordinators skip DOWN replicas (the write
 * becomes a hint) instead of waiting for a timeout on every request.
 */
@Component
public class Membership {

    private static final Logger log = LoggerFactory.getLogger(Membership.class);

    public enum Status { UP, DOWN }

    public record MemberState(String id, String url, Status status, long lastSeenMsAgo) {
    }

    private final String selfId;
    private final Map<String, String> members;
    private final List<String> peers;
    private final long failureTimeoutMs;
    private final NodeClient client;
    private final Optional<RedisLiveness> redis;
    private final FaultInjector faults;
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private final Map<String, Status> status = new ConcurrentHashMap<>();
    private volatile String source = "http";

    public Membership(ClusterProperties props, NodeClient client, Optional<RedisLiveness> redis,
                      FaultInjector faults, MeterRegistry metrics) {
        this.selfId = props.nodeId();
        this.members = props.members();
        this.peers = members.keySet().stream().filter(id -> !id.equals(selfId)).toList();
        this.failureTimeoutMs = props.failureTimeoutMs();
        this.client = client;
        this.redis = redis;
        this.faults = faults;
        long now = System.currentTimeMillis();
        members.keySet().forEach(id -> {
            lastSeen.put(id, now);
            status.put(id, Status.UP); // optimistic until the first heartbeats come back
        });
        Gauge.builder("kv.cluster.nodes.up", () -> status.values().stream().filter(s -> s == Status.UP).count())
                .description("Nodes this node currently considers UP").register(metrics);
    }

    @Scheduled(fixedDelayString = "${kv.heartbeat-interval-ms:500}")
    public void heartbeat() {
        if (redis.isPresent() && !faults.isIsolated() && redisHeartbeat(redis.get())) {
            return;
        }
        httpHeartbeat();
    }

    private boolean redisHeartbeat(RedisLiveness liveness) {
        try {
            liveness.heartbeat(selfId, members.get(selfId));
            Set<String> alive = liveness.aliveAmong(peers);
            peers.forEach(id -> {
                if (alive.contains(id)) {
                    markAlive(id);
                } else {
                    markDown(id, "heartbeat key expired in Redis");
                }
            });
            if (!"redis".equals(source)) {
                log.info("Failure detector: using Redis heartbeats");
                source = "redis";
            }
            return true;
        } catch (RuntimeException e) {
            if ("redis".equals(source)) {
                log.warn("Failure detector: Redis unavailable ({}), falling back to HTTP heartbeats", e.getMessage());
            }
            source = "http";
            peers.forEach(id -> lastSeen.merge(id, System.currentTimeMillis(), Math::max)); // grace period for HTTP
            return false;
        }
    }

    private void httpHeartbeat() {
        long now = System.currentTimeMillis();
        peers.forEach(id -> {
            client.ping(members.get(id)).thenAccept(alive -> {
                if (alive) {
                    markAlive(id);
                }
            });
            if (now - lastSeen.get(id) > failureTimeoutMs) {
                markDown(id, "no heartbeat for " + (now - lastSeen.get(id)) + " ms");
            }
        });
    }

    private void markAlive(String id) {
        lastSeen.put(id, System.currentTimeMillis());
        if (status.put(id, Status.UP) == Status.DOWN) {
            log.info("Failure detector: {} is back UP", id);
        }
    }

    private void markDown(String id, String reason) {
        if (status.put(id, Status.DOWN) == Status.UP) {
            log.warn("Failure detector: {} is DOWN ({})", id, reason);
        }
    }

    public boolean isUp(String id) {
        return id.equals(selfId) || status.get(id) == Status.UP;
    }

    public String urlOf(String id) {
        String url = members.get(id);
        if (url == null) {
            throw new IllegalArgumentException("Unknown node " + id);
        }
        return url;
    }

    public String selfId() {
        return selfId;
    }

    /** "redis" or "http": where liveness information currently comes from. */
    public String source() {
        return source;
    }

    public List<MemberState> snapshot() {
        long now = System.currentTimeMillis();
        return members.entrySet().stream()
                .map(e -> new MemberState(e.getKey(), e.getValue(),
                        isUp(e.getKey()) ? Status.UP : Status.DOWN,
                        e.getKey().equals(selfId) ? 0 : now - lastSeen.get(e.getKey())))
                .toList();
    }
}
