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
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static membership plus heartbeat-based failure detection. Every node pings every peer each
 * {@code kv.heartbeat-interval-ms}; a peer that has not answered for {@code kv.failure-timeout-ms}
 * is marked DOWN. Coordinators skip DOWN replicas (the write becomes a hint) instead of waiting
 * for a timeout on every request.
 */
@Component
public class Membership {

    private static final Logger log = LoggerFactory.getLogger(Membership.class);

    public enum Status { UP, DOWN }

    public record MemberState(String id, String url, Status status, long lastSeenMsAgo) {
    }

    private final String selfId;
    private final Map<String, String> members;
    private final long failureTimeoutMs;
    private final NodeClient client;
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private final Map<String, Status> status = new ConcurrentHashMap<>();

    public Membership(ClusterProperties props, NodeClient client, MeterRegistry metrics) {
        this.selfId = props.nodeId();
        this.members = props.members();
        this.failureTimeoutMs = props.failureTimeoutMs();
        this.client = client;
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
        long now = System.currentTimeMillis();
        members.forEach((id, url) -> {
            if (id.equals(selfId)) {
                return;
            }
            client.ping(url).thenAccept(alive -> {
                if (alive) {
                    markAlive(id);
                }
            });
            if (status.get(id) == Status.UP && now - lastSeen.get(id) > failureTimeoutMs) {
                status.put(id, Status.DOWN);
                log.warn("Failure detector: {} is DOWN (no heartbeat for {} ms)", id, now - lastSeen.get(id));
            }
        });
    }

    private void markAlive(String id) {
        lastSeen.put(id, System.currentTimeMillis());
        if (status.put(id, Status.UP) == Status.DOWN) {
            log.info("Failure detector: {} is back UP", id);
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

    public List<MemberState> snapshot() {
        long now = System.currentTimeMillis();
        return members.entrySet().stream()
                .map(e -> new MemberState(e.getKey(), e.getValue(),
                        isUp(e.getKey()) ? Status.UP : Status.DOWN,
                        e.getKey().equals(selfId) ? 0 : now - lastSeen.get(e.getKey())))
                .toList();
    }
}
