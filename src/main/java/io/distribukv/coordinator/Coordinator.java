package io.distribukv.coordinator;

import io.distribukv.cluster.HintedHandoff;
import io.distribukv.cluster.Membership;
import io.distribukv.cluster.NodeClient;
import io.distribukv.config.ClusterProperties;
import io.distribukv.replication.ReplicationLog;
import io.distribukv.ring.ConsistentHashRing;
import io.distribukv.storage.HybridClock;
import io.distribukv.storage.StorageEngine;
import io.distribukv.storage.VersionedValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Leaderless (Dynamo-style) request coordinator. Any node can coordinate any request: it finds the
 * key's N replicas on the ring, fans the request out in parallel and returns as soon as the number
 * of responses required by the consistency level has arrived.
 *
 * <p>When the Kafka replication log is enabled, EVENTUAL writes are acknowledged once Kafka has
 * durably stored them; replicas receive them by a best-effort direct push and, guaranteed, by
 * consuming the log. STRONG writes still use a synchronous quorum and are also appended to the
 * log as a durable backstop for replicas that missed them.
 */
@Service
public class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);
    private static final long LOG_APPEND_TIMEOUT_MS = 6_000;

    private final ClusterProperties props;
    private final ConsistentHashRing ring;
    private final StorageEngine storage;
    private final HybridClock clock;
    private final Membership membership;
    private final NodeClient client;
    private final HintedHandoff hints;
    private final Optional<ReplicationLog> replicationLog;
    private final MeterRegistry metrics;
    private final Counter readRepairs;
    private final Timer replicaAckLatency;

    public Coordinator(ClusterProperties props, ConsistentHashRing ring, StorageEngine storage, HybridClock clock,
                       Membership membership, NodeClient client, HintedHandoff hints,
                       Optional<ReplicationLog> replicationLog, MeterRegistry metrics) {
        this.props = props;
        this.ring = ring;
        this.storage = storage;
        this.clock = clock;
        this.membership = membership;
        this.client = client;
        this.hints = hints;
        this.replicationLog = replicationLog;
        this.metrics = metrics;
        this.readRepairs = Counter.builder("kv.read.repairs")
                .description("Stale replicas updated during reads").register(metrics);
        this.replicaAckLatency = Timer.builder("kv.replication.ack.latency")
                .description("Time for a remote replica to acknowledge a write").register(metrics);
    }

    public List<String> replicasFor(String key) {
        return ring.preferenceList(key, props.replicationFactor());
    }

    public WriteResult put(String key, String value, Consistency consistency) {
        return timed("put", consistency, () -> write(key, VersionedValue.of(value, clock.tick(), props.nodeId()), consistency));
    }

    public WriteResult delete(String key, Consistency consistency) {
        return timed("delete", consistency, () -> write(key, VersionedValue.deleted(clock.tick(), props.nodeId()), consistency));
    }

    public ReadResult get(String key, Consistency consistency) {
        return timed("get", consistency, () -> read(key, consistency));
    }

    private WriteResult write(String key, VersionedValue version, Consistency consistency) {
        List<String> replicas = replicasFor(key);
        if (replicationLog.isPresent()) {
            if (consistency == Consistency.EVENTUAL) {
                Optional<WriteResult> logged = writeThroughLog(key, version, replicas);
                if (logged.isPresent()) {
                    return logged.get();
                }
                // Kafka is unavailable: degrade to a direct W=1 write below.
            } else {
                replicationLog.get().append(key, version)
                        .exceptionally(e -> {
                            log.debug("Replication log append failed for {}: {}", key, e.toString());
                            return null;
                        });
            }
        }
        return quorumWrite(key, version, consistency, replicas);
    }

    private Optional<WriteResult> writeThroughLog(String key, VersionedValue version, List<String> replicas) {
        String position;
        try {
            position = replicationLog.orElseThrow().append(key, version).get(LOG_APPEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.warn("Replication log unavailable, falling back to direct replication: {}", e.toString());
            return Optional.empty();
        }
        List<String> applied = new CopyOnWriteArrayList<>();
        for (String replica : replicas) {
            writeToReplica(replica, key, version).whenComplete((ignored, error) -> {
                if (error == null) {
                    applied.add(replica);
                } else {
                    hints.store(replica, key, version); // fast path; the log still guarantees delivery
                }
            });
        }
        return Optional.of(new WriteResult(key, version.timestamp(), Consistency.EVENTUAL, props.nodeId(),
                replicas, List.copyOf(applied), 0, position));
    }

    private WriteResult quorumWrite(String key, VersionedValue version, Consistency consistency, List<String> replicas) {
        int required = consistency.requiredResponses(replicas.size());
        List<String> acked = new CopyOnWriteArrayList<>();
        AtomicInteger failures = new AtomicInteger();
        CompletableFuture<Void> quorum = new CompletableFuture<>();

        for (String replica : replicas) {
            long start = System.nanoTime();
            writeToReplica(replica, key, version).whenComplete((ignored, error) -> {
                if (error == null) {
                    if (!replica.equals(props.nodeId())) {
                        replicaAckLatency.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    }
                    acked.add(replica);
                    if (acked.size() >= required) {
                        quorum.complete(null);
                    }
                } else {
                    // The replica missed this write; keep it and replay it when the node is back.
                    hints.store(replica, key, version);
                    if (failures.incrementAndGet() > replicas.size() - required) {
                        quorum.completeExceptionally(new QuorumNotReachedException("write", required, acked));
                    }
                }
            });
        }

        awaitQuorum(quorum, "write", required, acked);
        return new WriteResult(key, version.timestamp(), consistency, props.nodeId(), replicas, List.copyOf(acked),
                required, null);
    }

    private CompletableFuture<Void> writeToReplica(String replica, String key, VersionedValue version) {
        if (replica.equals(props.nodeId())) {
            try {
                storage.apply(key, version);
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        if (!membership.isUp(replica)) {
            return CompletableFuture.failedFuture(new IllegalStateException(replica + " is down"));
        }
        return client.put(membership.urlOf(replica), key, version);
    }

    private ReadResult read(String key, Consistency consistency) {
        List<String> replicas = replicasFor(key);
        int required = consistency.requiredResponses(replicas.size());
        Map<String, Optional<VersionedValue>> responses = new ConcurrentHashMap<>();
        AtomicInteger failures = new AtomicInteger();
        CompletableFuture<Void> quorum = new CompletableFuture<>();
        List<CompletableFuture<?>> all = new ArrayList<>();

        for (String replica : replicas) {
            CompletableFuture<Optional<VersionedValue>> response = readFromReplica(replica, key);
            all.add(response.whenComplete((value, error) -> {
                if (error == null) {
                    responses.put(replica, value);
                    if (responses.size() >= required) {
                        quorum.complete(null);
                    }
                } else if (failures.incrementAndGet() > replicas.size() - required) {
                    quorum.completeExceptionally(
                            new QuorumNotReachedException("read", required, List.copyOf(responses.keySet())));
                }
            }));
        }

        awaitQuorum(quorum, "read", required, List.copyOf(responses.keySet()));
        Map<String, Optional<VersionedValue>> atQuorum = Map.copyOf(responses);
        VersionedValue newest = newest(atQuorum);

        // Read repair: once every replica has answered (or failed), push the newest version to stale ones.
        CompletableFuture.allOf(all.stream().map(f -> f.exceptionally(e -> null)).toArray(CompletableFuture[]::new))
                .thenRun(() -> repair(key, responses));

        return new ReadResult(key, newest == null || newest.tombstone() ? null : newest.value(),
                newest == null ? 0 : newest.timestamp(), newest != null && !newest.tombstone(),
                consistency, props.nodeId(), replicas, List.copyOf(atQuorum.keySet()), required);
    }

    private CompletableFuture<Optional<VersionedValue>> readFromReplica(String replica, String key) {
        if (replica.equals(props.nodeId())) {
            try {
                return CompletableFuture.completedFuture(storage.get(key));
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        if (!membership.isUp(replica)) {
            return CompletableFuture.failedFuture(new IllegalStateException(replica + " is down"));
        }
        return client.get(membership.urlOf(replica), key);
    }

    private void repair(String key, Map<String, Optional<VersionedValue>> responses) {
        VersionedValue newest = newest(responses);
        if (newest == null) {
            return;
        }
        responses.forEach((replica, value) -> {
            if (value.isPresent() && !newest.isNewerThan(value.get())) {
                return;
            }
            readRepairs.increment();
            log.info("Read repair: key={} replica={} -> version {}", key, replica, newest.timestamp());
            writeToReplica(replica, key, newest)
                    .exceptionally(e -> {
                        hints.store(replica, key, newest);
                        return null;
                    });
        });
    }

    private static VersionedValue newest(Map<String, Optional<VersionedValue>> responses) {
        return responses.values().stream().flatMap(Optional::stream)
                .max(Comparator.naturalOrder()).orElse(null);
    }

    private void awaitQuorum(CompletableFuture<Void> quorum, String op, int required, List<String> responded) {
        try {
            quorum.get(props.requestTimeoutMs() + 500, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof QuorumNotReachedException q) {
                throw q;
            }
            throw new IllegalStateException(e.getCause());
        } catch (TimeoutException e) {
            throw new QuorumNotReachedException(op, required, responded);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private <T> T timed(String op, Consistency consistency, Supplier<T> action) {
        Timer.Sample sample = Timer.start(metrics);
        String outcome = "ok";
        try {
            return action.get();
        } catch (QuorumNotReachedException e) {
            outcome = "quorum_failed";
            throw e;
        } catch (RuntimeException e) {
            outcome = "error";
            throw e;
        } finally {
            sample.stop(Timer.builder("kv.requests")
                    .description("Client requests handled by this coordinator")
                    .tags("op", op, "consistency", consistency.name(), "outcome", outcome)
                    .register(metrics));
        }
    }

    /**
     * @param requiredAcks   replicas that had to confirm before responding (0 when the Kafka log provided durability)
     * @param replicationLog log position of the write when it was acknowledged by the Kafka log, otherwise null
     */
    public record WriteResult(String key, long version, Consistency consistency, String coordinator,
                              List<String> replicas, List<String> acknowledgedBy, int requiredAcks,
                              String replicationLog) {
    }

    public record ReadResult(String key, String value, long version, boolean found, Consistency consistency,
                             String coordinator, List<String> replicas, List<String> respondedBy, int requiredResponses) {
    }
}
