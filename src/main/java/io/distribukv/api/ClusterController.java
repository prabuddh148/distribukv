package io.distribukv.api;

import io.distribukv.cluster.FaultInjector;
import io.distribukv.cluster.HintedHandoff;
import io.distribukv.cluster.Membership;
import io.distribukv.cluster.NodeClient;
import io.distribukv.config.ClusterProperties;
import io.distribukv.coordinator.Coordinator;
import io.distribukv.replication.ReplicationLogConsumer;
import io.distribukv.ring.ConsistentHashRing;
import io.distribukv.storage.StorageEngine;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Cluster introspection used by the dashboard, scripts and tests. */
@RestController
public class ClusterController {

    private final ClusterProperties props;
    private final ConsistentHashRing ring;
    private final Membership membership;
    private final NodeClient client;
    private final StorageEngine storage;
    private final HintedHandoff hints;
    private final FaultInjector faults;
    private final Coordinator coordinator;
    private final Optional<ReplicationLogConsumer> logConsumer;

    public ClusterController(ClusterProperties props, ConsistentHashRing ring, Membership membership,
                             NodeClient client, StorageEngine storage, HintedHandoff hints,
                             FaultInjector faults, Coordinator coordinator,
                             Optional<ReplicationLogConsumer> logConsumer) {
        this.props = props;
        this.ring = ring;
        this.membership = membership;
        this.client = client;
        this.storage = storage;
        this.hints = hints;
        this.faults = faults;
        this.coordinator = coordinator;
        this.logConsumer = logConsumer;
    }

    /** This node's own counters. Reachable even while the node is isolated. */
    @GetMapping("/cluster/local")
    public Map<String, Object> local() {
        Map<String, Object> local = new LinkedHashMap<>();
        local.put("nodeId", props.nodeId());
        long keys;
        try {
            keys = storage.liveKeyCount();
        } catch (RuntimeException e) {
            keys = -1; // storage unavailable (e.g. MySQL down)
        }
        local.put("keys", keys);
        local.put("hintsPending", hints.pending());
        local.put("isolated", faults.isIsolated());
        local.put("replicationLagMs", logConsumer.map(ReplicationLogConsumer::lastLagMs).orElse(null));
        return local;
    }

    /** Cluster view from this node: failure-detector state plus each node's counters. */
    @GetMapping("/cluster/status")
    public Map<String, Object> status() {
        Map<String, Double> ownership = ring.primaryOwnership();
        List<Membership.MemberState> members = membership.snapshot();
        Map<String, CompletableFuture<Map<String, Object>>> stats = new LinkedHashMap<>();
        members.forEach(m -> stats.put(m.id(), m.id().equals(props.nodeId())
                ? CompletableFuture.completedFuture(local())
                : client.stats(m.url())));

        List<Map<String, Object>> nodes = members.stream().map(m -> {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", m.id());
            node.put("url", m.url());
            node.put("status", m.status());
            node.put("lastSeenMsAgo", m.lastSeenMsAgo());
            node.put("primaryOwnership", ownership.getOrDefault(m.id(), 0.0));
            try {
                node.put("stats", stats.get(m.id()).get(400, TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                node.put("stats", null);
            }
            return node;
        }).toList();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("self", props.nodeId());
        result.put("replicationFactor", props.replicationFactor());
        result.put("defaultConsistency", props.defaultConsistency());
        result.put("stack", Map.of(
                "storage", storage.type(),
                "replicationLog", props.kafka().enabled() ? "kafka:" + props.kafka().topic() : "none",
                "membership", membership.source()));
        result.put("nodes", nodes);
        return result;
    }

    @GetMapping("/cluster/ring")
    public Map<String, Object> ring(@RequestParam String key) {
        KvController.validateKey(key);
        return Map.of("key", key, "hash", ConsistentHashRing.hash(key), "replicas", coordinator.replicasFor(key));
    }

    /** Lets the dashboard partition or heal any node by proxying to that node's admin endpoint. */
    @PostMapping("/cluster/nodes/{id}/isolate")
    public Map<String, Object> isolate(@PathVariable String id, @RequestParam boolean enabled) throws Exception {
        if (id.equals(props.nodeId())) {
            faults.setIsolated(enabled);
        } else {
            client.setIsolated(membership.urlOf(id), enabled).get(2, TimeUnit.SECONDS);
        }
        return Map.of("nodeId", id, "isolated", enabled);
    }
}
