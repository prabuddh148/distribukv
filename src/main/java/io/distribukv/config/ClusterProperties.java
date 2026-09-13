package io.distribukv.config;

import io.distribukv.coordinator.Consistency;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Node configuration. {@code cluster} is the static membership list, e.g.
 * {@code node1=http://node1:8080,node2=http://node2:8080}.
 */
@ConfigurationProperties(prefix = "kv")
public record ClusterProperties(
        String nodeId,
        List<String> cluster,
        @DefaultValue("3") int replicationFactor,
        @DefaultValue("256") int virtualNodes,
        @DefaultValue("STRONG") Consistency defaultConsistency,
        @DefaultValue("./data") String dataDir,
        @DefaultValue("false") boolean fsync,
        @DefaultValue("500") long heartbeatIntervalMs,
        @DefaultValue("2000") long failureTimeoutMs,
        @DefaultValue("1000") long requestTimeoutMs,
        @DefaultValue("1000") long hintDeliveryIntervalMs,
        @DefaultValue("true") boolean chaosEnabled,
        @DefaultValue("0") long chaosAutoHealSeconds,
        @DefaultValue Storage storage,
        @DefaultValue Kafka kafka,
        @DefaultValue Redis redis,
        @DefaultValue PublicDemo publicDemo) {

    /** Local storage engine: an append-only commit log on disk, or a MySQL database per node. */
    public record Storage(
            @DefaultValue("log") String type,
            @DefaultValue("jdbc:mysql://localhost:3306/?allowPublicKeyRetrieval=true&useSSL=false") String mysqlUrl,
            @DefaultValue("root") String mysqlUsername,
            String mysqlPassword) {
    }

    /** Kafka replication log. */
    public record Kafka(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("localhost:9092") String bootstrapServers,
            @DefaultValue("kv-replication") String topic,
            @DefaultValue("6") int partitions,
            @DefaultValue("1") int topicReplicas) {
    }

    /** Redis-backed heartbeats for failure detection. */
    public record Redis(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("redis://localhost:6379") String url) {
    }

    /** Guard rails for traffic arriving through a public tunnel. */
    public record PublicDemo(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("10") int requestsPerSecond,
            @DefaultValue("4096") int maxValueBytes) {
    }

    public ClusterProperties {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("kv.node-id is required");
        }
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("kv.cluster is required");
        }
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("kv.replication-factor must be >= 1");
        }
    }

    /** Parses {@code cluster} into an ordered id -> base URL map. */
    public Map<String, String> members() {
        Map<String, String> members = new LinkedHashMap<>();
        for (String entry : cluster) {
            String trimmed = entry.trim();
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException("Invalid kv.cluster entry '" + entry + "', expected id=url");
            }
            String url = trimmed.substring(eq + 1).trim();
            members.put(trimmed.substring(0, eq).trim(), url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
        }
        if (!members.containsKey(nodeId)) {
            throw new IllegalArgumentException("kv.node-id '" + nodeId + "' is not listed in kv.cluster");
        }
        return members;
    }
}
