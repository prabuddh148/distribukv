package io.distribukv;

import io.distribukv.support.TestCluster;
import io.distribukv.support.TestCluster.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static io.distribukv.support.TestCluster.eventually;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a real 4-node cluster (N=3) with the self-contained stack (commit log storage, HTTP
 * heartbeats, no Kafka) and exercises replication and failure handling over HTTP exactly as
 * clients and peers use it.
 */
class ClusterIntegrationTest {

    @TempDir
    static Path dataDir;

    private static TestCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new TestCluster(4, dataDir, id -> List.of());
        cluster.startAll();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @BeforeEach
    void allNodesHealthy() {
        cluster.awaitHealthy();
    }

    @Test
    void writeIsStoredOnExactlyNReplicasAndReadableFromAnyNode() {
        Response put = cluster.send("PUT", "node1", "/kv/user:42?consistency=STRONG", "alice");
        assertThat(put.status()).isEqualTo(200);
        assertThat(put.body().path("acknowledgedBy").size()).isGreaterThanOrEqualTo(2);

        List<String> replicas = cluster.replicasOf("user:42");
        assertThat(replicas).hasSize(3);
        eventually(() -> cluster.nodeIds().forEach(id -> assertThat(cluster.send("GET", id, "/internal/kv/user:42", null).status())
                .as("local copy on %s", id).isEqualTo(replicas.contains(id) ? 200 : 404)));

        cluster.nodeIds().forEach(id -> {
            Response get = cluster.send("GET", id, "/kv/user:42", null);
            assertThat(get.status()).isEqualTo(200);
            assertThat(get.text("value")).isEqualTo("alice");
        });
    }

    @Test
    void strongReadsAndWritesSurviveOneReplicaFailureAndHintsRepairIt() {
        String key = "cart:7";
        assertThat(cluster.send("PUT", "node1", "/kv/" + key, "v1").status()).isEqualTo(200);
        List<String> replicas = cluster.replicasOf(key);
        String victim = replicas.get(0);
        String coordinator = cluster.otherThan(victim);

        cluster.stop(victim);

        Response put = cluster.send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "v2");
        assertThat(put.status()).isEqualTo(200);
        assertThat(put.body().path("acknowledgedBy").toString()).doesNotContain(victim);
        assertThat(cluster.send("GET", coordinator, "/kv/" + key + "?consistency=STRONG", null).text("value")).isEqualTo("v2");

        cluster.start(victim);
        eventually(() -> assertThat(cluster.send("GET", victim, "/internal/kv/" + key, null).text("value")).isEqualTo("v2"));
    }

    @Test
    void restartedNodeRecoversDataFromItsCommitLog() {
        String key = "session:abc";
        assertThat(cluster.send("PUT", "node2", "/kv/" + key, "persisted").status()).isEqualTo(200);
        String replica = cluster.replicasOf(key).get(1);
        eventually(() -> assertThat(cluster.send("GET", replica, "/internal/kv/" + key, null).status()).isEqualTo(200));

        cluster.stop(replica);
        cluster.start(replica);

        // Checked immediately after startup, before any peer could have re-sent the value.
        assertThat(cluster.send("GET", replica, "/internal/kv/" + key, null).text("value")).isEqualTo("persisted");
    }

    @Test
    void strongWritesFailWithoutQuorumWhileEventualWritesSucceed() {
        String key = "order:9";
        List<String> replicas = cluster.replicasOf(key);
        String coordinator = cluster.notIn(replicas);

        cluster.stop(replicas.get(0));
        cluster.stop(replicas.get(1));

        Response strong = cluster.send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "strong");
        assertThat(strong.status()).isEqualTo(503);
        assertThat(strong.text("error")).isEqualTo("quorum_not_reached");

        Response eventual = cluster.send("PUT", coordinator, "/kv/" + key + "?consistency=EVENTUAL", "eventual");
        assertThat(eventual.status()).isEqualTo(200);
        assertThat(cluster.send("GET", coordinator, "/kv/" + key + "?consistency=EVENTUAL", null).text("value"))
                .isEqualTo("eventual");
        assertThat(cluster.send("GET", coordinator, "/kv/" + key + "?consistency=STRONG", null).status()).isEqualTo(503);

        cluster.start(replicas.get(0));
        cluster.start(replicas.get(1));
        eventually(() -> replicas.forEach(id ->
                assertThat(cluster.send("GET", id, "/internal/kv/" + key, null).text("value")).as(id).isEqualTo("eventual")));
    }

    @Test
    void readRepairUpdatesAStaleReplica() {
        String key = "profile:1";
        assertThat(cluster.send("PUT", "node1", "/kv/" + key, "old").status()).isEqualTo(200);
        List<String> replicas = cluster.replicasOf(key);
        eventually(() -> replicas.forEach(id ->
                assertThat(cluster.send("GET", id, "/internal/kv/" + key, null).text("value")).isEqualTo("old")));

        // Bypass the coordinator: only two replicas receive the newer version.
        String newer = "{\"value\":\"new\",\"timestamp\":" + (System.currentTimeMillis() + 1000)
                + ",\"nodeId\":\"test\",\"tombstone\":false}";
        cluster.send("PUT", replicas.get(0), "/internal/kv/" + key, newer);
        cluster.send("PUT", replicas.get(1), "/internal/kv/" + key, newer);
        String stale = replicas.get(2);

        assertThat(cluster.send("GET", stale, "/kv/" + key + "?consistency=STRONG", null).text("value")).isEqualTo("new");
        eventually(() -> assertThat(cluster.send("GET", stale, "/internal/kv/" + key, null).text("value")).isEqualTo("new"));
    }

    @Test
    void isolatedNodeIsRoutedAroundAndCatchesUpAfterHealing() {
        String key = "partition:1";
        String isolated = cluster.replicasOf(key).get(0);
        String coordinator = cluster.otherThan(isolated);

        cluster.send("POST", coordinator, "/cluster/nodes/" + isolated + "/isolate?enabled=true", null);
        assertThat(cluster.send("GET", isolated, "/kv/" + key, null).status()).isEqualTo(503);

        assertThat(cluster.send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "during-partition").status())
                .isEqualTo(200);

        cluster.send("POST", coordinator, "/cluster/nodes/" + isolated + "/isolate?enabled=false", null);
        eventually(() -> assertThat(cluster.send("GET", isolated, "/internal/kv/" + key, null).text("value"))
                .isEqualTo("during-partition"));
    }

    @Test
    void deleteHidesTheKeyEverywhere() {
        String key = "temp:1";
        cluster.send("PUT", "node3", "/kv/" + key, "bye");
        assertThat(cluster.send("DELETE", "node4", "/kv/" + key, null).status()).isEqualTo(200);
        cluster.nodeIds().forEach(id -> assertThat(cluster.send("GET", id, "/kv/" + key, null).status()).isEqualTo(404));
    }

    @Test
    void rejectsInvalidKeysAndConsistency() {
        assertThat(cluster.send("PUT", "node1", "/kv/bad%20key", "x").status()).isEqualTo(400);
        assertThat(cluster.send("GET", "node1", "/kv/ok?consistency=MAYBE", null).status()).isEqualTo(400);
    }
}
