package io.distribukv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs a real 4-node cluster (N=3) in one JVM, each node on its own port and data directory, and
 * exercises replication and failure handling over HTTP exactly as clients and peers use it.
 */
class ClusterIntegrationTest {

    private static final int NODES = 4;

    @TempDir
    static Path dataDir;

    private static final Map<String, Integer> ports = new LinkedHashMap<>();
    private static final Map<String, ConfigurableApplicationContext> running = new LinkedHashMap<>();
    private static final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private static final ObjectMapper json = new ObjectMapper();
    private static String clusterSpec;

    record Response(int status, JsonNode body) {
        String text(String field) {
            return body.path(field).asText();
        }
    }

    @BeforeAll
    static void startCluster() throws IOException {
        for (int i = 1; i <= NODES; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put("node" + i, socket.getLocalPort());
            }
        }
        clusterSpec = ports.entrySet().stream()
                .map(e -> e.getKey() + "=http://localhost:" + e.getValue())
                .collect(Collectors.joining(","));
        ports.keySet().forEach(ClusterIntegrationTest::start);
    }

    @AfterAll
    static void stopCluster() {
        running.values().forEach(ConfigurableApplicationContext::close);
    }

    @BeforeEach
    void allNodesHealthy() {
        ports.keySet().forEach(id -> {
            if (!running.containsKey(id)) {
                start(id);
            }
            send("POST", id, "/admin/isolate?enabled=false", null);
        });
        eventually(() -> ports.keySet().forEach(id -> {
            JsonNode nodes = send("GET", id, "/cluster/status", null).body().path("nodes");
            nodes.forEach(node -> assertThat(node.path("status").asText()).as("%s sees %s", id, node.path("id")).isEqualTo("UP"));
        }));
    }

    @Test
    void writeIsStoredOnExactlyNReplicasAndReadableFromAnyNode() {
        Response put = send("PUT", "node1", "/kv/user:42?consistency=STRONG", "alice");
        assertThat(put.status()).isEqualTo(200);
        assertThat(put.body().path("acknowledgedBy").size()).isGreaterThanOrEqualTo(2);

        List<String> replicas = replicasOf("user:42");
        assertThat(replicas).hasSize(3);
        eventually(() -> ports.keySet().forEach(id -> assertThat(send("GET", id, "/internal/kv/user:42", null).status())
                .as("local copy on %s", id).isEqualTo(replicas.contains(id) ? 200 : 404)));

        ports.keySet().forEach(id -> {
            Response get = send("GET", id, "/kv/user:42", null);
            assertThat(get.status()).isEqualTo(200);
            assertThat(get.text("value")).isEqualTo("alice");
        });
    }

    @Test
    void strongReadsAndWritesSurviveOneReplicaFailureAndHintsRepairIt() {
        String key = "cart:7";
        assertThat(send("PUT", "node1", "/kv/" + key, "v1").status()).isEqualTo(200);
        List<String> replicas = replicasOf(key);
        String victim = replicas.get(0);
        String coordinator = otherThan(victim);

        stop(victim);

        Response put = send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "v2");
        assertThat(put.status()).isEqualTo(200);
        assertThat(put.body().path("acknowledgedBy").toString()).doesNotContain(victim);
        Response get = send("GET", coordinator, "/kv/" + key + "?consistency=STRONG", null);
        assertThat(get.text("value")).isEqualTo("v2");

        start(victim);
        eventually(() -> assertThat(send("GET", victim, "/internal/kv/" + key, null).text("value")).isEqualTo("v2"));
    }

    @Test
    void restartedNodeRecoversDataFromItsCommitLog() {
        String key = "session:abc";
        assertThat(send("PUT", "node2", "/kv/" + key, "persisted").status()).isEqualTo(200);
        String replica = replicasOf(key).get(1);
        eventually(() -> assertThat(send("GET", replica, "/internal/kv/" + key, null).status()).isEqualTo(200));

        stop(replica);
        start(replica);

        // Checked immediately after startup, before any peer could have re-sent the value.
        assertThat(send("GET", replica, "/internal/kv/" + key, null).text("value")).isEqualTo("persisted");
    }

    @Test
    void strongWritesFailWithoutQuorumWhileEventualWritesSucceed() {
        String key = "order:9";
        List<String> replicas = replicasOf(key);
        String coordinator = ports.keySet().stream().filter(id -> !replicas.contains(id)).findFirst().orElseThrow();

        stop(replicas.get(0));
        stop(replicas.get(1));

        Response strong = send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "strong");
        assertThat(strong.status()).isEqualTo(503);
        assertThat(strong.text("error")).isEqualTo("quorum_not_reached");

        Response eventual = send("PUT", coordinator, "/kv/" + key + "?consistency=EVENTUAL", "eventual");
        assertThat(eventual.status()).isEqualTo(200);
        assertThat(send("GET", coordinator, "/kv/" + key + "?consistency=EVENTUAL", null).text("value"))
                .isEqualTo("eventual");
        assertThat(send("GET", coordinator, "/kv/" + key + "?consistency=STRONG", null).status()).isEqualTo(503);

        start(replicas.get(0));
        start(replicas.get(1));
        eventually(() -> replicas.forEach(id ->
                assertThat(send("GET", id, "/internal/kv/" + key, null).text("value")).as(id).isEqualTo("eventual")));
    }

    @Test
    void readRepairUpdatesAStaleReplica() {
        String key = "profile:1";
        assertThat(send("PUT", "node1", "/kv/" + key, "old").status()).isEqualTo(200);
        List<String> replicas = replicasOf(key);
        eventually(() -> replicas.forEach(id ->
                assertThat(send("GET", id, "/internal/kv/" + key, null).text("value")).isEqualTo("old")));

        // Bypass the coordinator: only two replicas receive the newer version.
        String newer = "{\"value\":\"new\",\"timestamp\":" + (System.currentTimeMillis() + 1000)
                + ",\"nodeId\":\"test\",\"tombstone\":false}";
        send("PUT", replicas.get(0), "/internal/kv/" + key, newer);
        send("PUT", replicas.get(1), "/internal/kv/" + key, newer);
        String stale = replicas.get(2);

        assertThat(send("GET", stale, "/kv/" + key + "?consistency=STRONG", null).text("value")).isEqualTo("new");
        eventually(() -> assertThat(send("GET", stale, "/internal/kv/" + key, null).text("value")).isEqualTo("new"));
    }

    @Test
    void isolatedNodeIsRoutedAroundAndCatchesUpAfterHealing() {
        String key = "partition:1";
        List<String> replicas = replicasOf(key);
        String isolated = replicas.get(0);
        String coordinator = otherThan(isolated);

        send("POST", coordinator, "/cluster/nodes/" + isolated + "/isolate?enabled=true", null);
        assertThat(send("GET", isolated, "/kv/" + key, null).status()).isEqualTo(503);

        assertThat(send("PUT", coordinator, "/kv/" + key + "?consistency=STRONG", "during-partition").status())
                .isEqualTo(200);

        send("POST", coordinator, "/cluster/nodes/" + isolated + "/isolate?enabled=false", null);
        eventually(() -> assertThat(send("GET", isolated, "/internal/kv/" + key, null).text("value"))
                .isEqualTo("during-partition"));
    }

    @Test
    void deleteHidesTheKeyEverywhere() {
        String key = "temp:1";
        send("PUT", "node3", "/kv/" + key, "bye");
        assertThat(send("DELETE", "node4", "/kv/" + key, null).status()).isEqualTo(200);
        ports.keySet().forEach(id -> assertThat(send("GET", id, "/kv/" + key, null).status()).isEqualTo(404));
    }

    @Test
    void rejectsInvalidKeysAndConsistency() {
        assertThat(send("PUT", "node1", "/kv/bad%20key", "x").status()).isEqualTo(400);
        assertThat(send("GET", "node1", "/kv/ok?consistency=MAYBE", null).status()).isEqualTo(400);
    }

    // --- helpers -------------------------------------------------------------------------------

    private static void start(String id) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(DistribuKvApplication.class).run(
                "--server.port=" + ports.get(id),
                "--kv.node-id=" + id,
                "--kv.cluster=" + clusterSpec,
                "--kv.data-dir=" + dataDir,
                "--kv.heartbeat-interval-ms=100",
                "--kv.failure-timeout-ms=600",
                "--kv.request-timeout-ms=500",
                "--kv.hint-delivery-interval-ms=200",
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN",
                "--logging.level.io.distribukv=INFO");
        running.put(id, context);
    }

    private static void stop(String id) {
        running.remove(id).close();
    }

    private static List<String> replicasOf(String key) {
        List<String> replicas = new ArrayList<>();
        send("GET", "node1", "/cluster/ring?key=" + key, null).body().path("replicas").forEach(n -> replicas.add(n.asText()));
        return replicas;
    }

    private static String otherThan(String id) {
        return ports.keySet().stream().filter(n -> !n.equals(id)).findFirst().orElseThrow();
    }

    private static Response send(String method, String node, String path, String body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + ports.get(node) + path))
                .timeout(Duration.ofSeconds(5))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", body.startsWith("{") ? "application/json" : "text/plain");
        }
        try {
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            String text = response.body();
            return new Response(response.statusCode(), text == null || text.isBlank() || !text.startsWith("{")
                    ? json.createObjectNode() : json.readTree(text));
        } catch (IOException e) {
            return new Response(-1, json.createObjectNode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void eventually(Runnable assertion) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (true) {
            try {
                assertion.run();
                return;
            } catch (AssertionError e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }
}
