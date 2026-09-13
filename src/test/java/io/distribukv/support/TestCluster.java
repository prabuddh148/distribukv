package io.distribukv.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.distribukv.DistribuKvApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** A real multi-node cluster inside one JVM: each node is its own Spring context, port and data directory. */
public final class TestCluster implements AutoCloseable {

    public record Response(int status, JsonNode body) {
        public String text(String field) {
            return body.path(field).asText();
        }
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Map<String, Integer> ports = new LinkedHashMap<>();
    private final Map<String, ConfigurableApplicationContext> running = new LinkedHashMap<>();
    private final Path dataDir;
    private final Function<String, List<String>> extraArgs;
    private final String clusterSpec;

    public TestCluster(int nodes, Path dataDir, Function<String, List<String>> extraArgs) {
        this.dataDir = dataDir;
        this.extraArgs = extraArgs;
        for (int i = 1; i <= nodes; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put("node" + i, socket.getLocalPort());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        this.clusterSpec = ports.entrySet().stream()
                .map(e -> e.getKey() + "=http://localhost:" + e.getValue())
                .collect(Collectors.joining(","));
    }

    public List<String> nodeIds() {
        return List.copyOf(ports.keySet());
    }

    public void startAll() {
        ports.keySet().forEach(this::start);
    }

    public void start(String id) {
        List<String> args = new ArrayList<>(List.of(
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
                "--logging.level.io.distribukv=INFO"));
        args.addAll(extraArgs.apply(id));
        running.put(id, new SpringApplicationBuilder(DistribuKvApplication.class).run(args.toArray(String[]::new)));
    }

    public void stop(String id) {
        running.remove(id).close();
    }

    /** Restarts stopped nodes, heals partitions and waits until every node sees every node UP. */
    public void awaitHealthy() {
        ports.keySet().forEach(id -> {
            if (!running.containsKey(id)) {
                start(id);
            }
            send("POST", id, "/admin/isolate?enabled=false", null);
        });
        eventually(Duration.ofSeconds(30), () -> ports.keySet().forEach(id -> {
            JsonNode nodes = send("GET", id, "/cluster/status", null).body().path("nodes");
            assertThat(nodes.size()).as("status from %s", id).isEqualTo(ports.size());
            nodes.forEach(node -> assertThat(node.path("status").asText())
                    .as("%s sees %s", id, node.path("id")).isEqualTo("UP"));
        }));
    }

    public List<String> replicasOf(String key) {
        List<String> replicas = new ArrayList<>();
        send("GET", nodeIds().get(0), "/cluster/ring?key=" + key, null).body().path("replicas")
                .forEach(n -> replicas.add(n.asText()));
        return replicas;
    }

    public String otherThan(String id) {
        return ports.keySet().stream().filter(n -> !n.equals(id)).findFirst().orElseThrow();
    }

    public String notIn(List<String> ids) {
        return ports.keySet().stream().filter(n -> !ids.contains(n)).findFirst().orElseThrow();
    }

    public Response send(String method, String node, String path, String body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + ports.get(node) + path))
                .timeout(Duration.ofSeconds(10))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", body.startsWith("{") ? "application/json" : "text/plain");
        }
        try {
            HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
            String text = response.body();
            return new Response(response.statusCode(), text == null || !text.startsWith("{")
                    ? JSON.createObjectNode() : JSON.readTree(text));
        } catch (IOException e) {
            return new Response(-1, JSON.createObjectNode());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public static void eventually(Runnable assertion) {
        eventually(Duration.ofSeconds(15), assertion);
    }

    public static void eventually(Duration timeout, Runnable assertion) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
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

    @Override
    public void close() {
        running.values().forEach(ConfigurableApplicationContext::close);
        running.clear();
    }
}
