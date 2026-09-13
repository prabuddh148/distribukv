package io.distribukv;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import static org.assertj.core.api.Assertions.assertThat;

class PublicDemoGuardTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    static Path dataDir;

    private static ConfigurableApplicationContext node;
    private static int port;

    @BeforeAll
    static void start() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        node = new SpringApplicationBuilder(DistribuKvApplication.class).run(
                "--server.port=" + port,
                "--kv.node-id=node1",
                "--kv.cluster=node1=http://localhost:" + port,
                "--kv.data-dir=" + dataDir,
                "--kv.public-demo.enabled=true",
                "--kv.public-demo.requests-per-second=2",
                "--kv.public-demo.max-value-bytes=10",
                "--kv.public-demo.allowed-origins=https://example.github.io",
                "--spring.main.banner-mode=off",
                "--logging.level.root=WARN");
    }

    @AfterAll
    static void stop() {
        node.close();
    }

    @Test
    void peerAndLocalTrafficIsNotRestricted() throws Exception {
        assertThat(send("GET", "/internal/ping", null, null)).isEqualTo(200);
    }

    @Test
    void tunnelTrafficCannotReachInternalOrAdminEndpoints() throws Exception {
        assertThat(send("GET", "/internal/ping", "203.0.113.1", null)).isEqualTo(403);
        assertThat(send("POST", "/admin/isolate?enabled=true", "203.0.113.1", null)).isEqualTo(403);
        assertThat(send("GET", "/actuator/metrics", "203.0.113.1", null)).isEqualTo(403);
    }

    @Test
    void tunnelTrafficHasValueSizeLimit() throws Exception {
        assertThat(send("PUT", "/kv/demo:1", "203.0.113.2", "small")).isEqualTo(200);
        assertThat(send("PUT", "/kv/demo:2", "203.0.113.2", "this value is too large")).isEqualTo(413);
    }

    @Test
    void tunnelTrafficIsRateLimitedPerClientIp() throws Exception {
        int limited = 0;
        for (int i = 0; i < 20; i++) {
            if (send("GET", "/cluster/status", "203.0.113.3", null) == 429) {
                limited++;
            }
        }
        assertThat(limited).isPositive();
        assertThat(send("GET", "/cluster/status", "203.0.113.4", null)).isEqualTo(200);
    }

    @Test
    void projectPageOriginMayReadClusterStatusButOtherOriginsMayNot() throws Exception {
        HttpResponse<Void> allowed = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/cluster/status"))
                .header("Origin", "https://example.github.io").GET().build(), HttpResponse.BodyHandlers.discarding());
        assertThat(allowed.statusCode()).isEqualTo(200);
        assertThat(allowed.headers().firstValue("Access-Control-Allow-Origin")).contains("https://example.github.io");

        HttpResponse<Void> denied = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/cluster/status"))
                .header("Origin", "https://evil.example").GET().build(), HttpResponse.BodyHandlers.discarding());
        assertThat(denied.statusCode()).isEqualTo(403);
    }

    private static int send(String method, String path, String clientIp, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (clientIp != null) {
            request.header("CF-Connecting-IP", clientIp);
        }
        if (body != null) {
            request.header("Content-Type", "text/plain");
        }
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }
}
