package io.distribukv.cluster;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.distribukv.config.ClusterProperties;
import io.distribukv.storage.VersionedValue;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Node-to-node HTTP calls. All calls are async and time out after {@code kv.request-timeout-ms}. */
@Component
public class NodeClient {

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final Duration timeout;
    private final FaultInjector faults;

    public NodeClient(ClusterProperties props, ObjectMapper mapper, FaultInjector faults) {
        this.timeout = Duration.ofMillis(props.requestTimeoutMs());
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .build();
        this.mapper = mapper;
        this.faults = faults;
    }

    public CompletableFuture<Void> put(String baseUrl, String key, VersionedValue value) {
        HttpRequest request = request(baseUrl + "/internal/kv/" + encode(key))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(toJson(value)))
                .build();
        return send(request).thenAccept(response -> requireSuccess(response));
    }

    public CompletableFuture<Optional<VersionedValue>> get(String baseUrl, String key) {
        return send(request(baseUrl + "/internal/kv/" + encode(key)).GET().build()).thenApply(response -> {
            if (response.statusCode() == 404) {
                return Optional.empty();
            }
            requireSuccess(response);
            return Optional.of(fromJson(response.body(), new TypeReference<VersionedValue>() { }));
        });
    }

    public CompletableFuture<Boolean> ping(String baseUrl) {
        return send(request(baseUrl + "/internal/ping").GET().build())
                .thenApply(response -> response.statusCode() == 200)
                .exceptionally(e -> false);
    }

    public CompletableFuture<Map<String, Object>> stats(String baseUrl) {
        return send(request(baseUrl + "/cluster/local").GET().build()).thenApply(response -> {
            requireSuccess(response);
            return fromJson(response.body(), new TypeReference<Map<String, Object>>() { });
        });
    }

    public CompletableFuture<Void> setIsolated(String baseUrl, boolean isolated) {
        // Deliberately bypasses the fault injector so an isolated node can still be healed.
        HttpRequest request = request(baseUrl + "/admin/isolate?enabled=" + isolated)
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAccept(this::requireSuccess);
    }

    private CompletableFuture<HttpResponse<String>> send(HttpRequest request) {
        if (faults.isIsolated()) {
            return CompletableFuture.failedFuture(new IOException("node is isolated (fault injection)"));
        }
        return http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, error) -> error == null
                        ? CompletableFuture.completedFuture(response)
                        : retryOnce(request, error))
                .thenCompose(future -> future);
    }

    /**
     * Retries a peer request once after a connection error. After a peer restarts, pooled keep-alive
     * connections to it are dead and the JDK client does not retry PUTs by itself. Peer operations are
     * idempotent (last-write-wins), so a single retry is safe. Timeouts are not retried (that would
     * double the wait) and neither is a refused connection (the peer is down; fail fast).
     */
    private CompletableFuture<HttpResponse<String>> retryOnce(HttpRequest request, Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        boolean retryable = cause instanceof IOException
                && !(cause instanceof HttpTimeoutException)
                && !(cause instanceof ConnectException)
                && !faults.isIsolated();
        return retryable
                ? http.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                : CompletableFuture.failedFuture(cause);
    }

    private HttpRequest.Builder request(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(timeout);
    }

    private void requireSuccess(HttpResponse<String> response) {
        if (response.statusCode() / 100 != 2) {
            throw new CompletionException(new IOException(
                    "HTTP " + response.statusCode() + " from " + response.uri()));
        }
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private <T> T fromJson(String body, TypeReference<T> type) {
        try {
            return mapper.readValue(body, type);
        } catch (JsonProcessingException e) {
            throw new CompletionException(e);
        }
    }

    private static String encode(String key) {
        return URLEncoder.encode(key, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
