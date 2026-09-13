import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dependency-free load generator. Run against a live cluster with:
 *
 *   java bench/Bench.java --ops 5000 --concurrency 32 --urls http://localhost:8081,http://localhost:8082
 *
 * Requests are spread round-robin across the given nodes, so every node acts as a coordinator.
 */
public class Bench {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new java.util.HashMap<>(Map.of(
                "--urls", "http://localhost:8081,http://localhost:8082,http://localhost:8083,http://localhost:8084,http://localhost:8085",
                "--ops", "5000",
                "--concurrency", "32",
                "--value-bytes", "100"));
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        String[] urls = opts.get("--urls").split(",");
        int ops = Integer.parseInt(opts.get("--ops"));
        int concurrency = Integer.parseInt(opts.get("--concurrency"));
        String value = "x".repeat(Integer.parseInt(opts.get("--value-bytes")));
        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

        System.out.printf("%d nodes, %d ops per run, concurrency %d, %d-byte values%n%n",
                urls.length, ops, concurrency, value.length());
        System.out.println("| Consistency | Op  | Throughput (ops/s) | p50 (ms) | p95 (ms) | p99 (ms) | Errors |");
        System.out.println("|-------------|-----|-------------------:|---------:|---------:|---------:|-------:|");
        run(http, urls, "EVENTUAL", "PUT", ops, concurrency, value, 200); // warm-up, not printed
        for (String consistency : new String[]{"STRONG", "EVENTUAL"}) {
            for (String op : new String[]{"PUT", "GET"}) {
                System.out.println(run(http, urls, consistency, op, ops, concurrency, value, 0));
            }
        }
    }

    private static String run(HttpClient http, String[] urls, String consistency, String op, int ops,
                              int concurrency, String value, int warmupOps) throws InterruptedException {
        int total = warmupOps > 0 ? warmupOps : ops;
        long[] latencies = new long[total];
        AtomicInteger errors = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        long start = System.nanoTime();
        for (int i = 0; i < total; i++) {
            final int n = i;
            pool.submit(() -> {
                String url = urls[n % urls.length] + "/kv/bench-" + consistency + "-" + n + "?consistency=" + consistency;
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10));
                request = op.equals("PUT")
                        ? request.header("Content-Type", "text/plain").PUT(HttpRequest.BodyPublishers.ofString(value))
                        : request.GET();
                long t0 = System.nanoTime();
                try {
                    int status = http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                    if (status != 200) {
                        errors.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
                latencies[n] = System.nanoTime() - t0;
            });
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
        double seconds = (System.nanoTime() - start) / 1e9;
        Arrays.sort(latencies);
        return String.format("| %-11s | %-3s | %18.0f | %8.2f | %8.2f | %8.2f | %6d |",
                consistency, op, total / seconds, pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99),
                errors.get());
    }

    private static double pct(long[] sorted, double p) {
        return sorted[(int) Math.min(sorted.length - 1, Math.floor(p * sorted.length))] / 1e6;
    }
}
