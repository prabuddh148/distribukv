package io.distribukv.api;

import io.distribukv.config.ClusterProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Guard rails for requests that arrive through a public Cloudflare tunnel (identified by the
 * {@code CF-Connecting-IP} header). Peer-to-peer and local traffic never carries that header and
 * is not affected. Public visitors may use the dashboard, the client API and the partition
 * simulator, but not node-internal or admin endpoints, and they are rate limited per IP.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnProperty(name = "kv.public-demo.enabled", havingValue = "true")
public class PublicDemoGuard extends OncePerRequestFilter {

    private final int requestsPerSecond;
    private final int maxValueBytes;
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public PublicDemoGuard(ClusterProperties props) {
        this.requestsPerSecond = props.publicDemo().requestsPerSecond();
        this.maxValueBytes = props.publicDemo().maxValueBytes();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String clientIp = request.getHeader("CF-Connecting-IP");
        if (clientIp == null) {
            chain.doFilter(request, response);
            return;
        }
        String path = request.getRequestURI();
        boolean blocked = path.startsWith("/internal/") || path.startsWith("/admin/")
                || (path.startsWith("/actuator") && !path.equals("/actuator/health") && !path.equals("/actuator/prometheus"));
        if (blocked) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Not available on the public demo");
            return;
        }
        if (buckets.size() > 10_000) {
            buckets.clear();
        }
        if (!buckets.computeIfAbsent(clientIp, ip -> new TokenBucket(requestsPerSecond)).tryAcquire()) {
            response.sendError(429, "Too many requests");
            return;
        }
        if ("PUT".equals(request.getMethod())
                && (request.getContentLengthLong() < 0 || request.getContentLengthLong() > maxValueBytes)) {
            response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
                    "Values are limited to " + maxValueBytes + " bytes on the public demo");
            return;
        }
        chain.doFilter(request, response);
    }

    private static final class TokenBucket {
        private final double ratePerMs;
        private final double capacity;
        private double tokens;
        private long last = System.currentTimeMillis();

        TokenBucket(int perSecond) {
            this.ratePerMs = perSecond / 1000.0;
            this.capacity = perSecond * 3.0;
            this.tokens = capacity;
        }

        synchronized boolean tryAcquire() {
            long now = System.currentTimeMillis();
            tokens = Math.min(capacity, tokens + (now - last) * ratePerMs);
            last = now;
            if (tokens < 1) {
                return false;
            }
            tokens -= 1;
            return true;
        }
    }
}
