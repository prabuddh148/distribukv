package io.distribukv.cluster;

import io.distribukv.config.ClusterProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Simulates a network partition around this node: while isolated, it rejects client and peer
 * traffic, fails every outgoing peer call, stops Redis heartbeats and pauses Kafka consumption.
 * Admin, dashboard and actuator endpoints stay up so the partition can be observed and healed.
 */
@Component
public class FaultInjector extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final AtomicBoolean isolated = new AtomicBoolean();
    private final AtomicLong generation = new AtomicLong();
    private final List<Consumer<Boolean>> listeners = new CopyOnWriteArrayList<>();
    private final boolean enabled;
    private final long autoHealSeconds;
    private final ScheduledExecutorService healer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("kv-auto-heal").daemon().factory());

    public FaultInjector(ClusterProperties props) {
        this.enabled = props.chaosEnabled();
        this.autoHealSeconds = props.chaosAutoHealSeconds();
    }

    public boolean isIsolated() {
        return isolated.get();
    }

    public void onChange(Consumer<Boolean> listener) {
        listeners.add(listener);
    }

    public void setIsolated(boolean value) {
        if (!enabled) {
            throw new IllegalStateException("Chaos endpoints are disabled (kv.chaos-enabled=false)");
        }
        if (isolated.getAndSet(value) == value) {
            return;
        }
        long current = generation.incrementAndGet();
        log.warn("Fault injection: node is now {}", value ? "ISOLATED" : "reconnected");
        listeners.forEach(listener -> listener.accept(value));
        if (value && autoHealSeconds > 0) {
            healer.schedule(() -> {
                if (generation.get() == current) {
                    setIsolated(false);
                }
            }, autoHealSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getRequestURI();
        if (isolated.get() && (path.startsWith("/internal/") || path.startsWith("/kv/"))) {
            response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Node is isolated (fault injection)");
            return;
        }
        chain.doFilter(request, response);
    }
}
