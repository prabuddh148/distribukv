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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simulates a network partition around this node: while isolated, it rejects client and peer
 * traffic and fails every outgoing peer call. Admin, dashboard and actuator endpoints stay up so
 * the partition can be observed and healed.
 */
@Component
public class FaultInjector extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);

    private final AtomicBoolean isolated = new AtomicBoolean();
    private final boolean enabled;

    public FaultInjector(ClusterProperties props) {
        this.enabled = props.chaosEnabled();
    }

    public boolean isIsolated() {
        return isolated.get();
    }

    public void setIsolated(boolean value) {
        if (!enabled) {
            throw new IllegalStateException("Chaos endpoints are disabled (kv.chaos-enabled=false)");
        }
        if (isolated.getAndSet(value) != value) {
            log.warn("Fault injection: node is now {}", value ? "ISOLATED" : "reconnected");
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
