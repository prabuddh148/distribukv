package io.distribukv.cluster;

import io.distribukv.storage.VersionedValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Hinted handoff: when a replica misses a write (down, isolated or timed out), the coordinator
 * keeps the write as a "hint" and replays it once the failure detector sees that replica UP again.
 * Replays are idempotent because replicas apply last-write-wins.
 */
@Component
public class HintedHandoff {

    private static final Logger log = LoggerFactory.getLogger(HintedHandoff.class);

    record Hint(String key, VersionedValue value) {
    }

    private final Map<String, Queue<Hint>> hints = new ConcurrentHashMap<>();
    private final Membership membership;
    private final NodeClient client;
    private final Counter stored;
    private final Counter delivered;

    public HintedHandoff(@Lazy Membership membership, NodeClient client, MeterRegistry metrics) {
        this.membership = membership;
        this.client = client;
        this.stored = Counter.builder("kv.hints.stored").description("Writes saved for an unavailable replica")
                .register(metrics);
        this.delivered = Counter.builder("kv.hints.delivered").description("Hints replayed to a recovered replica")
                .register(metrics);
        Gauge.builder("kv.hints.pending", this::pending).description("Hints waiting for delivery").register(metrics);
    }

    public void store(String replica, String key, VersionedValue value) {
        hints.computeIfAbsent(replica, r -> new ConcurrentLinkedQueue<>()).add(new Hint(key, value));
        stored.increment();
        log.debug("Stored hint for {}: key={}", replica, key);
    }

    @Scheduled(fixedDelayString = "${kv.hint-delivery-interval-ms:1000}")
    public void deliver() {
        hints.forEach((replica, queue) -> {
            if (queue.isEmpty() || !membership.isUp(replica)) {
                return;
            }
            int count = 0;
            Hint hint;
            while ((hint = queue.peek()) != null) {
                try {
                    client.put(membership.urlOf(replica), hint.key(), hint.value()).get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.debug("Hint delivery to {} failed, will retry: {}", replica, e.toString());
                    break;
                }
                queue.poll();
                delivered.increment();
                count++;
            }
            if (count > 0) {
                log.info("Hinted handoff: delivered {} writes to {}", count, replica);
            }
        });
    }

    public int pending() {
        return hints.values().stream().mapToInt(Queue::size).sum();
    }
}
