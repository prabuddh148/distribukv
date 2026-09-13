package io.distribukv.replication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.distribukv.cluster.FaultInjector;
import io.distribukv.config.ClusterProperties;
import io.distribukv.ring.ConsistentHashRing;
import io.distribukv.storage.HybridClock;
import io.distribukv.storage.StorageEngine;
import io.distribukv.storage.VersionedValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies writes from the Kafka replication log to this node's storage when this node is one of
 * the key's replicas. Offsets are committed after applying, so a node that was down resumes from
 * where it stopped and catches up on every write it missed. Applying is idempotent (LWW), so
 * records that also arrived directly, via hints or via read repair are harmless.
 */
@Component
@ConditionalOnProperty(name = "kv.kafka.enabled", havingValue = "true")
public class ReplicationLogConsumer {

    static final String LISTENER_ID = "kv-replication";

    private final ConsistentHashRing ring;
    private final ClusterProperties props;
    private final StorageEngine storage;
    private final HybridClock clock;
    private final ObjectMapper mapper;
    private final KafkaListenerEndpointRegistry registry;
    private final AtomicLong lastLagMs = new AtomicLong(-1);
    private final Counter applied;
    private final Timer lag;

    public ReplicationLogConsumer(ConsistentHashRing ring, ClusterProperties props, StorageEngine storage,
                                  HybridClock clock, ObjectMapper mapper, @Lazy KafkaListenerEndpointRegistry registry,
                                  FaultInjector faults, MeterRegistry metrics) {
        this.ring = ring;
        this.props = props;
        this.storage = storage;
        this.clock = clock;
        this.mapper = mapper;
        this.registry = registry;
        this.applied = Counter.builder("kv.replication.log.applied")
                .description("Writes from the Kafka log that changed this node's data").register(metrics);
        this.lag = Timer.builder("kv.replication.log.lag")
                .description("Time between a write and this node reading it from the log").register(metrics);
        Gauge.builder("kv.replication.log.lag.last.ms", lastLagMs, AtomicLong::get)
                .description("Replication lag of the most recently consumed record").register(metrics);
        // A partitioned node must not keep receiving writes through Kafka.
        faults.onChange(this::onIsolationChange);
    }

    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = "${kv.kafka.topic:kv-replication}")
    public void onRecord(ConsumerRecord<String, String> record) throws JsonProcessingException {
        VersionedValue value = mapper.readValue(record.value(), VersionedValue.class);
        long lagMs = Math.max(0, System.currentTimeMillis() - value.timestamp());
        lastLagMs.set(lagMs);
        lag.record(lagMs, TimeUnit.MILLISECONDS);

        if (!ring.preferenceList(record.key(), props.replicationFactor()).contains(props.nodeId())) {
            return;
        }
        clock.observe(value.timestamp());
        if (storage.apply(record.key(), value)) {
            applied.increment();
        }
    }

    public long lastLagMs() {
        return lastLagMs.get();
    }

    private void onIsolationChange(boolean isolated) {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container == null) {
            return;
        }
        if (isolated) {
            container.pause();
        } else {
            container.resume();
        }
    }
}
