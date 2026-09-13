package io.distribukv.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.distribukv.config.ClusterProperties;
import io.distribukv.storage.VersionedValue;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Publishes every write to a Kafka topic keyed by the data key, so all versions of a key land in
 * the same partition in order. Replicas consume the topic ({@link ReplicationLogConsumer}).
 */
@Component
@ConditionalOnProperty(name = "kv.kafka.enabled", havingValue = "true")
public class KafkaReplicationLog implements ReplicationLog, DisposableBean {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final String topic;
    private final Counter appended;
    private final Counter failures;
    // KafkaTemplate.send can block (up to max.block.ms) while the broker is unreachable; keep that
    // off request threads, with a bounded queue so an outage cannot exhaust memory.
    private final ThreadPoolExecutor sender = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(10_000), Thread.ofPlatform().name("kv-kafka-sender").daemon().factory());

    public KafkaReplicationLog(KafkaTemplate<String, String> kafka, ObjectMapper mapper, ClusterProperties props,
                               MeterRegistry metrics) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.topic = props.kafka().topic();
        this.appended = Counter.builder("kv.replication.log.appended").description("Writes appended to Kafka").register(metrics);
        this.failures = Counter.builder("kv.replication.log.failures").description("Failed Kafka appends").register(metrics);
    }

    @Override
    public CompletableFuture<String> append(String key, VersionedValue value) {
        CompletableFuture<String> result = new CompletableFuture<>();
        try {
            sender.execute(() -> {
                try {
                    kafka.send(topic, key, mapper.writeValueAsString(value)).whenComplete((sent, error) -> {
                        if (error != null) {
                            failures.increment();
                            result.completeExceptionally(error);
                        } else {
                            appended.increment();
                            RecordMetadata meta = sent.getRecordMetadata();
                            result.complete("kafka:" + meta.topic() + "/" + meta.partition() + "@" + meta.offset());
                        }
                    });
                } catch (Exception e) {
                    failures.increment();
                    result.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            failures.increment();
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public void destroy() {
        sender.shutdownNow();
    }
}
