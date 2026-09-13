package io.distribukv.replication;

import io.distribukv.config.ClusterProperties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
@EnableKafka
@ConditionalOnProperty(name = "kv.kafka.enabled", havingValue = "true")
public class KafkaReplicationConfig {

    @Bean
    public KafkaAdmin kafkaAdmin(ClusterProperties props) {
        KafkaAdmin admin = new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers()));
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    @Bean
    public NewTopic replicationTopic(ClusterProperties props) {
        return TopicBuilder.name(props.kafka().topic())
                .partitions(props.kafka().partitions())
                .replicas(props.kafka().topicReplicas())
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory(ClusterProperties props) {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                // Fail fast when Kafka is down so requests can fall back to direct replication.
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000));
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory(ClusterProperties props) {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.kafka().bootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                // Each node is its own consumer group: every node sees every write.
                ConsumerConfig.GROUP_ID_CONFIG, "kv-" + props.nodeId(),
                // Static membership: a restarted node gets its partitions back without a rebalance wait.
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, props.nodeId(),
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
