package io.distribukv.cluster;

import io.distribukv.config.ClusterProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

@Configuration
@ConditionalOnProperty(name = "kv.redis.enabled", havingValue = "true")
public class RedisConfig {

    @Bean
    public LettuceConnectionFactory redisConnectionFactory(ClusterProperties props) {
        LettuceClientConfiguration client = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(500)) // heartbeats must never stall the scheduler
                .build();
        return new LettuceConnectionFactory(LettuceConnectionFactory.createRedisConfiguration(props.redis().url()), client);
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
