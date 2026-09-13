package io.distribukv.cluster;

import io.distribukv.config.ClusterProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Heartbeats stored in Redis as keys with a TTL: {@code kv:alive:<nodeId>}. A node that stops
 * refreshing its key (crash, pause, partition) disappears after {@code kv.failure-timeout-ms}.
 * One round trip tells a node the liveness of the whole cluster, instead of pinging every peer.
 */
@Component
@ConditionalOnProperty(name = "kv.redis.enabled", havingValue = "true")
public class RedisLiveness {

    private static final String KEY_PREFIX = "kv:alive:";

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public RedisLiveness(StringRedisTemplate redis, ClusterProperties props) {
        this.redis = redis;
        this.ttl = Duration.ofMillis(props.failureTimeoutMs());
    }

    public void heartbeat(String nodeId, String url) {
        redis.opsForValue().set(KEY_PREFIX + nodeId, url, ttl);
    }

    public Set<String> aliveAmong(Collection<String> nodeIds) {
        List<String> ids = List.copyOf(nodeIds);
        List<String> values = redis.opsForValue().multiGet(ids.stream().map(id -> KEY_PREFIX + id).toList());
        Set<String> alive = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            if (values != null && values.get(i) != null) {
                alive.add(ids.get(i));
            }
        }
        return alive;
    }
}
