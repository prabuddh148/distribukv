package io.distribukv.config;

import io.distribukv.ring.ConsistentHashRing;
import io.distribukv.storage.CommitLogStorage;
import io.distribukv.storage.HybridClock;
import io.distribukv.storage.MySqlStorage;
import io.distribukv.storage.StorageEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;

@Configuration
public class KvConfig {

    @Bean
    public ConsistentHashRing consistentHashRing(ClusterProperties props) {
        ConsistentHashRing ring = new ConsistentHashRing(props.virtualNodes());
        props.members().keySet().forEach(ring::addNode);
        return ring;
    }

    @Bean(destroyMethod = "close")
    public StorageEngine storageEngine(ClusterProperties props) throws IOException {
        ClusterProperties.Storage storage = props.storage();
        return switch (storage.type()) {
            case "log" -> new CommitLogStorage(Path.of(props.dataDir(), props.nodeId()), props.fsync());
            case "mysql" -> new MySqlStorage(storage.mysqlUrl(), storage.mysqlUsername(),
                    storage.mysqlPassword() == null ? "" : storage.mysqlPassword(), props.nodeId());
            default -> throw new IllegalArgumentException("kv.storage.type must be 'log' or 'mysql', got " + storage.type());
        };
    }

    @Bean
    public HybridClock hybridClock() {
        return new HybridClock();
    }
}
