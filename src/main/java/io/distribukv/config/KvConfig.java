package io.distribukv.config;

import io.distribukv.ring.ConsistentHashRing;
import io.distribukv.storage.HybridClock;
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
        return new StorageEngine(Path.of(props.dataDir(), props.nodeId()), props.fsync());
    }

    @Bean
    public HybridClock hybridClock() {
        return new HybridClock();
    }
}
