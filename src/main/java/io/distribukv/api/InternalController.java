package io.distribukv.api;

import io.distribukv.cluster.FaultInjector;
import io.distribukv.config.ClusterProperties;
import io.distribukv.storage.HybridClock;
import io.distribukv.storage.StorageEngine;
import io.distribukv.storage.VersionedValue;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Node-to-node replica operations plus the fault-injection switch. Not meant for clients. */
@RestController
public class InternalController {

    private final StorageEngine storage;
    private final HybridClock clock;
    private final ClusterProperties props;
    private final FaultInjector faults;

    public InternalController(StorageEngine storage, HybridClock clock, ClusterProperties props, FaultInjector faults) {
        this.storage = storage;
        this.clock = clock;
        this.props = props;
        this.faults = faults;
    }

    @PutMapping("/internal/kv/{key}")
    public ResponseEntity<Void> replicaPut(@PathVariable String key, @RequestBody VersionedValue value) {
        clock.observe(value.timestamp());
        storage.apply(key, value);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/internal/kv/{key}")
    public ResponseEntity<VersionedValue> replicaGet(@PathVariable String key) {
        return ResponseEntity.of(storage.get(key));
    }

    @GetMapping("/internal/ping")
    public Map<String, String> ping() {
        return Map.of("nodeId", props.nodeId());
    }

    @PostMapping("/admin/isolate")
    public Map<String, Object> isolate(@RequestParam boolean enabled) {
        faults.setIsolated(enabled);
        return Map.of("nodeId", props.nodeId(), "isolated", faults.isIsolated());
    }
}
