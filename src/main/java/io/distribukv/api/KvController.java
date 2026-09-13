package io.distribukv.api;

import io.distribukv.config.ClusterProperties;
import io.distribukv.coordinator.Consistency;
import io.distribukv.coordinator.Coordinator;
import io.distribukv.coordinator.Coordinator.ReadResult;
import io.distribukv.coordinator.Coordinator.WriteResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.regex.Pattern;

/** Client API. Any node accepts any key and coordinates the request across the key's replicas. */
@RestController
@RequestMapping("/kv")
public class KvController {

    static final Pattern KEY = Pattern.compile("[A-Za-z0-9._:-]{1,256}");
    private static final int MAX_VALUE_BYTES = 1024 * 1024;

    private final Coordinator coordinator;
    private final ClusterProperties props;

    public KvController(Coordinator coordinator, ClusterProperties props) {
        this.coordinator = coordinator;
        this.props = props;
    }

    @PutMapping("/{key}")
    public WriteResult put(@PathVariable String key, @RequestBody String value,
                           @RequestParam(required = false) Consistency consistency) {
        validateKey(key);
        if (value.length() > MAX_VALUE_BYTES) {
            throw new IllegalArgumentException("Value exceeds " + MAX_VALUE_BYTES + " bytes");
        }
        return coordinator.put(key, value, orDefault(consistency));
    }

    @GetMapping("/{key}")
    public ResponseEntity<ReadResult> get(@PathVariable String key,
                                          @RequestParam(required = false) Consistency consistency) {
        validateKey(key);
        ReadResult result = coordinator.get(key, orDefault(consistency));
        return ResponseEntity.status(result.found() ? HttpStatus.OK : HttpStatus.NOT_FOUND).body(result);
    }

    @DeleteMapping("/{key}")
    public WriteResult delete(@PathVariable String key, @RequestParam(required = false) Consistency consistency) {
        validateKey(key);
        return coordinator.delete(key, orDefault(consistency));
    }

    private Consistency orDefault(Consistency consistency) {
        return consistency != null ? consistency : props.defaultConsistency();
    }

    static void validateKey(String key) {
        if (!KEY.matcher(key).matches()) {
            throw new IllegalArgumentException("Keys must match " + KEY.pattern());
        }
    }
}
