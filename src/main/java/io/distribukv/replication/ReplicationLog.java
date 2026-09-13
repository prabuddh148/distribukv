package io.distribukv.replication;

import io.distribukv.storage.VersionedValue;

import java.util.concurrent.CompletableFuture;

/** An ordered, durable log of writes that replicas consume to catch up. */
public interface ReplicationLog {

    /**
     * Appends a write without blocking the caller.
     *
     * @return completes with the log position once the log has durably stored the write
     */
    CompletableFuture<String> append(String key, VersionedValue value);
}
