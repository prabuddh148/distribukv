package io.distribukv.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/** A node's local replica store. Implementations must apply writes with last-write-wins. */
public interface StorageEngine extends Closeable {

    /**
     * Applies the write if it is newer than what is stored.
     *
     * @return true if the write was applied (false means a newer or equal version already exists)
     */
    boolean apply(String key, VersionedValue value);

    /** The stored version, including tombstones. */
    Optional<VersionedValue> get(String key);

    long liveKeyCount();

    /** Short name shown in the dashboard, e.g. "commit-log" or "mysql". */
    String type();

    @Override
    void close() throws IOException;
}
