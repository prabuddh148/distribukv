package io.distribukv.storage;

import java.util.Comparator;

/**
 * A value tagged with the version used for last-write-wins conflict resolution.
 * Deletes are stored as tombstones so a delete can win against an older concurrent write.
 */
public record VersionedValue(String value, long timestamp, String nodeId, boolean tombstone)
        implements Comparable<VersionedValue> {

    private static final Comparator<VersionedValue> ORDER = Comparator
            .comparingLong(VersionedValue::timestamp)
            .thenComparing(VersionedValue::nodeId); // deterministic tie-break

    public static VersionedValue of(String value, long timestamp, String nodeId) {
        return new VersionedValue(value, timestamp, nodeId, false);
    }

    public static VersionedValue deleted(long timestamp, String nodeId) {
        return new VersionedValue(null, timestamp, nodeId, true);
    }

    public boolean isNewerThan(VersionedValue other) {
        return other == null || compareTo(other) > 0;
    }

    @Override
    public int compareTo(VersionedValue other) {
        return ORDER.compare(this, other);
    }
}
