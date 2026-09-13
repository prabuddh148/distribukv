package io.distribukv.storage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Wall-clock milliseconds that never go backwards and never repeat on this node, and that move
 * forward past any timestamp observed from a peer (a Lamport clock on top of physical time).
 * This bounds, but does not eliminate, the effect of clock skew on last-write-wins.
 */
public final class HybridClock {

    private final AtomicLong last = new AtomicLong();

    public long tick() {
        return last.updateAndGet(previous -> Math.max(System.currentTimeMillis(), previous + 1));
    }

    public void observe(long remoteTimestamp) {
        last.accumulateAndGet(remoteTimestamp, Math::max);
    }
}
