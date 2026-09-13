package io.distribukv.ring;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent hash ring with virtual nodes.
 *
 * <p>Each physical node owns {@code virtualNodes} tokens on a 64-bit ring. A key is stored on the
 * first N distinct physical nodes found walking clockwise from the key's hash (its "preference
 * list"). Adding or removing a node only moves the key ranges adjacent to that node's tokens,
 * roughly 1/nodes of the data, instead of reshuffling everything as {@code hash % nodes} would.
 */
public final class ConsistentHashRing {

    private static final double RING_SIZE = 18446744073709551616.0; // 2^64

    private static final ThreadLocal<MessageDigest> MD5 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    });

    private final int virtualNodes;
    private final NavigableMap<Long, String> tokens = new TreeMap<>();
    private final Set<String> nodes = new LinkedHashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes < 1) {
            throw new IllegalArgumentException("virtualNodes must be >= 1");
        }
        this.virtualNodes = virtualNodes;
    }

    public void addNode(String nodeId) {
        lock.writeLock().lock();
        try {
            if (nodes.add(nodeId)) {
                for (int i = 0; i < virtualNodes; i++) {
                    tokens.put(hash(nodeId + "#" + i), nodeId);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeNode(String nodeId) {
        lock.writeLock().lock();
        try {
            if (nodes.remove(nodeId)) {
                tokens.values().removeIf(nodeId::equals);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** The first {@code n} distinct nodes clockwise from the key's position on the ring. */
    public List<String> preferenceList(String key, int n) {
        lock.readLock().lock();
        try {
            int wanted = Math.min(n, nodes.size());
            List<String> result = new ArrayList<>(wanted);
            if (wanted == 0) {
                return result;
            }
            long position = hash(key);
            collect(tokens.tailMap(position, true).values(), result, wanted);
            collect(tokens.headMap(position, false).values(), result, wanted);
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    private static void collect(Collection<String> candidates, List<String> result, int wanted) {
        for (String nodeId : candidates) {
            if (result.size() == wanted) {
                return;
            }
            if (!result.contains(nodeId)) {
                result.add(nodeId);
            }
        }
    }

    public Set<String> nodes() {
        lock.readLock().lock();
        try {
            return new LinkedHashSet<>(nodes);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Fraction of the hash space for which each node is the primary (first) owner. */
    public Map<String, Double> primaryOwnership() {
        lock.readLock().lock();
        try {
            Map<String, Double> ownership = new LinkedHashMap<>();
            nodes.forEach(node -> ownership.put(node, 0.0));
            if (tokens.size() == 1) {
                ownership.put(tokens.firstEntry().getValue(), 1.0);
                return ownership;
            }
            long previous = tokens.lastKey();
            for (Map.Entry<Long, String> token : tokens.entrySet()) {
                // A token owns the range (previous, token]; subtraction wraps modulo 2^64.
                double span = unsignedToDouble(token.getKey() - previous) / RING_SIZE;
                ownership.merge(token.getValue(), span, Double::sum);
                previous = token.getKey();
            }
            return ownership;
        } finally {
            lock.readLock().unlock();
        }
    }

    public static long hash(String value) {
        byte[] digest = MD5.get().digest(value.getBytes(StandardCharsets.UTF_8));
        long h = 0;
        for (int i = 0; i < 8; i++) {
            h = (h << 8) | (digest[i] & 0xFF);
        }
        return h;
    }

    private static double unsignedToDouble(long value) {
        return (double) (value >>> 1) * 2.0 + (value & 1);
    }
}
