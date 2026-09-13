package io.distribukv.ring;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ConsistentHashRingTest {

    private static final int KEYS = 50_000;

    private static ConsistentHashRing ringOf(int nodes) {
        ConsistentHashRing ring = new ConsistentHashRing(256);
        for (int i = 1; i <= nodes; i++) {
            ring.addNode("node" + i);
        }
        return ring;
    }

    @Test
    void preferenceListContainsDistinctNodes() {
        ConsistentHashRing ring = ringOf(5);
        for (int i = 0; i < 1_000; i++) {
            List<String> replicas = ring.preferenceList("key-" + i, 3);
            assertThat(replicas).hasSize(3).doesNotHaveDuplicates();
        }
    }

    @Test
    void preferenceListIsCappedAtClusterSize() {
        assertThat(ringOf(2).preferenceList("k", 3)).hasSize(2);
        assertThat(new ConsistentHashRing(10).preferenceList("k", 3)).isEmpty();
    }

    @Test
    void placementIsDeterministic() {
        assertThat(ringOf(5).preferenceList("user:42", 3)).isEqualTo(ringOf(5).preferenceList("user:42", 3));
    }

    @Test
    void keysAreSpreadEvenlyAcrossNodes() {
        ConsistentHashRing ring = ringOf(5);
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            counts.merge(ring.preferenceList("key-" + i, 1).get(0), 1, Integer::sum);
        }
        double expected = KEYS / 5.0;
        assertThat(counts).hasSize(5);
        counts.values().forEach(count -> assertThat((double) count).isCloseTo(expected, within(expected * 0.3)));
    }

    @Test
    void addingANodeMovesOnlyItsShareOfKeys() {
        ConsistentHashRing ring = ringOf(4);
        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            before.put("key-" + i, ring.preferenceList("key-" + i, 1).get(0));
        }

        ring.addNode("node5");

        int moved = 0;
        for (Map.Entry<String, String> entry : before.entrySet()) {
            String owner = ring.preferenceList(entry.getKey(), 1).get(0);
            if (!owner.equals(entry.getValue())) {
                moved++;
                assertThat(owner).as("keys only move to the new node").isEqualTo("node5");
            }
        }
        // Ideal is 1/5 = 20%; hash % N would move ~80%.
        assertThat(moved / (double) KEYS).isBetween(0.12, 0.28);
    }

    @Test
    void removingANodeOnlyReassignsItsKeys() {
        ConsistentHashRing ring = ringOf(5);
        Map<String, String> before = new HashMap<>();
        for (int i = 0; i < KEYS; i++) {
            before.put("key-" + i, ring.preferenceList("key-" + i, 1).get(0));
        }

        ring.removeNode("node3");

        before.forEach((key, owner) -> {
            if (!owner.equals("node3")) {
                assertThat(ring.preferenceList(key, 1).get(0)).isEqualTo(owner);
            }
        });
        assertThat(ring.nodes()).doesNotContain("node3");
    }

    @Test
    void ownershipCoversTheWholeRing() {
        Map<String, Double> ownership = ringOf(5).primaryOwnership();
        assertThat(ownership.values().stream().mapToDouble(Double::doubleValue).sum()).isCloseTo(1.0, within(1e-9));
        ownership.values().forEach(share -> assertThat(share).isBetween(0.12, 0.28));
        assertThat(ringOf(1).primaryOwnership().get("node1")).isCloseTo(1.0, within(1e-9));
    }
}
