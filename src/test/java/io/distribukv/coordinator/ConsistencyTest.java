package io.distribukv.coordinator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConsistencyTest {

    @Test
    void strongRequiresAMajority() {
        assertThat(Consistency.STRONG.requiredResponses(1)).isEqualTo(1);
        assertThat(Consistency.STRONG.requiredResponses(3)).isEqualTo(2);
        assertThat(Consistency.STRONG.requiredResponses(5)).isEqualTo(3);
    }

    @Test
    void strongReadAndWriteQuorumsOverlap() {
        for (int n = 1; n <= 7; n++) {
            int quorum = Consistency.STRONG.requiredResponses(n);
            assertThat(quorum + quorum).as("R + W > N for N=%d", n).isGreaterThan(n);
        }
    }

    @Test
    void eventualRequiresOneReplica() {
        assertThat(Consistency.EVENTUAL.requiredResponses(3)).isEqualTo(1);
    }
}
