package io.distribukv.coordinator;

/** Client-selectable consistency level, mapped to how many replicas must respond. */
public enum Consistency {

    /** Majority quorum for reads and writes: R + W > N, so every read overlaps the latest write. */
    STRONG,

    /** A single replica acknowledges; the rest converge via hinted handoff and read repair. */
    EVENTUAL;

    public int requiredResponses(int replicas) {
        return this == STRONG ? replicas / 2 + 1 : 1;
    }
}
