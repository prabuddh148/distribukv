package io.distribukv.coordinator;

import java.util.List;

public class QuorumNotReachedException extends RuntimeException {

    private final int required;
    private final List<String> responded;

    public QuorumNotReachedException(String operation, int required, List<String> responded) {
        super(operation + " needed " + required + " replica responses but got " + responded.size() + " " + responded);
        this.required = required;
        this.responded = List.copyOf(responded);
    }

    public int required() {
        return required;
    }

    public List<String> responded() {
        return responded;
    }
}
