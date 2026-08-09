package io.github.zilch404.attribution.core;

import java.io.Serializable;
import java.util.Objects;

public record DecisionDraft(
        String activationId,
        RuleKey rule,
        String engineVersion,
        AttributionResult result,
        DecisionChange change,
        EvaluationDisposition disposition,
        String supersedesDecisionId,
        int inspectedCandidates,
        int eligibleCandidates) implements Serializable {

    public DecisionDraft {
        if (activationId == null || activationId.isBlank()) {
            throw new IllegalArgumentException("activationId must not be blank");
        }
        rule = Objects.requireNonNull(rule, "rule");
        if (engineVersion == null || engineVersion.isBlank()) {
            throw new IllegalArgumentException("engineVersion must not be blank");
        }
        result = Objects.requireNonNull(result, "result");
        change = Objects.requireNonNull(change, "change");
        disposition = Objects.requireNonNull(disposition, "disposition");
        if (inspectedCandidates < 0 || eligibleCandidates < 0) {
            throw new IllegalArgumentException("candidate counts must not be negative");
        }
        if (eligibleCandidates > inspectedCandidates) {
            throw new IllegalArgumentException("eligibleCandidates cannot exceed inspectedCandidates");
        }
        if (disposition == EvaluationDisposition.MATERIALIZE
                && result.outcome() == AttributionOutcome.UNKNOWN) {
            throw new IllegalArgumentException("UNKNOWN results cannot be materialized");
        }
        if (disposition != EvaluationDisposition.MATERIALIZE) {
            if (change != DecisionChange.NO_CHANGE || supersedesDecisionId != null) {
                throw new IllegalArgumentException(
                        "non-materialized evaluations must be NO_CHANGE without supersedesDecisionId");
            }
        } else if (change == DecisionChange.INITIAL) {
            if (supersedesDecisionId != null) {
                throw new IllegalArgumentException("initial decisions cannot supersede another decision");
            }
        } else if (change == DecisionChange.NO_CHANGE
                || supersedesDecisionId == null
                || supersedesDecisionId.isBlank()) {
            throw new IllegalArgumentException(
                    "materialized updates require a change and supersedesDecisionId");
        }
    }
}
