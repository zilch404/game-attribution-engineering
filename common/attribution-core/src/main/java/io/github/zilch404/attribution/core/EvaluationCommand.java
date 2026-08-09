package io.github.zilch404.attribution.core;

import io.github.zilch404.attribution.model.ActivationEvent;

import java.util.Objects;

public record EvaluationCommand(
        ActivationEvent activation,
        CandidateSnapshot candidates,
        AttributionRule rule,
        AttributionDecision currentDecision) {

    public EvaluationCommand {
        activation = Objects.requireNonNull(activation, "activation");
        candidates = Objects.requireNonNull(candidates, "candidates");
        rule = Objects.requireNonNull(rule, "rule");
        if (currentDecision != null
                && !currentDecision.activationId().equals(activation.activationId())) {
            throw new IllegalArgumentException("currentDecision belongs to another activation");
        }
    }
}
