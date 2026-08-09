package io.github.zilch404.attribution.job;

import io.github.zilch404.attribution.core.AttributionDecision;
import io.github.zilch404.attribution.model.ActivationEvent;

import java.io.Serializable;
import java.util.Objects;

public record AttributionEvaluationRequest(
        ActivationEvent activation,
        AttributionDecision currentDecision) implements Serializable {

    public AttributionEvaluationRequest {
        activation = Objects.requireNonNull(activation, "activation");
        if (currentDecision != null
                && !currentDecision.activationId().equals(activation.activationId())) {
            throw new IllegalArgumentException("currentDecision belongs to another activation");
        }
    }
}
