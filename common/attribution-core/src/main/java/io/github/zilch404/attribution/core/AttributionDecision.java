package io.github.zilch404.attribution.core;

import java.io.Serializable;
import java.util.Objects;

public record AttributionDecision(
        String decisionId,
        String activationId,
        long decisionVersion,
        RuleKey rule,
        String engineVersion,
        AttributionResult result) implements Serializable {

    public AttributionDecision {
        if (decisionId == null || decisionId.isBlank()) {
            throw new IllegalArgumentException("decisionId must not be blank");
        }
        if (activationId == null || activationId.isBlank()) {
            throw new IllegalArgumentException("activationId must not be blank");
        }
        if (decisionVersion < 1) {
            throw new IllegalArgumentException("decisionVersion must be positive");
        }
        rule = Objects.requireNonNull(rule, "rule");
        if (engineVersion == null || engineVersion.isBlank()) {
            throw new IllegalArgumentException("engineVersion must not be blank");
        }
        result = Objects.requireNonNull(result, "result");
    }
}
