package io.github.zilch404.attribution.core;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record AttributionRule(
        RuleKey key,
        Duration lookback,
        List<String> identityPriority) implements Serializable {

    public AttributionRule {
        key = Objects.requireNonNull(key, "key");
        lookback = Objects.requireNonNull(lookback, "lookback");
        if (lookback.isZero() || lookback.isNegative()) {
            throw new IllegalArgumentException("lookback must be positive");
        }
        identityPriority = List.copyOf(Objects.requireNonNull(identityPriority, "identityPriority"));
        if (identityPriority.isEmpty()) {
            throw new IllegalArgumentException("identityPriority must not be empty");
        }
        if (identityPriority.stream().anyMatch(type -> type == null || type.isBlank())) {
            throw new IllegalArgumentException("identityPriority must not contain blank values");
        }
        if (new HashSet<>(identityPriority).size() != identityPriority.size()) {
            throw new IllegalArgumentException("identityPriority must not contain duplicates");
        }
    }

    public int rankOf(String identityType) {
        return identityPriority.indexOf(identityType);
    }
}
