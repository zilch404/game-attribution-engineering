package io.github.zilch404.attribution.core;

import java.io.Serializable;

public record RuleKey(String ruleId, long ruleVersion) implements Serializable {
    public RuleKey {
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must not be blank");
        }
        if (ruleVersion < 1) {
            throw new IllegalArgumentException("ruleVersion must be positive");
        }
    }
}
