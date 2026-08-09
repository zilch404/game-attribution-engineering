package io.github.zilch404.attribution.normalize;

import java.io.Serializable;
import java.util.Objects;

public record IdentityNormalizationRule(
        Canonicalization canonicalization,
        String normalizationVersion,
        String keyVersion) implements Serializable {

    public IdentityNormalizationRule {
        canonicalization = Objects.requireNonNull(canonicalization, "canonicalization");
        normalizationVersion = requireVersion(normalizationVersion, "normalizationVersion");
        keyVersion = requireVersion(keyVersion, "keyVersion");
    }

    private static String requireVersion(String value, String field) {
        if (value == null || !value.matches("[a-z0-9][a-z0-9._-]{0,63}")) {
            throw new IllegalArgumentException(
                    field + " must be a lowercase version token of at most 64 characters");
        }
        return value;
    }
}
