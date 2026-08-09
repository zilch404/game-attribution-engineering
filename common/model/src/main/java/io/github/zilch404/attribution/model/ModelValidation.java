package io.github.zilch404.attribution.model;

import java.util.Objects;
import java.util.Set;

final class ModelValidation {
    private ModelValidation() {
    }

    static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    static <T> Set<T> copyNonEmptySet(Set<T> values, String field) {
        Objects.requireNonNull(values, field);
        if (values.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        if (values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(field + " must not contain null");
        }
        return Set.copyOf(values);
    }
}
