package io.github.zilch404.attribution.model;

import java.util.Objects;
import java.util.List;
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

    static <T extends Comparable<? super T>> List<T> copySortedNonEmptyList(
            List<T> values,
            String field) {
        Objects.requireNonNull(values, field);
        if (values.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be empty");
        }
        if (values.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(field + " must not contain null");
        }
        List<T> sorted = values.stream().sorted().toList();
        if (sorted.stream().distinct().count() != sorted.size()) {
            throw new IllegalArgumentException(field + " must not contain duplicates");
        }
        return sorted;
    }
}
