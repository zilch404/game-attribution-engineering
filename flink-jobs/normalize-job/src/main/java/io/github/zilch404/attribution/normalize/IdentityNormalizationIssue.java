package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.SourcePosition;

import java.io.Serializable;
import java.util.Objects;

public record IdentityNormalizationIssue(
        SourcePosition sourcePosition,
        String eventId,
        int identityIndex,
        IdentityIssueCode code) implements Serializable {

    public IdentityNormalizationIssue {
        sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (identityIndex < 0) {
            throw new IllegalArgumentException("identityIndex must not be negative");
        }
        code = Objects.requireNonNull(code, "code");
    }
}
