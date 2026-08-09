package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.IdentityKey;

import java.io.Serializable;
import java.util.Objects;

public record NormalizedIdentity(
        String eventId,
        AttributionScope scope,
        IdentityKey identity,
        String normalizationVersion) implements Serializable {

    public NormalizedIdentity {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        scope = Objects.requireNonNull(scope, "scope");
        identity = Objects.requireNonNull(identity, "identity");
        if (normalizationVersion == null || normalizationVersion.isBlank()) {
            throw new IllegalArgumentException("normalizationVersion must not be blank");
        }
    }
}
