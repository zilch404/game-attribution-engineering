package io.github.zilch404.attribution.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public record ActivationEvent(
        String eventId,
        String activationId,
        AttributionScope scope,
        Instant occurredAt,
        Set<IdentityKey> identities) implements Serializable {

    public ActivationEvent {
        eventId = ModelValidation.requireNonBlank(eventId, "eventId");
        activationId = ModelValidation.requireNonBlank(activationId, "activationId");
        scope = Objects.requireNonNull(scope, "scope");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt");
        identities = ModelValidation.copyNonEmptySet(identities, "identities");
    }
}
