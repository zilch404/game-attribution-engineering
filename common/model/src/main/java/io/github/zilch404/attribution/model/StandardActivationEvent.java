package io.github.zilch404.attribution.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public record StandardActivationEvent(
        StandardEventMetadata metadata,
        AttributionScope scope,
        String sourceSystem,
        String clientPlatform,
        String activationId,
        String installId,
        String directClickId,
        List<StandardIdentity> identities) implements StandardEvent {

    public StandardActivationEvent {
        metadata = Objects.requireNonNull(metadata, "metadata");
        if (metadata.eventType() != EventType.ACTIVATION) {
            throw new IllegalArgumentException("metadata eventType must be ACTIVATION");
        }
        scope = Objects.requireNonNull(scope, "scope");
        sourceSystem = ModelValidation.requireNonBlank(sourceSystem, "sourceSystem");
        clientPlatform = ModelValidation.requireNonBlank(clientPlatform, "clientPlatform");
        activationId = ModelValidation.requireNonBlank(activationId, "activationId");
        installId = ModelValidation.requireNonBlank(installId, "installId");
        directClickId = Objects.requireNonNullElse(directClickId, "");
        identities = ModelValidation.copySortedNonEmptyList(identities, "identities");
    }

    /**
     * Returns only the narrow event view consumed by the deterministic scoring core.
     * Callers must retain this standard event for install mapping and direct-click lookup.
     */
    public ActivationEvent toCoreEvent() {
        return new ActivationEvent(
                metadata.eventId(),
                activationId,
                scope,
                metadata.eventTime(),
                identities.stream()
                        .map(StandardIdentity::toIdentityKey)
                        .collect(Collectors.toUnmodifiableSet()));
    }
}
