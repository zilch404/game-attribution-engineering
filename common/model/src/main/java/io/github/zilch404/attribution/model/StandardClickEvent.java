package io.github.zilch404.attribution.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public record StandardClickEvent(
        StandardEventMetadata metadata,
        AttributionScope scope,
        String sourceSystem,
        String clientPlatform,
        String clickId,
        List<StandardIdentity> identities,
        CampaignRef campaign,
        boolean eligible) implements StandardEvent {

    public StandardClickEvent {
        metadata = Objects.requireNonNull(metadata, "metadata");
        if (metadata.eventType() != EventType.CLICK) {
            throw new IllegalArgumentException("metadata eventType must be CLICK");
        }
        scope = Objects.requireNonNull(scope, "scope");
        sourceSystem = ModelValidation.requireNonBlank(sourceSystem, "sourceSystem");
        clientPlatform = ModelValidation.requireNonBlank(clientPlatform, "clientPlatform");
        clickId = ModelValidation.requireNonBlank(clickId, "clickId");
        identities = ModelValidation.copySortedNonEmptyList(identities, "identities");
        campaign = Objects.requireNonNull(campaign, "campaign");
    }

    /** Returns only the narrow event view consumed by the deterministic scoring core. */
    public ClickEvent toCoreEvent() {
        return new ClickEvent(
                metadata.eventId(),
                clickId,
                scope,
                metadata.eventTime(),
                identities.stream()
                        .map(StandardIdentity::toIdentityKey)
                        .collect(Collectors.toUnmodifiableSet()),
                campaign,
                eligible);
    }
}
