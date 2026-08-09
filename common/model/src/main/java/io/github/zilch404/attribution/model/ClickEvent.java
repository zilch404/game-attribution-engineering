package io.github.zilch404.attribution.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public record ClickEvent(
        String eventId,
        String clickId,
        AttributionScope scope,
        Instant occurredAt,
        Set<IdentityKey> identities,
        CampaignRef campaign,
        boolean eligible) implements Serializable {

    public ClickEvent {
        eventId = ModelValidation.requireNonBlank(eventId, "eventId");
        clickId = ModelValidation.requireNonBlank(clickId, "clickId");
        scope = Objects.requireNonNull(scope, "scope");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt");
        identities = ModelValidation.copyNonEmptySet(identities, "identities");
        campaign = Objects.requireNonNull(campaign, "campaign");
    }
}
