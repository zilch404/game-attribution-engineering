package io.github.zilch404.attribution.model;

import java.io.Serializable;
import java.util.Objects;

public record CampaignRef(
        String platform,
        String accountId,
        String campaignId,
        String adGroupId,
        String creativeId) implements Serializable {

    public CampaignRef {
        platform = ModelValidation.requireNonBlank(platform, "platform");
        accountId = Objects.requireNonNullElse(accountId, "");
        campaignId = Objects.requireNonNullElse(campaignId, "");
        adGroupId = Objects.requireNonNullElse(adGroupId, "");
        creativeId = Objects.requireNonNullElse(creativeId, "");
    }
}
