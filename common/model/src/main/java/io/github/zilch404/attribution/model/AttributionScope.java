package io.github.zilch404.attribution.model;

import java.io.Serializable;

public record AttributionScope(String tenantId, String gameId, String appId) implements Serializable {
    public AttributionScope {
        tenantId = ModelValidation.requireNonBlank(tenantId, "tenantId");
        gameId = ModelValidation.requireNonBlank(gameId, "gameId");
        appId = ModelValidation.requireNonBlank(appId, "appId");
    }
}
