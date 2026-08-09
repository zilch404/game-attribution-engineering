package io.github.zilch404.attribution.model;

import java.io.Serializable;

public record IdentityKey(String type, String valueHash) implements Serializable {
    public IdentityKey {
        type = ModelValidation.requireNonBlank(type, "type");
        valueHash = ModelValidation.requireNonBlank(valueHash, "valueHash");
    }
}
