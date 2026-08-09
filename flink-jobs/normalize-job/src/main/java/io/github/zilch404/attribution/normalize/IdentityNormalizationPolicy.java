package io.github.zilch404.attribution.normalize;

import java.io.Serializable;
import java.util.Optional;

public interface IdentityNormalizationPolicy extends Serializable {
    Optional<IdentityNormalizationRule> ruleFor(String identityType);
}
