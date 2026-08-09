package io.github.zilch404.attribution.index;

import java.io.Serializable;

@FunctionalInterface
public interface ClickCandidateLookupFactory extends Serializable {
    ClickCandidateLookup create() throws Exception;
}
