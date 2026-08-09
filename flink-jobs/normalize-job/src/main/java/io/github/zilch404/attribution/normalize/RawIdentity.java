package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.AttributionScope;

import java.io.Serializable;

/**
 * Internal normalization command. A trusted ingestion policy, never an external payload,
 * must select {@code canonicalization}.
 */
public record RawIdentity(
        String eventId,
        AttributionScope scope,
        String type,
        String rawValue,
        Canonicalization canonicalization) implements Serializable {
}
