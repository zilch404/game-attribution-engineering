package io.github.zilch404.attribution.index;

import io.github.zilch404.attribution.core.AttributionRule;
import io.github.zilch404.attribution.model.ActivationEvent;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.IdentityKey;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public record ClickCandidateQuery(
        String activationId,
        AttributionScope scope,
        List<IdentityKey> identities,
        Instant fromInclusive,
        Instant toInclusive,
        int maxCandidates) implements Serializable {

    /**
     * {@code maxCandidates} is the maximum list size returned to the engine, not proof that a
     * result is complete. Adapters must probe one extra row and mark overflow as TRUNCATED.
     */

    public ClickCandidateQuery {
        if (activationId == null || activationId.isBlank()) {
            throw new IllegalArgumentException("activationId must not be blank");
        }
        scope = Objects.requireNonNull(scope, "scope");
        identities = List.copyOf(Objects.requireNonNull(identities, "identities"));
        if (identities.isEmpty()) {
            throw new IllegalArgumentException("identities must not be empty");
        }
        fromInclusive = Objects.requireNonNull(fromInclusive, "fromInclusive");
        toInclusive = Objects.requireNonNull(toInclusive, "toInclusive");
        if (fromInclusive.isAfter(toInclusive)) {
            throw new IllegalArgumentException("fromInclusive must not be after toInclusive");
        }
        if (maxCandidates < 1) {
            throw new IllegalArgumentException("maxCandidates must be positive");
        }
    }

    public static ClickCandidateQuery from(
            ActivationEvent activation,
            AttributionRule rule,
            int maxCandidates) {
        List<IdentityKey> supportedIdentities = activation.identities().stream()
                .filter(identity -> rule.rankOf(identity.type()) >= 0)
                .sorted(Comparator
                        .comparingInt((IdentityKey identity) -> rule.rankOf(identity.type()))
                        .thenComparing(IdentityKey::type)
                        .thenComparing(IdentityKey::valueHash))
                .toList();
        return new ClickCandidateQuery(
                activation.activationId(),
                activation.scope(),
                supportedIdentities,
                activation.occurredAt().minus(rule.lookback()),
                activation.occurredAt(),
                maxCandidates);
    }
}
