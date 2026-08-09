package io.github.zilch404.attribution.job;

import io.github.zilch404.attribution.core.AttributionDecision;
import io.github.zilch404.attribution.core.AttributionEngine;
import io.github.zilch404.attribution.core.AttributionRule;
import io.github.zilch404.attribution.core.CandidateReadStatus;
import io.github.zilch404.attribution.core.CandidateSnapshot;
import io.github.zilch404.attribution.core.DecisionDraft;
import io.github.zilch404.attribution.core.EvaluationCommand;
import io.github.zilch404.attribution.index.ClickCandidateLookup;
import io.github.zilch404.attribution.index.ClickCandidateQuery;
import io.github.zilch404.attribution.model.ActivationEvent;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class AttributionAsyncEvaluator {
    private final ClickCandidateLookup lookup;
    private final AttributionEngine engine;
    private final AttributionRule rule;
    private final int maxCandidates;

    public AttributionAsyncEvaluator(
            ClickCandidateLookup lookup,
            AttributionEngine engine,
            AttributionRule rule,
            int maxCandidates) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.rule = Objects.requireNonNull(rule, "rule");
        if (maxCandidates < 1) {
            throw new IllegalArgumentException("maxCandidates must be positive");
        }
        this.maxCandidates = maxCandidates;
    }

    public CompletionStage<DecisionDraft> evaluate(
            ActivationEvent activation,
            AttributionDecision currentDecision) {
        boolean hasSupportedIdentity = activation.identities().stream()
                .anyMatch(identity -> rule.rankOf(identity.type()) >= 0);
        if (!hasSupportedIdentity) {
            return CompletableFuture.completedFuture(
                    engine.evaluate(new EvaluationCommand(
                            activation,
                            CandidateSnapshot.unavailable(
                                    CandidateReadStatus.NO_SUPPORTED_IDENTITY,
                                    "unsupported-identity:" + activation.activationId()),
                            rule,
                            currentDecision)));
        }
        ClickCandidateQuery query = ClickCandidateQuery.from(activation, rule, maxCandidates);
        CompletionStage<CandidateSnapshot> lookupResult;
        try {
            lookupResult = lookup.findCandidates(query);
        } catch (RuntimeException failure) {
            lookupResult = CompletableFuture.failedFuture(failure);
        }
        if (lookupResult == null) {
            lookupResult = CompletableFuture.failedFuture(
                    new IllegalStateException("lookup returned a null CompletionStage"));
        }
        return lookupResult.handle((snapshot, failure) -> {
            CandidateSnapshot effectiveSnapshot = failure == null && snapshot != null
                    ? snapshot
                    : CandidateSnapshot.unavailable(
                            CandidateReadStatus.FAILED,
                            "lookup-failure:" + activation.activationId());
            return engine.evaluate(new EvaluationCommand(
                    activation,
                    effectiveSnapshot,
                    rule,
                    currentDecision));
        });
    }

    public DecisionDraft timeout(ActivationEvent activation, AttributionDecision currentDecision) {
        return engine.evaluate(new EvaluationCommand(
                activation,
                CandidateSnapshot.unavailable(
                        CandidateReadStatus.TIMED_OUT,
                        "lookup-timeout:" + activation.activationId()),
                rule,
                currentDecision));
    }
}
