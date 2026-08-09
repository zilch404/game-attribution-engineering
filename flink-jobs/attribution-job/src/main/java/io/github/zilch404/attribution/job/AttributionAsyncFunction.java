package io.github.zilch404.attribution.job;

import io.github.zilch404.attribution.core.AttributionRule;
import io.github.zilch404.attribution.core.DecisionDraft;
import io.github.zilch404.attribution.core.LastTouchAttributionEngine;
import io.github.zilch404.attribution.index.ClickCandidateLookup;
import io.github.zilch404.attribution.index.ClickCandidateLookupFactory;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Objects;

public final class AttributionAsyncFunction
        extends RichAsyncFunction<AttributionEvaluationRequest, DecisionDraft> {
    private final ClickCandidateLookupFactory lookupFactory;
    private final AttributionRule rule;
    private final int maxCandidates;

    private transient ClickCandidateLookup lookup;
    private transient AttributionAsyncEvaluator evaluator;

    public AttributionAsyncFunction(
            ClickCandidateLookupFactory lookupFactory,
            AttributionRule rule,
            int maxCandidates) {
        this.lookupFactory = Objects.requireNonNull(lookupFactory, "lookupFactory");
        this.rule = Objects.requireNonNull(rule, "rule");
        if (maxCandidates < 1) {
            throw new IllegalArgumentException("maxCandidates must be positive");
        }
        this.maxCandidates = maxCandidates;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        lookup = lookupFactory.create();
        evaluator = new AttributionAsyncEvaluator(
                lookup,
                new LastTouchAttributionEngine(),
                rule,
                maxCandidates);
    }

    @Override
    public void asyncInvoke(
            AttributionEvaluationRequest request,
            ResultFuture<DecisionDraft> resultFuture) {
        evaluator.evaluate(request.activation(), request.currentDecision())
                .whenComplete((decision, failure) -> {
                    if (failure != null) {
                        resultFuture.completeExceptionally(failure);
                    } else {
                        resultFuture.complete(Collections.singleton(decision));
                    }
                });
    }

    @Override
    public void timeout(
            AttributionEvaluationRequest request,
            ResultFuture<DecisionDraft> resultFuture) {
        resultFuture.complete(Collections.singleton(
                evaluator.timeout(request.activation(), request.currentDecision())));
    }

    @Override
    public void close() throws Exception {
        if (lookup != null) {
            lookup.close();
        }
    }
}
