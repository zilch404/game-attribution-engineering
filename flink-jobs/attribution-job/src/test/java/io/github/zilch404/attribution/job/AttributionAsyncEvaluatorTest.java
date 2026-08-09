package io.github.zilch404.attribution.job;

import io.github.zilch404.attribution.core.AttributionOutcome;
import io.github.zilch404.attribution.core.AttributionRule;
import io.github.zilch404.attribution.core.CandidateReadStatus;
import io.github.zilch404.attribution.core.CandidateSnapshot;
import io.github.zilch404.attribution.core.DecisionReason;
import io.github.zilch404.attribution.core.DecisionStatus;
import io.github.zilch404.attribution.core.EvaluationDisposition;
import io.github.zilch404.attribution.core.LastTouchAttributionEngine;
import io.github.zilch404.attribution.core.RuleKey;
import io.github.zilch404.attribution.index.ClickCandidateLookup;
import io.github.zilch404.attribution.index.ClickCandidateLookupFactory;
import io.github.zilch404.attribution.model.ActivationEvent;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.ClickEvent;
import io.github.zilch404.attribution.model.IdentityKey;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class AttributionAsyncEvaluatorTest {
    private static final Instant NOW = Instant.parse("2026-01-08T00:00:00Z");
    private static final AttributionScope SCOPE = new AttributionScope("tenant", "game", "app");
    private static final IdentityKey DEVICE = new IdentityKey("device_id", "hash");
    private static final AttributionRule RULE = new AttributionRule(
            new RuleKey("default", 1),
            Duration.ofDays(7),
            List.of("device_id"));

    @Test
    void buildsTheBoundedLookupQueryAndEvaluatesTheResult() {
        ClickEvent click = new ClickEvent(
                "click-event",
                "click-1",
                SCOPE,
                NOW.minusSeconds(10),
                Set.of(DEVICE),
                new CampaignRef("gdt", "", "campaign", "", ""),
                true);
        ClickCandidateLookup lookup = query -> {
            assertThat(query.fromInclusive()).isEqualTo(NOW.minus(Duration.ofDays(7)));
            assertThat(query.toInclusive()).isEqualTo(NOW);
            assertThat(query.maxCandidates()).isEqualTo(128);
            assertThat(query.identities()).containsExactly(DEVICE);
            return CompletableFuture.completedFuture(
                    CandidateSnapshot.authoritative(
                            List.of(click),
                            "trace",
                            query.fromInclusive(),
                            query.toInclusive(),
                            NOW.plusSeconds(1)));
        };

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.ATTRIBUTED);
        assertThat(decision.result().matchedClickId()).isEqualTo("click-1");
        assertThat(DecisionRoutingProcessFunction.routeOf(decision))
                .isEqualTo(DecisionRoute.MATERIALIZE);
    }

    @Test
    void lookupFailureIsUnknownInsteadOfOrganic() {
        ClickCandidateLookup lookup = query -> CompletableFuture.failedFuture(
                new IllegalStateException("Fluss unavailable"));

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.RETRYABLE);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.LOOKUP_FAILED);
        assertThat(DecisionRoutingProcessFunction.routeOf(decision))
                .isEqualTo(DecisionRoute.RETRY);
    }

    @Test
    void synchronousLookupFailureIsUnknownInsteadOfFailingTheOperator() {
        ClickCandidateLookup lookup = query -> {
            throw new IllegalStateException("client queue rejected the request");
        };

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.LOOKUP_FAILED);
    }

    @Test
    void nullLookupStageIsUnknownInsteadOfFailingTheOperator() {
        ClickCandidateLookup lookup = query -> null;

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.LOOKUP_FAILED);
    }

    @Test
    void nullLookupResultIsUnknownInsteadOfAnExceptionalCompletion() {
        ClickCandidateLookup lookup = query -> CompletableFuture.completedFuture(null);

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.LOOKUP_FAILED);
        assertThat(decision.disposition()).isEqualTo(EvaluationDisposition.RETRY_ONLY);
    }

    @Test
    void unsupportedIdentitiesDoNotInvokeTheIndex() {
        ActivationEvent unsupported = new ActivationEvent(
                "activation-event",
                "activation-unsupported",
                SCOPE,
                NOW,
                Set.of(new IdentityKey("unsupported", "hash")));
        ClickCandidateLookup lookup = query -> {
            throw new AssertionError("lookup must not be invoked");
        };

        var decision = evaluator(lookup).evaluate(unsupported, null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.NO_SUPPORTED_IDENTITY);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.QUARANTINED);
        assertThat(decision.disposition()).isEqualTo(EvaluationDisposition.QUARANTINE);
        assertThat(DecisionRoutingProcessFunction.routeOf(decision))
                .isEqualTo(DecisionRoute.QUARANTINE);
    }

    @Test
    void lookupQueryContainsOnlyRuleSupportedIdentitiesInPriorityOrder() {
        IdentityKey token = new IdentityKey("click_id", "token");
        IdentityKey unsupported = new IdentityKey("openid", "openid");
        AttributionRule rule = new AttributionRule(
                new RuleKey("multi-id", 1),
                Duration.ofDays(7),
                List.of("click_id", "device_id"));
        ActivationEvent activation = new ActivationEvent(
                "activation-event",
                "activation-1",
                SCOPE,
                NOW,
                Set.of(DEVICE, unsupported, token));
        ClickCandidateLookup lookup = query -> {
            assertThat(query.identities()).containsExactly(token, DEVICE);
            return CompletableFuture.completedFuture(CandidateSnapshot.unavailable(
                    CandidateReadStatus.INDEX_NOT_READY,
                    "trace"));
        };

        new AttributionAsyncEvaluator(lookup, new LastTouchAttributionEngine(), rule, 128)
                .evaluate(activation, null)
                .toCompletableFuture()
                .join();
    }

    @Test
    void flinkAdaptersAreSerializableBeforeClientsAreCreatedInOpen() {
        ClickCandidateLookupFactory factory = () -> query ->
                CompletableFuture.completedFuture(CandidateSnapshot.unavailable(
                        CandidateReadStatus.INDEX_NOT_READY,
                        "trace"));

        assertThatCode(() -> InstantiationUtil.serializeObject(
                new AttributionAsyncFunction(factory, RULE, 128)))
                .doesNotThrowAnyException();
        assertThatCode(() -> InstantiationUtil.serializeObject(
                new DecisionRoutingProcessFunction()))
                .doesNotThrowAnyException();
    }

    @Test
    void flinkSerializersRoundTripTheEvaluationBoundary() throws IOException {
        ActivationEvent activation = activation();
        var draft = evaluator(query -> CompletableFuture.completedFuture(
                        CandidateSnapshot.unavailable(CandidateReadStatus.INDEX_NOT_READY, "trace")))
                .evaluate(activation, null)
                .toCompletableFuture()
                .join();
        AttributionEvaluationRequest request = new AttributionEvaluationRequest(activation, null);

        assertThat(roundTrip(request, AttributionEvaluationRequest.class)).isEqualTo(request);
        assertThat(roundTrip(draft, io.github.zilch404.attribution.core.DecisionDraft.class))
                .isEqualTo(draft);
    }

    @Test
    void indexNotReadyIsUnknownInsteadOfOrganic() {
        ClickCandidateLookup lookup = query -> CompletableFuture.completedFuture(
                CandidateSnapshot.unavailable(CandidateReadStatus.INDEX_NOT_READY, "trace"));

        var decision = evaluator(lookup).evaluate(activation(), null).toCompletableFuture().join();

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.INDEX_NOT_READY);
    }

    @Test
    void flinkTimeoutIsAVisibleRetryableDecision() {
        ClickCandidateLookup lookup = query -> new CompletableFuture<>();

        var decision = evaluator(lookup).timeout(activation(), null);

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.LOOKUP_TIMED_OUT);
    }

    private static AttributionAsyncEvaluator evaluator(ClickCandidateLookup lookup) {
        return new AttributionAsyncEvaluator(lookup, new LastTouchAttributionEngine(), RULE, 128);
    }

    private static ActivationEvent activation() {
        return new ActivationEvent(
                "activation-event",
                "activation-1",
                SCOPE,
                NOW,
                Set.of(DEVICE));
    }

    private static <T> T roundTrip(T value, Class<T> type) throws IOException {
        var serializer = TypeInformation.of(type).createSerializer(new SerializerConfigImpl());
        DataOutputSerializer output = new DataOutputSerializer(512);
        serializer.serialize(value, output);
        return serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }
}
