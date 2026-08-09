package io.github.zilch404.attribution.core;

import io.github.zilch404.attribution.model.ActivationEvent;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.ClickEvent;
import io.github.zilch404.attribution.model.IdentityKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LastTouchAttributionEngineTest {
    private static final Instant ACTIVATION_TIME = Instant.parse("2026-01-08T00:00:00Z");
    private static final AttributionScope SCOPE = new AttributionScope("tenant", "game", "app");
    private static final IdentityKey TOKEN = new IdentityKey("click_id", "token-1");
    private static final IdentityKey DEVICE = new IdentityKey("device_id", "device-1");
    private static final CampaignRef CAMPAIGN = new CampaignRef("ocean-engine", "a", "c", "g", "r");
    private static final AttributionRule RULE = new AttributionRule(
            new RuleKey("default-last-touch", 1),
            Duration.ofDays(7),
            List.of("click_id", "device_id"));

    private final AttributionEngine engine = new LastTouchAttributionEngine();

    @Test
    void strongerIdentityBeatsNewerWeakIdentity() {
        ClickEvent strong = click("strong", ACTIVATION_TIME.minusSeconds(300), TOKEN);
        ClickEvent weak = click("weak", ACTIVATION_TIME.minusSeconds(60), DEVICE);

        DecisionDraft decision = evaluate(List.of(weak, strong), CandidateReadStatus.AUTHORITATIVE, null);

        assertThat(decision.result().matchedClickId()).isEqualTo("strong");
        assertThat(decision.result().matchedIdentity()).isEqualTo(TOKEN);
    }

    @Test
    void latestTouchWinsWithinSameIdentityRank() {
        ClickEvent older = click("older", ACTIVATION_TIME.minusSeconds(300), DEVICE);
        ClickEvent newer = click("newer", ACTIVATION_TIME.minusSeconds(60), DEVICE);

        DecisionDraft decision = evaluate(List.of(newer, older), CandidateReadStatus.AUTHORITATIVE, null);

        assertThat(decision.result().matchedClickId()).isEqualTo("newer");
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.FINAL);
    }

    @Test
    void lookbackBoundaryIsInclusiveAndFutureClicksAreRejected() {
        ClickEvent boundary = click("boundary", ACTIVATION_TIME.minus(Duration.ofDays(7)), DEVICE);
        ClickEvent tooOld = click("too-old", boundary.occurredAt().minusMillis(1), DEVICE);
        ClickEvent future = click("future", ACTIVATION_TIME.plusMillis(1), DEVICE);

        DecisionDraft decision = evaluate(
                List.of(tooOld, future, boundary),
                CandidateReadStatus.AUTHORITATIVE,
                null);

        assertThat(decision.result().matchedClickId()).isEqualTo("boundary");
        assertThat(decision.eligibleCandidates()).isEqualTo(1);
    }

    @Test
    void candidateOrderAndReplayDoNotChangeWinner() {
        ClickEvent a = click("click-a", ACTIVATION_TIME.minusSeconds(10), DEVICE);
        ClickEvent b = click("click-b", ACTIVATION_TIME.minusSeconds(10), DEVICE);
        List<ClickEvent> candidates = new ArrayList<>(List.of(b, a, a));

        for (int seed = 0; seed < 20; seed++) {
            Collections.shuffle(candidates, new java.util.Random(seed));
            DecisionDraft decision = evaluate(candidates, CandidateReadStatus.AUTHORITATIVE, null);
            assertThat(decision.result().matchedClickId()).isEqualTo("click-a");
            assertThat(decision.inspectedCandidates()).isEqualTo(2);
        }
    }

    @Test
    void multipleSharedIdentitiesAtTheSameRankUseAStableTieBreak() {
        IdentityKey deviceA = new IdentityKey("device_id", "device-a");
        IdentityKey deviceB = new IdentityKey("device_id", "device-b");
        ActivationEvent activation = new ActivationEvent(
                "activation-event",
                "activation-1",
                SCOPE,
                ACTIVATION_TIME,
                Set.of(deviceB, deviceA));
        ClickEvent click = new ClickEvent(
                "event-click",
                "click",
                SCOPE,
                ACTIVATION_TIME.minusSeconds(10),
                Set.of(deviceB, deviceA),
                CAMPAIGN,
                true);

        DecisionDraft decision = engine.evaluate(new EvaluationCommand(
                activation,
                snapshot(List.of(click), CandidateReadStatus.AUTHORITATIVE),
                RULE,
                null));

        assertThat(decision.result().matchedIdentity()).isEqualTo(deviceA);
    }

    @Test
    void authoritativeEmptySnapshotIsFinalOrganic() {
        DecisionDraft decision = evaluate(List.of(), CandidateReadStatus.AUTHORITATIVE, null);

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.ORGANIC);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.FINAL);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.NO_ELIGIBLE_CLICK);
    }

    @Test
    void incompleteEmptySnapshotIsRetryableUnknown() {
        for (CandidateReadStatus status : List.of(
                CandidateReadStatus.PARTIAL,
                CandidateReadStatus.TRUNCATED,
                CandidateReadStatus.INDEX_NOT_READY,
                CandidateReadStatus.TIMED_OUT,
                CandidateReadStatus.FAILED)) {
            DecisionDraft decision = evaluate(List.of(), status, null);
            assertThat(decision.result().outcome()).as(status.name()).isEqualTo(AttributionOutcome.UNKNOWN);
            assertThat(decision.result().status()).as(status.name()).isEqualTo(DecisionStatus.RETRYABLE);
        }
    }

    @Test
    void incompleteSnapshotWithCandidateIsProvisional() {
        DecisionDraft decision = evaluate(
                List.of(click("candidate", ACTIVATION_TIME.minusSeconds(10), DEVICE)),
                CandidateReadStatus.PARTIAL,
                null);

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.ATTRIBUTED);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.PROVISIONAL);
    }

    @Test
    void conflictingDuplicateIsNeverSilentlyResolved() {
        ClickEvent original = click("duplicate", ACTIVATION_TIME.minusSeconds(10), DEVICE);
        ClickEvent conflicting = new ClickEvent(
                "other-event",
                original.clickId(),
                original.scope(),
                original.occurredAt(),
                original.identities(),
                new CampaignRef("gdt", "", "other", "", ""),
                true);

        DecisionDraft decision = evaluate(
                List.of(original, conflicting),
                CandidateReadStatus.AUTHORITATIVE,
                null);

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(decision.result().reason()).isEqualTo(DecisionReason.CONFLICTING_DUPLICATE);
        assertThat(decision.disposition()).isEqualTo(EvaluationDisposition.QUARANTINE);
    }

    @Test
    void aForeignScopeCannotPoisonDeduplication() {
        ClickEvent local = click("shared-click-id", ACTIVATION_TIME.minusSeconds(10), DEVICE);
        ClickEvent foreign = new ClickEvent(
                "foreign-event",
                local.clickId(),
                new AttributionScope("tenant", "other-game", "other-app"),
                local.occurredAt(),
                local.identities(),
                new CampaignRef("gdt", "", "foreign", "", ""),
                true);

        DecisionDraft decision = evaluate(
                List.of(foreign, local),
                CandidateReadStatus.AUTHORITATIVE,
                null);

        assertThat(decision.result().matchedClickId()).isEqualTo(local.clickId());
        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.ATTRIBUTED);
    }

    @Test
    void aLateBetterCandidateProducesCorrection() {
        ClickEvent oldClick = click("old", ACTIVATION_TIME.minusSeconds(120), DEVICE);
        AttributionResult oldResult = evaluate(
                List.of(oldClick),
                CandidateReadStatus.AUTHORITATIVE,
                null).result();
        AttributionDecision current = new AttributionDecision(
                "decision-1",
                "activation-1",
                1,
                RULE.key(),
                LastTouchAttributionEngine.ENGINE_VERSION,
                oldResult);

        ClickEvent lateBetterClick = click("better", ACTIVATION_TIME.minusSeconds(60), DEVICE);
        DecisionDraft corrected = evaluate(
                List.of(oldClick, lateBetterClick),
                CandidateReadStatus.AUTHORITATIVE,
                current);

        assertThat(corrected.result().matchedClickId()).isEqualTo("better");
        assertThat(corrected.change()).isEqualTo(DecisionChange.CORRECTION);
        assertThat(corrected.supersedesDecisionId()).isEqualTo("decision-1");
    }

    @Test
    void completingTheSameResultProducesFinalization() {
        ClickEvent click = click("candidate", ACTIVATION_TIME.minusSeconds(60), DEVICE);
        AttributionResult provisional = evaluate(
                List.of(click),
                CandidateReadStatus.PARTIAL,
                null).result();
        AttributionDecision current = new AttributionDecision(
                "decision-1",
                "activation-1",
                1,
                RULE.key(),
                LastTouchAttributionEngine.ENGINE_VERSION,
                provisional);

        DecisionDraft finalized = evaluate(
                List.of(click),
                CandidateReadStatus.AUTHORITATIVE,
                current);

        assertThat(finalized.change()).isEqualTo(DecisionChange.FINALIZATION);
        assertThat(finalized.result().status()).isEqualTo(DecisionStatus.FINAL);
    }

    @Test
    void staleAuthoritativeWatermarkCannotProduceAFinalDecision() {
        ClickEvent click = click("candidate", ACTIVATION_TIME.minusSeconds(60), DEVICE);
        CandidateSnapshot staleSnapshot = new CandidateSnapshot(
                List.of(click),
                new CandidateEvidence(
                        CandidateReadStatus.AUTHORITATIVE,
                        "stale-index",
                        ACTIVATION_TIME.minus(RULE.lookback()),
                        ACTIVATION_TIME,
                        ACTIVATION_TIME.minusMillis(1),
                        false,
                        1));

        DecisionDraft decision = engine.evaluate(new EvaluationCommand(
                activation(),
                staleSnapshot,
                RULE,
                null));

        assertThat(decision.result().outcome()).isEqualTo(AttributionOutcome.ATTRIBUTED);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.PROVISIONAL);
    }

    @Test
    void transientFailureNeverDowngradesAKnownDecision() {
        ClickEvent click = click("candidate", ACTIVATION_TIME.minusSeconds(60), DEVICE);
        AttributionResult knownResult = evaluate(
                List.of(click),
                CandidateReadStatus.AUTHORITATIVE,
                null).result();
        AttributionDecision current = new AttributionDecision(
                "decision-1",
                "activation-1",
                1,
                RULE.key(),
                LastTouchAttributionEngine.ENGINE_VERSION,
                knownResult);

        DecisionDraft retryAttempt = evaluate(List.of(), CandidateReadStatus.FAILED, current);

        assertThat(retryAttempt.result().outcome()).isEqualTo(AttributionOutcome.UNKNOWN);
        assertThat(retryAttempt.change()).isEqualTo(DecisionChange.NO_CHANGE);
        assertThat(retryAttempt.disposition()).isEqualTo(EvaluationDisposition.RETRY_ONLY);
        assertThat(retryAttempt.supersedesDecisionId()).isNull();
    }

    @Test
    void permanentLookupFailureIsQuarantinedInsteadOfRetriedForever() {
        DecisionDraft decision = evaluate(
                List.of(),
                CandidateReadStatus.PERMANENT_FAILURE,
                null);

        assertThat(decision.result().reason())
                .isEqualTo(DecisionReason.LOOKUP_PERMANENT_FAILURE);
        assertThat(decision.result().status()).isEqualTo(DecisionStatus.QUARANTINED);
        assertThat(decision.disposition()).isEqualTo(EvaluationDisposition.QUARANTINE);
    }

    @Test
    void unknownResultCannotBeConstructedAsMaterializable() {
        AttributionResult unknown = new AttributionResult(
                AttributionOutcome.UNKNOWN,
                DecisionStatus.RETRYABLE,
                null,
                null,
                null,
                DecisionReason.LOOKUP_FAILED);

        assertThatThrownBy(() -> new DecisionDraft(
                "activation-1",
                RULE.key(),
                LastTouchAttributionEngine.ENGINE_VERSION,
                unknown,
                DecisionChange.INITIAL,
                EvaluationDisposition.MATERIALIZE,
                null,
                0,
                0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UNKNOWN");
    }

    @Test
    void unavailableFactoryRejectsStatusesThatRequireExplicitCoverage() {
        assertThatThrownBy(() ->
                CandidateSnapshot.unavailable(CandidateReadStatus.TRUNCATED, "trace"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() ->
                CandidateSnapshot.unavailable(CandidateReadStatus.PARTIAL, "trace"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPartialCandidateNeverOverwritesAnExistingFinalDecision() {
        ClickEvent original = click("original", ACTIVATION_TIME.minusSeconds(120), DEVICE);
        AttributionResult finalResult = evaluate(
                List.of(original),
                CandidateReadStatus.AUTHORITATIVE,
                null).result();
        AttributionDecision current = new AttributionDecision(
                "decision-1",
                "activation-1",
                1,
                RULE.key(),
                LastTouchAttributionEngine.ENGINE_VERSION,
                finalResult);

        ClickEvent partialBetter = click("partial-better", ACTIVATION_TIME.minusSeconds(60), DEVICE);
        DecisionDraft retryAttempt = evaluate(
                List.of(partialBetter),
                CandidateReadStatus.PARTIAL,
                current);

        assertThat(retryAttempt.result().matchedClickId()).isEqualTo("partial-better");
        assertThat(retryAttempt.disposition()).isEqualTo(EvaluationDisposition.RETRY_ONLY);
        assertThat(retryAttempt.change()).isEqualTo(DecisionChange.NO_CHANGE);
        assertThat(retryAttempt.supersedesDecisionId()).isNull();
    }

    private DecisionDraft evaluate(
            List<ClickEvent> clicks,
            CandidateReadStatus status,
            AttributionDecision current) {
        return engine.evaluate(new EvaluationCommand(
                activation(),
                snapshot(clicks, status),
                RULE,
                current));
    }

    private static CandidateSnapshot snapshot(
            List<ClickEvent> clicks,
            CandidateReadStatus status) {
        boolean hasCoverage = switch (status) {
            case AUTHORITATIVE, PARTIAL, TRUNCATED -> true;
            default -> false;
        };
        return new CandidateSnapshot(
                clicks,
                new CandidateEvidence(
                        status,
                        "lookup-test",
                        hasCoverage ? ACTIVATION_TIME.minus(RULE.lookback()) : null,
                        hasCoverage ? ACTIVATION_TIME : null,
                        hasCoverage ? ACTIVATION_TIME.plusSeconds(60) : null,
                        status == CandidateReadStatus.TRUNCATED,
                        clicks.size()));
    }

    private static ActivationEvent activation() {
        return new ActivationEvent(
                "activation-event",
                "activation-1",
                SCOPE,
                ACTIVATION_TIME,
                Set.of(TOKEN, DEVICE));
    }

    private static ClickEvent click(String clickId, Instant occurredAt, IdentityKey identity) {
        return new ClickEvent(
                "event-" + clickId,
                clickId,
                SCOPE,
                occurredAt,
                Set.of(identity),
                CAMPAIGN,
                true);
    }
}
