package io.github.zilch404.attribution.core;

import io.github.zilch404.attribution.model.ActivationEvent;
import io.github.zilch404.attribution.model.ClickEvent;
import io.github.zilch404.attribution.model.IdentityKey;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class LastTouchAttributionEngine implements AttributionEngine {
    public static final String ENGINE_VERSION = "last-touch-v1";

    private static final Comparator<RankedCandidate> CANDIDATE_ORDER =
            Comparator.comparingInt(RankedCandidate::identityRank)
                    .thenComparing(
                            (RankedCandidate candidate) -> candidate.click().occurredAt(),
                            Comparator.reverseOrder())
                    .thenComparing(candidate -> candidate.click().clickId());

    @Override
    public String engineVersion() {
        return ENGINE_VERSION;
    }

    @Override
    public DecisionDraft evaluate(EvaluationCommand command) {
        boolean hasSupportedIdentity = command.activation().identities().stream()
                .anyMatch(identity -> command.rule().rankOf(identity.type()) >= 0);
        if (!hasSupportedIdentity) {
            return draft(
                    command,
                    unknown(DecisionReason.NO_SUPPORTED_IDENTITY),
                    0,
                    0);
        }

        List<ClickEvent> scopedClicks = command.candidates().clicks().stream()
                .filter(click -> click.scope().equals(command.activation().scope()))
                .toList();
        DeduplicationResult deduplication = deduplicate(scopedClicks);
        if (deduplication.conflicting()) {
            return draft(
                    command,
                    unknown(DecisionReason.CONFLICTING_DUPLICATE),
                    deduplication.clicks().size(),
                    0);
        }

        List<RankedCandidate> eligible = eligibleCandidates(
                command.activation(),
                deduplication.clicks(),
                command.rule());

        CandidateReadStatus effectiveStatus = effectiveReadStatus(command);
        AttributionResult result;
        if (eligible.isEmpty()) {
            result = effectiveStatus.isAuthoritative()
                    ? organic()
                    : unknown(reasonFor(effectiveStatus));
        } else {
            RankedCandidate winner = eligible.stream().min(CANDIDATE_ORDER).orElseThrow();
            DecisionStatus status = effectiveStatus.isAuthoritative()
                    ? DecisionStatus.FINAL
                    : DecisionStatus.PROVISIONAL;
            result = new AttributionResult(
                    AttributionOutcome.ATTRIBUTED,
                    status,
                    winner.click().clickId(),
                    winner.matchedIdentity(),
                    winner.click().campaign(),
                    DecisionReason.MATCHED);
        }

        return draft(command, result, deduplication.clicks().size(), eligible.size());
    }

    private DecisionDraft draft(
            EvaluationCommand command,
            AttributionResult result,
            int inspectedCandidates,
            int eligibleCandidates) {
        EvaluationDisposition disposition = dispositionFor(command.currentDecision(), result);
        DecisionChange change = disposition != EvaluationDisposition.MATERIALIZE
                ? DecisionChange.NO_CHANGE
                : classifyChange(
                        command.currentDecision(),
                        command.rule().key(),
                        engineVersion(),
                        result);
        if (change == DecisionChange.NO_CHANGE && disposition == EvaluationDisposition.MATERIALIZE) {
            disposition = EvaluationDisposition.NOOP;
        }
        String supersedes = disposition == EvaluationDisposition.MATERIALIZE
                && change != DecisionChange.INITIAL
                ? command.currentDecision().decisionId()
                : null;
        return new DecisionDraft(
                command.activation().activationId(),
                command.rule().key(),
                engineVersion(),
                result,
                change,
                disposition,
                supersedes,
                inspectedCandidates,
                eligibleCandidates);
    }

    private static EvaluationDisposition dispositionFor(
            AttributionDecision current,
            AttributionResult next) {
        if (next.outcome() == AttributionOutcome.UNKNOWN) {
            return next.status() == DecisionStatus.QUARANTINED
                    ? EvaluationDisposition.QUARANTINE
                    : EvaluationDisposition.RETRY_ONLY;
        }
        if (current != null
                && current.result().status() == DecisionStatus.FINAL
                && next.status() != DecisionStatus.FINAL) {
            return EvaluationDisposition.RETRY_ONLY;
        }
        return EvaluationDisposition.MATERIALIZE;
    }

    private static CandidateReadStatus effectiveReadStatus(EvaluationCommand command) {
        CandidateEvidence evidence = command.candidates().evidence();
        if (evidence.status().isAuthoritative()) {
            Instant requiredFrom = command.activation().occurredAt().minus(command.rule().lookback());
            if (evidence.indexWatermark().isBefore(command.activation().occurredAt())) {
                return CandidateReadStatus.INDEX_NOT_READY;
            }
            if (!command.candidates().isAuthoritativeFor(
                    requiredFrom,
                    command.activation().occurredAt())) {
                return CandidateReadStatus.PARTIAL;
            }
        }
        return evidence.status();
    }

    private static List<RankedCandidate> eligibleCandidates(
            ActivationEvent activation,
            List<ClickEvent> clicks,
            AttributionRule rule) {
        Instant fromInclusive = activation.occurredAt().minus(rule.lookback());
        Map<IdentityKey, Integer> activationIdentityRanks = new HashMap<>();
        for (IdentityKey identity : activation.identities()) {
            int rank = rule.rankOf(identity.type());
            if (rank >= 0) {
                activationIdentityRanks.merge(identity, rank, Math::min);
            }
        }

        List<RankedCandidate> eligible = new ArrayList<>();
        for (ClickEvent click : clicks) {
            if (!click.eligible() || !click.scope().equals(activation.scope())) {
                continue;
            }
            if (click.occurredAt().isBefore(fromInclusive)
                    || click.occurredAt().isAfter(activation.occurredAt())) {
                continue;
            }
            RankedIdentity identity = bestSharedIdentity(click, activationIdentityRanks);
            if (identity != null) {
                eligible.add(new RankedCandidate(click, identity.identity(), identity.rank()));
            }
        }
        return eligible;
    }

    private static RankedIdentity bestSharedIdentity(
            ClickEvent click,
            Map<IdentityKey, Integer> activationIdentityRanks) {
        RankedIdentity best = null;
        for (IdentityKey identity : click.identities()) {
            Integer rank = activationIdentityRanks.get(identity);
            if (rank != null) {
                RankedIdentity candidate = new RankedIdentity(identity, rank);
                if (best == null || compareIdentity(candidate, best) < 0) {
                    best = candidate;
                }
            }
        }
        return best;
    }

    private static int compareIdentity(RankedIdentity left, RankedIdentity right) {
        int rankComparison = Integer.compare(left.rank(), right.rank());
        if (rankComparison != 0) {
            return rankComparison;
        }
        int typeComparison = left.identity().type().compareTo(right.identity().type());
        return typeComparison != 0
                ? typeComparison
                : left.identity().valueHash().compareTo(right.identity().valueHash());
    }

    private static DeduplicationResult deduplicate(List<ClickEvent> clicks) {
        Map<String, ClickEvent> unique = new LinkedHashMap<>();
        boolean conflicting = false;
        for (ClickEvent click : clicks) {
            ClickEvent existing = unique.putIfAbsent(click.clickId(), click);
            if (existing != null && !existing.equals(click)) {
                conflicting = true;
            }
        }
        return new DeduplicationResult(List.copyOf(unique.values()), conflicting);
    }

    private static AttributionResult organic() {
        return new AttributionResult(
                AttributionOutcome.ORGANIC,
                DecisionStatus.FINAL,
                null,
                null,
                null,
                DecisionReason.NO_ELIGIBLE_CLICK);
    }

    private static AttributionResult unknown(DecisionReason reason) {
        boolean quarantinable = reason == DecisionReason.NO_SUPPORTED_IDENTITY
                || reason == DecisionReason.LOOKUP_PERMANENT_FAILURE
                || reason == DecisionReason.CONFLICTING_DUPLICATE;
        return new AttributionResult(
                AttributionOutcome.UNKNOWN,
                quarantinable ? DecisionStatus.QUARANTINED : DecisionStatus.RETRYABLE,
                null,
                null,
                null,
                reason);
    }

    private static DecisionReason reasonFor(CandidateReadStatus status) {
        return switch (status) {
            case AUTHORITATIVE -> throw new IllegalArgumentException("authoritative status is complete");
            case PARTIAL -> DecisionReason.LOOKUP_INCOMPLETE;
            case TRUNCATED -> DecisionReason.CANDIDATES_TRUNCATED;
            case INDEX_NOT_READY -> DecisionReason.INDEX_NOT_READY;
            case NO_SUPPORTED_IDENTITY -> DecisionReason.NO_SUPPORTED_IDENTITY;
            case TIMED_OUT -> DecisionReason.LOOKUP_TIMED_OUT;
            case FAILED -> DecisionReason.LOOKUP_FAILED;
            case PERMANENT_FAILURE -> DecisionReason.LOOKUP_PERMANENT_FAILURE;
        };
    }

    private static DecisionChange classifyChange(
            AttributionDecision current,
            RuleKey newRule,
            String newEngineVersion,
            AttributionResult next) {
        if (current == null) {
            return DecisionChange.INITIAL;
        }
        if (current.result().outcome() == AttributionOutcome.UNKNOWN
                && next.outcome() != AttributionOutcome.UNKNOWN) {
            return DecisionChange.RESOLUTION;
        }
        if (!sameBusinessResult(current.result(), next)) {
            return DecisionChange.CORRECTION;
        }
        if (current.result().status() != DecisionStatus.FINAL
                && next.status() == DecisionStatus.FINAL) {
            return DecisionChange.FINALIZATION;
        }
        if (!current.rule().equals(newRule) || !current.engineVersion().equals(newEngineVersion)) {
            return DecisionChange.REEVALUATION;
        }
        return DecisionChange.NO_CHANGE;
    }

    private static boolean sameBusinessResult(AttributionResult left, AttributionResult right) {
        return left.outcome() == right.outcome()
                && java.util.Objects.equals(left.matchedClickId(), right.matchedClickId())
                && java.util.Objects.equals(left.matchedIdentity(), right.matchedIdentity())
                && java.util.Objects.equals(left.campaign(), right.campaign());
    }

    private record RankedIdentity(IdentityKey identity, int rank) {
    }

    private record RankedCandidate(ClickEvent click, IdentityKey matchedIdentity, int identityRank) {
    }

    private record DeduplicationResult(List<ClickEvent> clicks, boolean conflicting) {
    }
}
