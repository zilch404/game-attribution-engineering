package io.github.zilch404.attribution.core;

import io.github.zilch404.attribution.model.ClickEvent;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record CandidateSnapshot(
        List<ClickEvent> clicks,
        CandidateEvidence evidence) implements Serializable {

    public CandidateSnapshot {
        clicks = List.copyOf(Objects.requireNonNull(clicks, "clicks"));
        evidence = Objects.requireNonNull(evidence, "evidence");
        if (evidence.returnedCount() != clicks.size()) {
            throw new IllegalArgumentException(
                    "returnedCount must equal the candidate list size");
        }
        if (switch (evidence.status()) {
            case INDEX_NOT_READY, NO_SUPPORTED_IDENTITY, TIMED_OUT, FAILED, PERMANENT_FAILURE ->
                    !clicks.isEmpty();
            default -> false;
        }) {
            throw new IllegalArgumentException(
                    evidence.status() + " snapshots must not contain candidates");
        }
    }

    public static CandidateSnapshot authoritative(
            List<ClickEvent> clicks,
            String lookupTraceId,
            Instant coveredFromInclusive,
            Instant coveredToInclusive,
            Instant indexWatermark) {
        Objects.requireNonNull(indexWatermark, "authoritative snapshots require indexWatermark");
        return new CandidateSnapshot(
                clicks,
                new CandidateEvidence(
                        CandidateReadStatus.AUTHORITATIVE,
                        lookupTraceId,
                        coveredFromInclusive,
                        coveredToInclusive,
                        indexWatermark,
                        false,
                        clicks.size()));
    }

    public static CandidateSnapshot unavailable(CandidateReadStatus status, String lookupTraceId) {
        if (status.isAuthoritative()) {
            throw new IllegalArgumentException("authoritative snapshots must use authoritative()");
        }
        if (status == CandidateReadStatus.PARTIAL || status == CandidateReadStatus.TRUNCATED) {
            throw new IllegalArgumentException(
                    status + " snapshots must declare candidate and coverage evidence explicitly");
        }
        return new CandidateSnapshot(
                List.of(),
                new CandidateEvidence(status, lookupTraceId, null, null, null, false, 0));
    }

    public boolean isAuthoritativeFor(Instant requiredFromInclusive, Instant requiredToInclusive) {
        Objects.requireNonNull(requiredFromInclusive, "requiredFromInclusive");
        Objects.requireNonNull(requiredToInclusive, "requiredToInclusive");
        return evidence.status().isAuthoritative()
                && !evidence.truncated()
                && evidence.returnedCount() == clicks.size()
                && !evidence.coveredFromInclusive().isAfter(requiredFromInclusive)
                && !evidence.coveredToInclusive().isBefore(requiredToInclusive)
                && !evidence.indexWatermark().isBefore(requiredToInclusive);
    }
}
