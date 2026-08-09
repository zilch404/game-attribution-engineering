package io.github.zilch404.attribution.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record CandidateEvidence(
        CandidateReadStatus status,
        String lookupTraceId,
        Instant coveredFromInclusive,
        Instant coveredToInclusive,
        Instant indexWatermark,
        boolean truncated,
        int returnedCount) implements Serializable {

    public CandidateEvidence {
        status = Objects.requireNonNull(status, "status");
        lookupTraceId = Objects.requireNonNullElse(lookupTraceId, "");
        if ((coveredFromInclusive == null) != (coveredToInclusive == null)) {
            throw new IllegalArgumentException("coverage bounds must both be present or absent");
        }
        if (coveredFromInclusive != null && coveredFromInclusive.isAfter(coveredToInclusive)) {
            throw new IllegalArgumentException("coveredFromInclusive must not follow coveredToInclusive");
        }
        if (returnedCount < 0) {
            throw new IllegalArgumentException("returnedCount must not be negative");
        }
        if (status == CandidateReadStatus.AUTHORITATIVE
                && (coveredFromInclusive == null || indexWatermark == null || truncated)) {
            throw new IllegalArgumentException(
                    "authoritative evidence requires coverage, watermark, and no truncation");
        }
        if (status == CandidateReadStatus.TRUNCATED && !truncated) {
            throw new IllegalArgumentException("TRUNCATED status requires truncated=true");
        }
        if (status != CandidateReadStatus.TRUNCATED && truncated) {
            throw new IllegalArgumentException("truncated=true requires TRUNCATED status");
        }
    }
}
