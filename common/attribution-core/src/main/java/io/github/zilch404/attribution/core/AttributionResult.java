package io.github.zilch404.attribution.core;

import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.IdentityKey;

import java.io.Serializable;
import java.util.Objects;

public record AttributionResult(
        AttributionOutcome outcome,
        DecisionStatus status,
        String matchedClickId,
        IdentityKey matchedIdentity,
        CampaignRef campaign,
        DecisionReason reason) implements Serializable {

    public AttributionResult {
        outcome = Objects.requireNonNull(outcome, "outcome");
        status = Objects.requireNonNull(status, "status");
        reason = Objects.requireNonNull(reason, "reason");

        if (outcome == AttributionOutcome.ATTRIBUTED) {
            if (matchedClickId == null || matchedClickId.isBlank()) {
                throw new IllegalArgumentException("attributed results require matchedClickId");
            }
            Objects.requireNonNull(matchedIdentity, "attributed results require matchedIdentity");
            Objects.requireNonNull(campaign, "attributed results require campaign");
            if (status == DecisionStatus.RETRYABLE || status == DecisionStatus.QUARANTINED) {
                throw new IllegalArgumentException("attributed results must be provisional or final");
            }
            if (reason != DecisionReason.MATCHED) {
                throw new IllegalArgumentException("attributed results require MATCHED reason");
            }
        } else if (matchedClickId != null || matchedIdentity != null || campaign != null) {
            throw new IllegalArgumentException("non-attributed results must not contain a matched click");
        }

        if (outcome == AttributionOutcome.ORGANIC && status != DecisionStatus.FINAL) {
            throw new IllegalArgumentException("organic results must be final");
        }
        if (outcome == AttributionOutcome.ORGANIC && reason != DecisionReason.NO_ELIGIBLE_CLICK) {
            throw new IllegalArgumentException("organic results require NO_ELIGIBLE_CLICK reason");
        }
        if (outcome == AttributionOutcome.UNKNOWN
                && status != DecisionStatus.RETRYABLE
                && status != DecisionStatus.QUARANTINED) {
            throw new IllegalArgumentException("unknown results must be retryable or quarantined");
        }
        if (outcome == AttributionOutcome.UNKNOWN
                && (reason == DecisionReason.MATCHED || reason == DecisionReason.NO_ELIGIBLE_CLICK)) {
            throw new IllegalArgumentException("unknown results require an incomplete lookup reason");
        }
        boolean quarantineReason = reason == DecisionReason.NO_SUPPORTED_IDENTITY
                || reason == DecisionReason.LOOKUP_PERMANENT_FAILURE
                || reason == DecisionReason.CONFLICTING_DUPLICATE;
        if (outcome == AttributionOutcome.UNKNOWN
                && quarantineReason != (status == DecisionStatus.QUARANTINED)) {
            throw new IllegalArgumentException(
                    "unknown status must agree with whether its reason is quarantinable");
        }
    }
}
