package io.github.zilch404.attribution.normalize;

import org.apache.flink.util.OutputTag;

public final class NormalizationOutputTags {
    public static final OutputTag<RejectedRecord> REJECTED_RECORDS =
            new OutputTag<>("normalization-rejected-records") { };
    public static final OutputTag<IdentityNormalizationIssue> IDENTITY_ISSUES =
            new OutputTag<>("normalization-identity-issues") { };

    private NormalizationOutputTags() {
    }
}
