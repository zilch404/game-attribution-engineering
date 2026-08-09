package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Objects;

public record NormalizedEventStreams(
        DataStream<StandardClickEvent> clicks,
        DataStream<StandardActivationEvent> activations,
        DataStream<RejectedRecord> rejectedRecords,
        DataStream<IdentityNormalizationIssue> identityIssues) {

    public NormalizedEventStreams {
        clicks = Objects.requireNonNull(clicks, "clicks");
        activations = Objects.requireNonNull(activations, "activations");
        rejectedRecords = Objects.requireNonNull(rejectedRecords, "rejectedRecords");
        identityIssues = Objects.requireNonNull(identityIssues, "identityIssues");
    }
}
