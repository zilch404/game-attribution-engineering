package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaInputRecord;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Objects;

public final class ActivationNormalizationProcessFunction
        extends ProcessFunction<KafkaInputRecord, StandardActivationEvent> {

    private final String expectedTopic;
    private final EventNormalizer normalizer;

    public ActivationNormalizationProcessFunction(
            String expectedTopic,
            IdentityNormalizationPolicy identityPolicy,
            Duration maxFutureClockSkew) {
        if (expectedTopic == null || expectedTopic.isBlank()) {
            throw new IllegalArgumentException("expectedTopic must not be blank");
        }
        this.expectedTopic = expectedTopic;
        this.normalizer = new EventNormalizer(
                Objects.requireNonNull(identityPolicy, "identityPolicy"),
                maxFutureClockSkew);
    }

    @Override
    public void processElement(
            KafkaInputRecord value,
            ProcessFunction<KafkaInputRecord, StandardActivationEvent>.Context context,
            Collector<StandardActivationEvent> output) {
        NormalizationOutcome<StandardActivationEvent> outcome =
                normalizer.normalizeActivation(value, expectedTopic);
        if (outcome instanceof NormalizationOutcome.Accepted<?> accepted) {
            accepted.identityIssues().forEach(issue ->
                    context.output(NormalizationOutputTags.IDENTITY_ISSUES, issue));
            output.collect((StandardActivationEvent) accepted.event());
            return;
        }
        NormalizationOutcome.Rejected<?> rejected =
                (NormalizationOutcome.Rejected<?>) outcome;
        context.output(NormalizationOutputTags.REJECTED_RECORDS, rejected.rejection());
    }
}
