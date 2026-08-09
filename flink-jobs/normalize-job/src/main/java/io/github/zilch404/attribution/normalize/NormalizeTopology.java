package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaInputRecord;
import io.github.zilch404.attribution.kafka.KafkaSourceFactory;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public final class NormalizeTopology {
    private NormalizeTopology() {
    }

    public static NormalizedEventStreams attach(
            StreamExecutionEnvironment environment,
            NormalizeTopologySettings settings,
            IdentityNormalizationPolicy identityPolicy) {
        Objects.requireNonNull(environment, "environment");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(identityPolicy, "identityPolicy");
        if (!environment.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "checkpointing must be enabled before attaching Kafka normalization sources");
        }

        DataStream<KafkaInputRecord> clickInput = environment.fromSource(
                        KafkaSourceFactory.create(settings.clickSource()),
                        WatermarkStrategy.noWatermarks(),
                        "raw-click-kafka-source")
                .uid("raw-click-kafka-source-v1");
        DataStream<KafkaInputRecord> activationInput = environment.fromSource(
                        KafkaSourceFactory.create(settings.activationSource()),
                        WatermarkStrategy.noWatermarks(),
                        "raw-activation-kafka-source")
                .uid("raw-activation-kafka-source-v1");

        SingleOutputStreamOperator<StandardClickEvent> normalizedClicks = clickInput
                .process(new ClickNormalizationProcessFunction(
                        settings.clickTopic(), identityPolicy, settings.maxFutureClockSkew()))
                .name("normalize-click-v1")
                .uid("normalize-click-v1");
        SingleOutputStreamOperator<StandardActivationEvent> normalizedActivations = activationInput
                .process(new ActivationNormalizationProcessFunction(
                        settings.activationTopic(), identityPolicy, settings.maxFutureClockSkew()))
                .name("normalize-activation-v1")
                .uid("normalize-activation-v1");

        DataStream<RejectedRecord> rejectedRecords = normalizedClicks
                .getSideOutput(NormalizationOutputTags.REJECTED_RECORDS)
                .union(normalizedActivations.getSideOutput(
                        NormalizationOutputTags.REJECTED_RECORDS));
        DataStream<IdentityNormalizationIssue> identityIssues = normalizedClicks
                .getSideOutput(NormalizationOutputTags.IDENTITY_ISSUES)
                .union(normalizedActivations.getSideOutput(
                        NormalizationOutputTags.IDENTITY_ISSUES));

        return new NormalizedEventStreams(
                normalizedClicks,
                normalizedActivations,
                rejectedRecords,
                identityIssues);
    }
}
