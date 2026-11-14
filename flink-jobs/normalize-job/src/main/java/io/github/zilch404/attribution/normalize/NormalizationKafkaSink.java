package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaOutputRecord;
import io.github.zilch404.attribution.kafka.KafkaSinkFactory;
import io.github.zilch404.attribution.kafka.KafkaSinkSettings;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

/** Encodes all normalization outcomes before attaching one cross-topic transaction sink. */
public final class NormalizationKafkaSink {
    public static final String SINK_UID = "normalization-kafka-sink-v1";

    private NormalizationKafkaSink() {
    }

    public static DataStreamSink<KafkaOutputRecord> attach(
            NormalizedEventStreams streams,
            NormalizationOutputTopics topics,
            KafkaSinkSettings sinkSettings) {
        Objects.requireNonNull(streams, "streams");
        Objects.requireNonNull(topics, "topics");
        Objects.requireNonNull(sinkSettings, "sinkSettings");
        if (!streams.clicks().getExecutionEnvironment()
                .getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "checkpointing must be enabled before attaching the exactly-once Kafka sink");
        }

        NormalizationKafkaEncoder encoder = new NormalizationKafkaEncoder(topics);
        TypeInformation<KafkaOutputRecord> outputType =
                TypeInformation.of(KafkaOutputRecord.class);

        SingleOutputStreamOperator<KafkaOutputRecord> clicks = streams.clicks()
                .map(encoder::encodeClick)
                .returns(outputType)
                .name("encode-standard-click-v1")
                .uid("encode-standard-click-v1");
        SingleOutputStreamOperator<KafkaOutputRecord> activations = streams.activations()
                .map(encoder::encodeActivation)
                .returns(outputType)
                .name("encode-standard-activation-v1")
                .uid("encode-standard-activation-v1");
        SingleOutputStreamOperator<KafkaOutputRecord> rejectedRecords = streams.rejectedRecords()
                .map(encoder::encodeRejectedRecord)
                .returns(outputType)
                .name("encode-normalization-rejected-v1")
                .uid("encode-normalization-rejected-v1");
        SingleOutputStreamOperator<KafkaOutputRecord> identityIssues = streams.identityIssues()
                .map(encoder::encodeIdentityIssue)
                .returns(outputType)
                .name("encode-identity-normalization-issue-v1")
                .uid("encode-identity-normalization-issue-v1");

        DataStream<KafkaOutputRecord> allOutputs = clicks
                .union(activations, rejectedRecords, identityIssues);
        return allOutputs
                .sinkTo(KafkaSinkFactory.create(sinkSettings))
                .name("normalization-kafka-sink")
                .uid(SINK_UID);
    }
}
