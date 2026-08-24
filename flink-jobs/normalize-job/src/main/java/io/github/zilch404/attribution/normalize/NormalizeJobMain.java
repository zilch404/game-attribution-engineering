package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaSinkSettings;
import io.github.zilch404.attribution.kafka.KafkaSourceSettings;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/** Deployable raw-Kafka to standard-Kafka normalization application. */
public final class NormalizeJobMain {
    public static final String JOB_NAME = "game-attribution-normalize-v1";

    private NormalizeJobMain() {
    }

    public static void main(String[] arguments) throws Exception {
        NormalizeJobConfig config = NormalizeJobArguments.parse(arguments);
        config.requireExplicitPlaceholderOptIn();

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(environment, config);
        attachTopology(environment, config);
        environment.execute(JOB_NAME);
    }

    static void configureEnvironment(
            StreamExecutionEnvironment environment,
            NormalizeJobConfig config) {
        environment.setParallelism(config.parallelism());
        environment.enableCheckpointing(
                config.checkpointInterval().toMillis(),
                CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(config.checkpointTimeout().toMillis());
        checkpointConfig.setMinPauseBetweenCheckpoints(
                config.checkpointMinPause().toMillis());
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        Configuration checkpointStorage = new Configuration();
        checkpointStorage.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        checkpointStorage.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                config.checkpointStorage());
        environment.configure(checkpointStorage);
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    }

    static void attachTopology(
            StreamExecutionEnvironment environment,
            NormalizeJobConfig config) {
        KafkaSourceSettings clickSource = new KafkaSourceSettings(
                config.bootstrapServers(),
                List.of(config.rawClickTopic()),
                config.clickGroupId(),
                config.startPosition(),
                config.consumerProperties());
        KafkaSourceSettings activationSource = new KafkaSourceSettings(
                config.bootstrapServers(),
                List.of(config.rawActivationTopic()),
                config.activationGroupId(),
                config.startPosition(),
                config.consumerProperties());

        NormalizedEventStreams streams = NormalizeTopology.attach(
                environment,
                new NormalizeTopologySettings(
                        clickSource,
                        activationSource,
                        config.maxFutureClockSkew()),
                ConfiguredIdentityNormalizationPolicy.gameAttributionV1());
        NormalizationKafkaSink.attach(
                streams,
                config.outputTopics(),
                new KafkaSinkSettings(
                        config.bootstrapServers(),
                        config.transactionalIdPrefix(),
                        config.transactionTimeout(),
                        config.producerProperties()));
    }
}
