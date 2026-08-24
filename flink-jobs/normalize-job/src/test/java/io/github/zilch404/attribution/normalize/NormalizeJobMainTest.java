package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaStartPosition;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NormalizeJobMainTest {

    @Test
    void buildsACompleteGraphWithOneCrossTopicKafkaSinkWithoutACluster() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        NormalizeJobConfig config = config();

        NormalizeJobMain.configureEnvironment(environment, config);
        NormalizeJobMain.attachTopology(environment, config);

        assertThat(environment.getCheckpointConfig().isCheckpointingEnabled()).isTrue();
        assertThat(environment.getCheckpointConfig().getCheckpointInterval())
                .isEqualTo(30_000L);
        assertThat(environment.getCheckpointConfig().getCheckpointingConsistencyMode())
                .isEqualTo(CheckpointingMode.EXACTLY_ONCE);
        assertThat(environment.getCheckpointConfig().getCheckpointTimeout())
                .isEqualTo(120_000L);
        assertThat(environment.getCheckpointConfig().getMinPauseBetweenCheckpoints())
                .isEqualTo(10_000L);
        assertThat(environment.getCheckpointConfig().getMaxConcurrentCheckpoints())
                .isEqualTo(1);
        assertThat(environment.getCheckpointConfig().getExternalizedCheckpointRetention())
                .isEqualTo(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        assertThat(environment.getConfiguration().get(
                CheckpointingOptions.CHECKPOINT_STORAGE)).isEqualTo("filesystem");
        assertThat(environment.getConfiguration().get(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY))
                .isEqualTo(config.checkpointStorage());
        assertThat(environment.getParallelism()).isEqualTo(2);
        assertThat(environment.getTransformations())
                .filteredOn(transformation ->
                        "normalization-kafka-sink".equals(transformation.getName()))
                .hasSize(1);

        String executionPlan = environment.getExecutionPlan();
        assertThat(executionPlan)
                .contains("raw-click-kafka-source")
                .contains("raw-activation-kafka-source")
                .contains("encode-standard-click-v1")
                .contains("encode-standard-activation-v1")
                .contains("encode-normalization-rejected-v1")
                .contains("encode-identity-normalization-issue-v1")
                .contains("normalization-kafka-sink");
    }

    private static NormalizeJobConfig config() {
        return new NormalizeJobConfig(
                "kafka:9092",
                "game-attribution.raw.click.v1",
                "game-attribution.raw.activation.v1",
                "normalize-click-v1",
                "normalize-activation-v1",
                KafkaStartPosition.committedOrEarliest(),
                new NormalizationOutputTopics(
                        "game-attribution.standard.click.v1",
                        "game-attribution.standard.activation.v1",
                        "game-attribution.normalization.invalid.v1",
                        "game-attribution.normalization.identity-issue.v1"),
                "gae-normalize-test-v1-",
                "file:///tmp/game-attribution-checkpoints",
                Duration.ofSeconds(30),
                Duration.ofMinutes(2),
                Duration.ofSeconds(10),
                Duration.ofMinutes(10),
                Duration.ofMinutes(15),
                Duration.ofMinutes(5),
                2,
                true,
                true,
                Map.of(),
                Map.of());
    }
}
