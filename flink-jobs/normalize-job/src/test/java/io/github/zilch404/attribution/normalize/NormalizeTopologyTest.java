package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaSourceSettings;
import io.github.zilch404.attribution.kafka.KafkaStartPosition;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NormalizeTopologyTest {

    @Test
    void buildsTwoTypedSourcesWithoutConnectingToKafka() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10_000);

        NormalizedEventStreams streams = NormalizeTopology.attach(
                environment,
                settings("normalize-click-v1", "normalize-activation-v1"),
                ConfiguredIdentityNormalizationPolicy.gameAttributionV1());

        assertThat(streams.clicks().getType().getTypeClass())
                .isEqualTo(StandardClickEvent.class);
        assertThat(streams.activations().getType().getTypeClass())
                .isEqualTo(StandardActivationEvent.class);
        assertThat(streams.rejectedRecords()).isNotNull();
        assertThat(streams.identityIssues()).isNotNull();
    }

    @Test
    void requiresIndependentConsumerGroupsAndTopics() {
        assertThatThrownBy(() -> settings("same-group", "same-group"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumer groups");

        KafkaSourceSettings sameTopic = new KafkaSourceSettings(
                "kafka:9092",
                List.of("game-attribution.raw.click.v1"),
                "activation-group",
                KafkaStartPosition.earliest());
        assertThatThrownBy(() -> new NormalizeTopologySettings(
                        clickSettings("click-group"),
                        sameTopic,
                        Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topics");
    }

    @Test
    void refusesAProductionSourceWithoutCheckpointing() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        assertThatThrownBy(() -> NormalizeTopology.attach(
                        environment,
                        settings("normalize-click-v1", "normalize-activation-v1"),
                        ConfiguredIdentityNormalizationPolicy.gameAttributionV1()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("checkpointing");
    }

    private static NormalizeTopologySettings settings(
            String clickGroup,
            String activationGroup) {
        return new NormalizeTopologySettings(
                clickSettings(clickGroup),
                new KafkaSourceSettings(
                        "kafka:9092",
                        List.of("game-attribution.raw.activation.v1"),
                        activationGroup,
                        KafkaStartPosition.earliest()),
                Duration.ofMinutes(5));
    }

    private static KafkaSourceSettings clickSettings(String group) {
        return new KafkaSourceSettings(
                "kafka:9092",
                List.of("game-attribution.raw.click.v1"),
                group,
                KafkaStartPosition.earliest());
    }
}
