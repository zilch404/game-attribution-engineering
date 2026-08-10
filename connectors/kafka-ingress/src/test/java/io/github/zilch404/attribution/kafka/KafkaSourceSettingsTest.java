package io.github.zilch404.attribution.kafka;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class KafkaSourceSettingsTest {

    @Test
    void copiesTopicsAndConsumerProperties() {
        List<String> topics = new java.util.ArrayList<>(List.of("clicks", "player-events"));
        Map<String, String> properties = new HashMap<>(Map.of(
                "security.protocol", "SASL_SSL",
                "sasl.jaas.config", "secret"));

        KafkaSourceSettings settings = new KafkaSourceSettings(
                "kafka-1:9092,kafka-2:9092",
                topics,
                "normalize-v1",
                KafkaStartPosition.committedOrEarliest(),
                properties);
        topics.clear();
        properties.clear();

        assertThat(settings.topics()).containsExactly("clicks", "player-events");
        assertThat(settings.consumerProperties()).containsEntry("security.protocol", "SASL_SSL");
        assertThat(settings.toString()).contains("sasl.jaas.config").doesNotContain("secret");
    }

    @Test
    void rejectsAmbiguousOrIncompleteSourceConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(() -> settings(" ", List.of("clicks")));
        assertThatIllegalArgumentException().isThrownBy(() -> settings("kafka:9092", List.of()));
        assertThatIllegalArgumentException().isThrownBy(() -> settings("kafka:9092", List.of(" ")));
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings("kafka:9092", List.of("clicks", "clicks")));
        assertThatIllegalArgumentException().isThrownBy(() -> new KafkaSourceSettings(
                "kafka:9092", List.of("clicks"), " ", KafkaStartPosition.earliest()));
        assertThatNullPointerException().isThrownBy(() -> new KafkaSourceSettings(
                "kafka:9092", List.of("clicks"), "normalize-v1", null));
    }

    @Test
    void rejectsPropertiesThatWouldOverrideFactorySemantics() {
        assertThatIllegalArgumentException().isThrownBy(() -> new KafkaSourceSettings(
                        "kafka:9092",
                        List.of("clicks"),
                        "normalize-v1",
                        KafkaStartPosition.committedOrEarliest(),
                        Map.of("group.id", "another-group")))
                .withMessageContaining("factory-managed property");
        assertThatIllegalArgumentException().isThrownBy(() -> new KafkaSourceSettings(
                        "kafka:9092",
                        List.of("clicks"),
                        "normalize-v1",
                        KafkaStartPosition.committedOrEarliest(),
                        Map.of("isolation.level", "read_uncommitted")))
                .withMessageContaining("factory-managed property");
    }

    @Test
    void exposesOnlyExplicitSupportedStartPositions() {
        assertThat(KafkaStartPosition.committedOrEarliest().kind())
                .isEqualTo(KafkaStartPosition.Kind.COMMITTED_OR_EARLIEST);
        assertThat(KafkaStartPosition.earliest().kind())
                .isEqualTo(KafkaStartPosition.Kind.EARLIEST);
        assertThat(KafkaStartPosition.timestamp(Instant.ofEpochMilli(123)).timestampMillis())
                .isEqualTo(123L);
        assertThatIllegalArgumentException().isThrownBy(() -> KafkaStartPosition.timestamp(-1));
    }

    @Test
    void buildsKafkaSourceWithoutConnectingToABroker() {
        KafkaSourceSettings settings = new KafkaSourceSettings(
                "kafka:9092",
                List.of("clicks"),
                "normalize-v1",
                KafkaStartPosition.committedOrEarliest(),
                Map.of("fetch.max.bytes", "1048576"));

        assertThat(KafkaSourceFactory.create(settings)).isNotNull();
    }

    private static KafkaSourceSettings settings(String servers, List<String> topics) {
        return new KafkaSourceSettings(
                servers, topics, "normalize-v1", KafkaStartPosition.committedOrEarliest());
    }
}
