package io.github.zilch404.attribution.kafka;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class KafkaSinkSettingsTest {

    @Test
    void copiesProducerPropertiesWithoutLoggingValues() {
        Map<String, String> properties = new HashMap<>(Map.of(
                "security.protocol", "SASL_SSL",
                "sasl.jaas.config", "secret"));

        KafkaSinkSettings settings = new KafkaSinkSettings(
                "kafka-1:9092,kafka-2:9092",
                "normalize-v1-prod-",
                Duration.ofMinutes(20),
                properties);
        properties.clear();

        assertThat(settings.producerProperties())
                .containsEntry("security.protocol", "SASL_SSL");
        assertThat(settings.toString())
                .contains("transactionalIdPrefix=normalize-v1-prod-", "sasl.jaas.config")
                .doesNotContain("secret");
    }

    @Test
    void rejectsIncompleteSinkConfiguration() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings(" ", "normalize-v1-", Duration.ofMinutes(1)));
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings("kafka:9092", " ", Duration.ofMinutes(1)));
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings("kafka:9092", "x".repeat(129), Duration.ofMinutes(1)));
        assertThatIllegalArgumentException().isThrownBy(() -> settings(
                "kafka:9092", "x".repeat(64_001), Duration.ofMinutes(1)));
        assertThatNullPointerException().isThrownBy(
                () -> settings("kafka:9092", "normalize-v1-", null));
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings("kafka:9092", "normalize-v1-", Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(
                () -> settings("kafka:9092", "normalize-v1-", Duration.ofNanos(1)));
        assertThatIllegalArgumentException().isThrownBy(() -> settings(
                "kafka:9092",
                "normalize-v1-",
                Duration.ofMillis((long) Integer.MAX_VALUE + 1)));
    }

    @Test
    void rejectsPropertiesThatCouldOverrideExactlyOnceSemantics() {
        for (String property : new String[]{
                "bootstrap.servers",
                "key.serializer",
                "value.serializer",
                "transactional.id",
                "transaction.timeout.ms",
                "enable.idempotence",
                "acks",
                "retries",
                "max.in.flight.requests.per.connection"}) {
            assertThatIllegalArgumentException().isThrownBy(() -> new KafkaSinkSettings(
                            "kafka:9092",
                            "normalize-v1-",
                            Duration.ofMinutes(1),
                            Map.of(property, "unsafe")))
                    .withMessageContaining("factory-managed property")
                    .withMessageContaining(property);
        }
    }

    @Test
    void buildsExactlyOnceKafkaSinkWithoutConnectingToABroker() {
        KafkaSinkSettings settings = new KafkaSinkSettings(
                "kafka:9092",
                "normalize-v1-test-",
                Duration.ofMinutes(20),
                Map.of("compression.type", "zstd"));

        assertThat(KafkaSinkFactory.create(settings)).isNotNull();
    }

    private static KafkaSinkSettings settings(
            String bootstrapServers,
            String transactionalIdPrefix,
            Duration transactionTimeout) {
        return new KafkaSinkSettings(
                bootstrapServers, transactionalIdPrefix, transactionTimeout);
    }
}
