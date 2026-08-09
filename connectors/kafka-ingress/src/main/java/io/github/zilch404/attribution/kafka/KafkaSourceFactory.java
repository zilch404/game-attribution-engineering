package io.github.zilch404.attribution.kafka;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Objects;
import java.util.Properties;

/** Builds the bounded configuration surface used by attribution Kafka consumers. */
public final class KafkaSourceFactory {
    private KafkaSourceFactory() {
    }

    public static KafkaSource<KafkaInputRecord> create(KafkaSourceSettings settings) {
        Objects.requireNonNull(settings, "settings");

        Properties properties = new Properties();
        properties.putAll(settings.consumerProperties());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.setProperty("client.id.prefix", settings.groupId());
        properties.setProperty("commit.offsets.on.checkpoint", "true");

        return KafkaSource.<KafkaInputRecord>builder()
                .setProperties(properties)
                .setBootstrapServers(settings.bootstrapServers())
                .setTopics(settings.topics())
                .setGroupId(settings.groupId())
                .setStartingOffsets(settings.startPosition().toOffsetsInitializer())
                .setDeserializer(new KafkaInputRecordDeserializationSchema())
                .build();
    }
}
