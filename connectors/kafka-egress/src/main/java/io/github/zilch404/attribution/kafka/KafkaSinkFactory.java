package io.github.zilch404.attribution.kafka;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Objects;
import java.util.Properties;

/** Builds the exactly-once Kafka sink used by attribution jobs. */
public final class KafkaSinkFactory {
    private KafkaSinkFactory() {
    }

    public static KafkaSink<KafkaOutputRecord> create(KafkaSinkSettings settings) {
        Objects.requireNonNull(settings, "settings");

        Properties properties = new Properties();
        properties.putAll(settings.producerProperties());
        properties.setProperty(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                Long.toString(settings.transactionTimeout().toMillis()));

        return KafkaSink.<KafkaOutputRecord>builder()
                .setKafkaProducerConfig(properties)
                .setBootstrapServers(settings.bootstrapServers())
                .setRecordSerializer(new KafkaOutputRecordSerializationSchema())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(settings.transactionalIdPrefix())
                .build();
    }
}
