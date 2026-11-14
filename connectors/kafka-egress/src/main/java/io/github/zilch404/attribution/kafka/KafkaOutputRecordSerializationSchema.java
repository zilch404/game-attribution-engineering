package io.github.zilch404.attribution.kafka;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serial;
import java.util.Objects;

/** Converts {@link KafkaOutputRecord} instances into byte-oriented Kafka producer records. */
public final class KafkaOutputRecordSerializationSchema
        implements KafkaRecordSerializationSchema<KafkaOutputRecord> {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            KafkaOutputRecord record, KafkaSinkContext context, Long timestamp) {
        Objects.requireNonNull(record, "record");
        // Let Kafka assign the write timestamp according to the topic timestamp policy. Reusing
        // Flink event time here could make replayed records appear expired immediately.
        return new ProducerRecord<>(
                record.topic(),
                null,
                null,
                record.key(),
                record.payload());
    }
}
