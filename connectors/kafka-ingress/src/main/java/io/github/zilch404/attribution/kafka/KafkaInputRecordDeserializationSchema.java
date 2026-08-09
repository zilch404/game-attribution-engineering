package io.github.zilch404.attribution.kafka;

import io.github.zilch404.attribution.model.SourcePosition;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

/** Preserves Kafka record content and source metadata for downstream validation and replay. */
public final class KafkaInputRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<KafkaInputRecord> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaInputRecord> out) {
        List<KafkaInputRecord.Header> headers = new ArrayList<>();
        record.headers().forEach(header -> headers.add(
                new KafkaInputRecord.Header(header.key(), header.value())));

        SourcePosition sourcePosition = new SourcePosition(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType().name());

        out.collect(new KafkaInputRecord(
                sourcePosition,
                record.key(),
                record.value(),
                headers,
                record.serializedKeySize(),
                record.serializedValueSize(),
                record.leaderEpoch().orElse(null)));
    }

    @Override
    public TypeInformation<KafkaInputRecord> getProducedType() {
        return TypeInformation.of(KafkaInputRecord.class);
    }
}
