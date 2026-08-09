package io.github.zilch404.attribution.kafka;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaInputRecordDeserializationSchemaTest {

    @Test
    void preservesRecordContentAndConsumerMetadata() throws Exception {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                "raw-events",
                3,
                91L,
                1_700_000_000_123L,
                TimestampType.CREATE_TIME,
                2,
                3,
                new byte[]{1, 2},
                new byte[]{3, 4, 5},
                new RecordHeaders(List.of(
                        new RecordHeader("trace", new byte[]{6}),
                        new RecordHeader("trace", new byte[]{7}))),
                Optional.of(8));
        List<KafkaInputRecord> output = new ArrayList<>();

        new KafkaInputRecordDeserializationSchema().deserialize(
                consumerRecord, collectingInto(output));

        assertThat(output).singleElement().satisfies(record -> {
            assertThat(record.sourcePosition().topic()).isEqualTo("raw-events");
            assertThat(record.sourcePosition().partition()).isEqualTo(3);
            assertThat(record.sourcePosition().offset()).isEqualTo(91L);
            assertThat(record.sourcePosition().recordTimestamp()).isEqualTo(1_700_000_000_123L);
            assertThat(record.sourcePosition().timestampType()).isEqualTo("CREATE_TIME");
            assertThat(record.key()).containsExactly(1, 2);
            assertThat(record.payload()).containsExactly(3, 4, 5);
            assertThat(record.headers()).extracting(KafkaInputRecord.Header::key)
                    .containsExactly("trace", "trace");
            assertThat(record.headers()).extracting(header -> header.value()[0])
                    .containsExactly((byte) 6, (byte) 7);
            assertThat(record.serializedKeySize()).isEqualTo(2);
            assertThat(record.serializedValueSize()).isEqualTo(3);
            assertThat(record.leaderEpoch()).isEqualTo(8);
        });
    }

    @Test
    void preservesNullKeysAndTombstonePayloads() throws Exception {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                "raw-events", 0, 0L, null, null);
        List<KafkaInputRecord> output = new ArrayList<>();

        new KafkaInputRecordDeserializationSchema().deserialize(
                consumerRecord, collectingInto(output));

        assertThat(output).singleElement().satisfies(record -> {
            assertThat(record.key()).isNull();
            assertThat(record.payload()).isNull();
            assertThat(record.serializedKeySize()).isEqualTo(-1);
            assertThat(record.serializedValueSize()).isEqualTo(-1);
        });
    }

    private static Collector<KafkaInputRecord> collectingInto(List<KafkaInputRecord> output) {
        return new Collector<>() {
            @Override
            public void collect(KafkaInputRecord record) {
                output.add(record);
            }

            @Override
            public void close() {
                // Nothing to release.
            }
        };
    }
}
