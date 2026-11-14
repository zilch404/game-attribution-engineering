package io.github.zilch404.attribution.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class KafkaOutputRecordSerializationSchemaTest {

    @Test
    void preservesDestinationKeyAndPayloadButLetsKafkaAssignTimestamp() {
        KafkaOutputRecord output = new KafkaOutputRecord(
                "normalized-activations", new byte[]{1, 2}, new byte[]{3, 4});

        ProducerRecord<byte[], byte[]> producerRecord =
                new KafkaOutputRecordSerializationSchema().serialize(output, null, 123L);

        assertThat(producerRecord.topic()).isEqualTo("normalized-activations");
        assertThat(producerRecord.partition()).isNull();
        assertThat(producerRecord.timestamp()).isNull();
        assertThat(producerRecord.key()).containsExactly(1, 2);
        assertThat(producerRecord.value()).containsExactly(3, 4);
    }

    @Test
    void preservesNullTimestampAndKafkaTombstone() {
        KafkaOutputRecord output = new KafkaOutputRecord("compacted-events", null, null);

        ProducerRecord<byte[], byte[]> producerRecord =
                new KafkaOutputRecordSerializationSchema().serialize(output, null, null);

        assertThat(producerRecord.timestamp()).isNull();
        assertThat(producerRecord.key()).isNull();
        assertThat(producerRecord.value()).isNull();
    }

    @Test
    void rejectsNullRecord() {
        assertThatNullPointerException().isThrownBy(
                () -> new KafkaOutputRecordSerializationSchema().serialize(null, null, null));
    }
}
