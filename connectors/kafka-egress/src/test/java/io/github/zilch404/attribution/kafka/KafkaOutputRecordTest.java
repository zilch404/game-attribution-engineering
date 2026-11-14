package io.github.zilch404.attribution.kafka;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class KafkaOutputRecordTest {

    @Test
    void defensivelyCopiesKeyAndPayload() {
        byte[] key = {1, 2};
        byte[] payload = {3, 4};

        KafkaOutputRecord record = new KafkaOutputRecord("normalized-clicks", key, payload);
        key[0] = 99;
        payload[0] = 99;
        record.key()[1] = 99;
        record.payload()[1] = 99;

        assertThat(record.key()).containsExactly(1, 2);
        assertThat(record.payload()).containsExactly(3, 4);
    }

    @Test
    void supportsKafkaNullKeysAndTombstonePayloads() {
        KafkaOutputRecord record = new KafkaOutputRecord("compacted-events", null, null);

        assertThat(record.key()).isNull();
        assertThat(record.payload()).isNull();
        assertThat(record.toString()).contains("keyLength=-1", "payloadLength=-1");
    }

    @Test
    void rejectsBlankTopic() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> new KafkaOutputRecord(" ", new byte[]{1}, new byte[]{2}));
    }

    @Test
    void toStringReportsOnlyLengthsInsteadOfRecordContent() {
        KafkaOutputRecord record = new KafkaOutputRecord(
                "normalized-clicks", "secret-key".getBytes(), "secret-payload".getBytes());

        assertThat(record.toString())
                .contains("topic=normalized-clicks", "keyLength=10", "payloadLength=14")
                .doesNotContain("secret-key", "secret-payload");
    }

    @Test
    void isJavaSerializableForFlinkTransport() throws Exception {
        KafkaOutputRecord original = new KafkaOutputRecord(
                "normalized-clicks", new byte[]{1}, new byte[]{2});

        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
            serialized = bytes.toByteArray();
        }

        KafkaOutputRecord restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            restored = (KafkaOutputRecord) input.readObject();
        }

        assertThat(restored).isEqualTo(original);
    }
}
