package io.github.zilch404.attribution.kafka;

import io.github.zilch404.attribution.model.SourcePosition;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaInputRecordTest {

    @Test
    void defensivelyCopiesKeyPayloadAndHeaderValues() {
        byte[] key = {1, 2};
        byte[] payload = {3, 4};
        byte[] headerValue = {5, 6};

        KafkaInputRecord record = new KafkaInputRecord(
                position(),
                key,
                payload,
                List.of(new KafkaInputRecord.Header("trace-id", headerValue)),
                key.length,
                payload.length,
                9);

        key[0] = 99;
        payload[0] = 99;
        headerValue[0] = 99;
        record.key()[1] = 99;
        record.payload()[1] = 99;
        record.headers().get(0).value()[1] = 99;

        assertThat(record.key()).containsExactly(1, 2);
        assertThat(record.payload()).containsExactly(3, 4);
        assertThat(record.headers().get(0).value()).containsExactly(5, 6);
    }

    @Test
    void toStringReportsOnlyLengthsInsteadOfRecordContent() {
        KafkaInputRecord record = new KafkaInputRecord(
                position(),
                "secret-key".getBytes(),
                "secret-payload".getBytes(),
                List.of(new KafkaInputRecord.Header("authorization", "secret-header".getBytes())),
                10,
                14,
                null);

        assertThat(record.toString())
                .contains("keyLength=10", "payloadLength=14", "headerCount=1")
                .doesNotContain(
                        "secret-key", "secret-payload", "secret-header", "authorization");
    }

    @Test
    void isJavaSerializableForFlinkTransport() throws Exception {
        KafkaInputRecord original = new KafkaInputRecord(
                position(),
                new byte[]{1},
                new byte[]{2},
                List.of(new KafkaInputRecord.Header("schema", new byte[]{3})),
                1,
                1,
                4);

        byte[] serialized;
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
            serialized = bytes.toByteArray();
        }

        KafkaInputRecord restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            restored = (KafkaInputRecord) input.readObject();
        }

        assertThat(restored).isEqualTo(original);
    }

    private static SourcePosition position() {
        return new SourcePosition("raw-events", 2, 42, 1_700_000_000_000L, "CREATE_TIME");
    }
}
