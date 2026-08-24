package io.github.zilch404.attribution.kafka;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A Kafka output record with an explicit destination topic.
 *
 * <p>Key and payload byte arrays are copied both on input and output so mutable buffers cannot
 * change a record while it is crossing an asynchronous Flink operator boundary.
 */
public final class KafkaOutputRecord implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String topic;
    private final byte[] key;
    private final byte[] payload;

    public KafkaOutputRecord(String topic, byte[] key, byte[] payload) {
        this.topic = requireNonBlank(topic, "topic");
        this.key = copy(key);
        this.payload = copy(payload);
    }

    public String topic() {
        return topic;
    }

    public byte[] key() {
        return copy(key);
    }

    public byte[] payload() {
        return copy(payload);
    }

    @Override
    public boolean equals(Object candidate) {
        if (this == candidate) {
            return true;
        }
        if (!(candidate instanceof KafkaOutputRecord other)) {
            return false;
        }
        return topic.equals(other.topic)
                && Arrays.equals(key, other.key)
                && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "KafkaOutputRecord["
                + "topic=" + topic
                + ", keyLength=" + length(key)
                + ", payloadLength=" + length(payload)
                + ']';
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : bytes.clone();
    }

    private static int length(byte[] bytes) {
        return bytes == null ? -1 : bytes.length;
    }
}
