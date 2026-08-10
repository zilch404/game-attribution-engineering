package io.github.zilch404.attribution.kafka;

import io.github.zilch404.attribution.model.SourcePosition;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A lossless, replayable Kafka input record.
 *
 * <p>All byte arrays are copied both on input and output. This keeps buffers owned by the Kafka
 * client (and buffers returned to callers) from mutating the record after construction.
 */
public final class KafkaInputRecord implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final SourcePosition sourcePosition;
    private final byte[] key;
    private final byte[] payload;
    private final List<Header> headers;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Integer leaderEpoch;

    public KafkaInputRecord(
            SourcePosition sourcePosition,
            byte[] key,
            byte[] payload,
            List<Header> headers,
            int serializedKeySize,
            int serializedValueSize,
            Integer leaderEpoch) {
        this.sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
        this.key = copy(key);
        this.payload = copy(payload);
        this.headers = copyHeaders(headers);
        this.serializedKeySize = requireValidSerializedSize(serializedKeySize, "serializedKeySize");
        this.serializedValueSize = requireValidSerializedSize(serializedValueSize, "serializedValueSize");
        if (leaderEpoch != null && leaderEpoch < 0) {
            throw new IllegalArgumentException("leaderEpoch must not be negative");
        }
        this.leaderEpoch = leaderEpoch;
    }

    public SourcePosition sourcePosition() {
        return sourcePosition;
    }

    public byte[] key() {
        return copy(key);
    }

    public byte[] payload() {
        return copy(payload);
    }

    public List<Header> headers() {
        return headers;
    }

    public int serializedKeySize() {
        return serializedKeySize;
    }

    public int serializedValueSize() {
        return serializedValueSize;
    }

    public Integer leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public boolean equals(Object candidate) {
        if (this == candidate) {
            return true;
        }
        if (!(candidate instanceof KafkaInputRecord other)) {
            return false;
        }
        return serializedKeySize == other.serializedKeySize
                && serializedValueSize == other.serializedValueSize
                && sourcePosition.equals(other.sourcePosition)
                && Arrays.equals(key, other.key)
                && Arrays.equals(payload, other.payload)
                && headers.equals(other.headers)
                && Objects.equals(leaderEpoch, other.leaderEpoch);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                sourcePosition, headers, serializedKeySize, serializedValueSize, leaderEpoch);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "KafkaInputRecord["
                + "sourcePosition=" + sourcePosition
                + ", keyLength=" + length(key)
                + ", payloadLength=" + length(payload)
                + ", headerCount=" + headers.size()
                + ", serializedKeySize=" + serializedKeySize
                + ", serializedValueSize=" + serializedValueSize
                + ", leaderEpoch=" + leaderEpoch
                + ']';
    }

    private static List<Header> copyHeaders(List<Header> headers) {
        Objects.requireNonNull(headers, "headers");
        if (headers.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("headers must not contain null");
        }
        return List.copyOf(headers);
    }

    private static int requireValidSerializedSize(int size, String field) {
        if (size < -1) {
            throw new IllegalArgumentException(field + " must be -1 or non-negative");
        }
        return size;
    }

    private static byte[] copy(byte[] bytes) {
        return bytes == null ? null : bytes.clone();
    }

    private static int length(byte[] bytes) {
        return bytes == null ? -1 : bytes.length;
    }

    /** A Kafka header that preserves duplicate keys and arrival order. */
    public static final class Header implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        private final String key;
        private final byte[] value;

        public Header(String key, byte[] value) {
            this.key = Objects.requireNonNull(key, "key");
            this.value = copy(value);
        }

        public String key() {
            return key;
        }

        public byte[] value() {
            return copy(value);
        }

        @Override
        public boolean equals(Object candidate) {
            if (this == candidate) {
                return true;
            }
            if (!(candidate instanceof Header other)) {
                return false;
            }
            return key.equals(other.key) && Arrays.equals(value, other.value);
        }

        @Override
        public int hashCode() {
            return 31 * key.hashCode() + Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return "Header[keyLength=" + key.length()
                    + ", valueLength=" + length(value) + ']';
        }
    }
}
