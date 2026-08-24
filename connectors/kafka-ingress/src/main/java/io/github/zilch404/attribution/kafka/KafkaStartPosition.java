package io.github.zilch404.attribution.kafka;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** Explicit startup semantics for a Kafka source. */
public final class KafkaStartPosition implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public enum Kind {
        COMMITTED,
        COMMITTED_OR_EARLIEST,
        EARLIEST,
        TIMESTAMP
    }

    private final Kind kind;
    private final Long timestampMillis;

    private KafkaStartPosition(Kind kind, Long timestampMillis) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (kind == Kind.TIMESTAMP) {
            if (timestampMillis == null || timestampMillis < 0) {
                throw new IllegalArgumentException("timestampMillis must be non-negative for TIMESTAMP");
            }
        } else if (timestampMillis != null) {
            throw new IllegalArgumentException("timestampMillis is only valid for TIMESTAMP");
        }
        this.timestampMillis = timestampMillis;
    }

    /** Starts only from committed offsets and fails if the group has none. */
    public static KafkaStartPosition committed() {
        return new KafkaStartPosition(Kind.COMMITTED, null);
    }

    public static KafkaStartPosition committedOrEarliest() {
        return new KafkaStartPosition(Kind.COMMITTED_OR_EARLIEST, null);
    }

    public static KafkaStartPosition earliest() {
        return new KafkaStartPosition(Kind.EARLIEST, null);
    }

    public static KafkaStartPosition timestamp(long timestampMillis) {
        return new KafkaStartPosition(Kind.TIMESTAMP, timestampMillis);
    }

    public static KafkaStartPosition timestamp(Instant timestamp) {
        Objects.requireNonNull(timestamp, "timestamp");
        return timestamp(timestamp.toEpochMilli());
    }

    public Kind kind() {
        return kind;
    }

    public Long timestampMillis() {
        return timestampMillis;
    }

    OffsetsInitializer toOffsetsInitializer() {
        return switch (kind) {
            case COMMITTED -> OffsetsInitializer.committedOffsets();
            case COMMITTED_OR_EARLIEST ->
                    OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case EARLIEST -> OffsetsInitializer.earliest();
            case TIMESTAMP -> OffsetsInitializer.timestamp(timestampMillis);
        };
    }

    @Override
    public boolean equals(Object candidate) {
        if (this == candidate) {
            return true;
        }
        if (!(candidate instanceof KafkaStartPosition other)) {
            return false;
        }
        return kind == other.kind && Objects.equals(timestampMillis, other.timestampMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, timestampMillis);
    }

    @Override
    public String toString() {
        return "KafkaStartPosition[kind=" + kind + ", timestampMillis=" + timestampMillis + ']';
    }
}
