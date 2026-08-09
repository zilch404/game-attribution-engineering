package io.github.zilch404.attribution.model;

import java.io.Serializable;

public record SourcePosition(
        String topic,
        int partition,
        long offset,
        long recordTimestamp,
        String timestampType) implements Serializable {

    public SourcePosition {
        topic = ModelValidation.requireNonBlank(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        if (recordTimestamp < -1) {
            throw new IllegalArgumentException("recordTimestamp must be -1 or greater");
        }
        timestampType = ModelValidation.requireNonBlank(timestampType, "timestampType");
    }
}
