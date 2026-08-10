package io.github.zilch404.attribution.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public record StandardEventMetadata(
        String eventId,
        EventType eventType,
        Instant eventTime,
        Instant ingestTime,
        int schemaVersion,
        SourcePosition sourcePosition) implements Serializable {

    public StandardEventMetadata {
        eventId = ModelValidation.requireNonBlank(eventId, "eventId");
        eventType = Objects.requireNonNull(eventType, "eventType");
        eventTime = Objects.requireNonNull(eventTime, "eventTime");
        ingestTime = Objects.requireNonNull(ingestTime, "ingestTime");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be positive");
        }
        sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
    }
}
