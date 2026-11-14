package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.SourcePosition;

import java.io.Serializable;
import java.util.Objects;

public record RejectedRecord(
        SourcePosition sourcePosition,
        int payloadSize,
        RejectionCode code,
        String safeField) implements Serializable {

    public RejectedRecord {
        sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (payloadSize < 0) {
            throw new IllegalArgumentException("payloadSize must not be negative");
        }
        code = Objects.requireNonNull(code, "code");
        safeField = Objects.requireNonNullElse(safeField, "");
        if (!safeField.matches("[a-z0-9_.]{0,64}")) {
            throw new IllegalArgumentException("safeField must be a predefined field path");
        }
    }

    @Override
    public String toString() {
        return "RejectedRecord["
                + "sourcePosition=" + sourcePosition
                + ", payloadSize=" + payloadSize
                + ", code=" + code
                + ", safeField=" + safeField
                + ']';
    }
}
