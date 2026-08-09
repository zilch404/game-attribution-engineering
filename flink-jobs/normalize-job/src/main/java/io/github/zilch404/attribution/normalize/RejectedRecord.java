package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.SourcePosition;

import java.io.Serializable;
import java.util.Objects;

public record RejectedRecord(
        SourcePosition sourcePosition,
        String payloadSha256,
        int payloadSize,
        RejectionCode code,
        String safeField) implements Serializable {

    public RejectedRecord {
        sourcePosition = Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (payloadSha256 == null || !payloadSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("payloadSha256 must be lowercase SHA-256 hex");
        }
        if (payloadSize < 0) {
            throw new IllegalArgumentException("payloadSize must not be negative");
        }
        code = Objects.requireNonNull(code, "code");
        safeField = Objects.requireNonNullElse(safeField, "");
        if (!safeField.matches("[a-z0-9_.]{0,64}")) {
            throw new IllegalArgumentException("safeField must be a predefined field path");
        }
    }
}
