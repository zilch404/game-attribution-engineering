package io.github.zilch404.attribution.model;

import java.io.Serializable;

public record StandardIdentity(
        String type,
        String valueHash,
        String normalizationVersion,
        String keyVersion) implements Comparable<StandardIdentity>, Serializable {

    public StandardIdentity {
        type = ModelValidation.requireNonBlank(type, "type");
        valueHash = ModelValidation.requireNonBlank(valueHash, "valueHash");
        if (!valueHash.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "valueHash must be 64 lowercase hexadecimal characters");
        }
        normalizationVersion = ModelValidation.requireNonBlank(
                normalizationVersion, "normalizationVersion");
        keyVersion = ModelValidation.requireNonBlank(keyVersion, "keyVersion");
    }

    public IdentityKey toIdentityKey() {
        return new IdentityKey(type, valueHash);
    }

    @Override
    public int compareTo(StandardIdentity other) {
        int byType = type.compareTo(other.type);
        if (byType != 0) {
            return byType;
        }
        int byHash = valueHash.compareTo(other.valueHash);
        if (byHash != 0) {
            return byHash;
        }
        int byNormalization = normalizationVersion.compareTo(other.normalizationVersion);
        if (byNormalization != 0) {
            return byNormalization;
        }
        return keyVersion.compareTo(other.keyVersion);
    }
}
