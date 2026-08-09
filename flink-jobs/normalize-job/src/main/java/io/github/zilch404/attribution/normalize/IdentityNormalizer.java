package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.IdentityKey;
import io.github.zilch404.attribution.model.AttributionScope;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

public final class IdentityNormalizer implements Serializable {
    public IdentityKey normalize(
            AttributionScope scope,
            String type,
            String rawValue,
            Canonicalization canonicalization) {
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        String normalizedType = normalizeType(type);
        String canonicalValue = canonicalize(rawValue, canonicalization);
        rejectKnownInvalidValues(canonicalValue);
        return new IdentityKey(
                normalizedType,
                sha256(
                        scope.tenantId(),
                        scope.gameId(),
                        scope.appId(),
                        normalizedType,
                        canonicalValue));
    }

    private static String normalizeType(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("identity type must not be blank");
        }
        String normalized = type.trim().toLowerCase(Locale.ROOT);
        if (!normalized.matches("[a-z0-9_]+")) {
            throw new IllegalArgumentException("identity type must match [a-z0-9_]+");
        }
        return normalized;
    }

    private static String canonicalize(String rawValue, Canonicalization canonicalization) {
        if (rawValue == null) {
            throw new IllegalArgumentException("identity value must not be null");
        }
        if (canonicalization == null) {
            throw new IllegalArgumentException("canonicalization must not be null");
        }
        String trimmed = rawValue.trim();
        String canonical = switch (canonicalization) {
            case CASE_SENSITIVE_TRIMMED -> trimmed;
            case LOWERCASE_TRIMMED -> trimmed.toLowerCase(Locale.ROOT);
            case LOWERCASE_ALPHANUMERIC -> trimmed
                    .toLowerCase(Locale.ROOT)
                    .replaceAll("[^a-z0-9]", "");
        };
        if (canonical.isBlank()) {
            throw new IllegalArgumentException("identity value is empty after canonicalization");
        }
        return canonical;
    }

    private static void rejectKnownInvalidValues(String canonicalValue) {
        String compact = canonicalValue.replaceAll("[^a-zA-Z0-9]", "");
        if (compact.isEmpty() || compact.chars().allMatch(character -> character == '0')) {
            throw new IllegalArgumentException("identity value is a known invalid placeholder");
        }
    }

    private static String sha256(String... fields) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
