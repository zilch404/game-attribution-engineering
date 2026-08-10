package io.github.zilch404.attribution.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public final class StableEventIds {
    private StableEventIds() {
    }

    public static String click(
            AttributionScope scope,
            String adPlatform,
            String upstreamClickId) {
        return "clk_v1_" + digest(
                "game-attribution/click/v1",
                scope.tenantId(),
                scope.gameId(),
                scope.appId(),
                requireNonBlank(adPlatform, "adPlatform"),
                requireNonBlank(upstreamClickId, "upstreamClickId"));
    }

    public static String activation(
            AttributionScope scope,
            String clientPlatform,
            String upstreamActivationId) {
        return "act_v1_" + digest(
                "game-attribution/activation/v1",
                scope.tenantId(),
                scope.gameId(),
                scope.appId(),
                requireNonBlank(clientPlatform, "clientPlatform"),
                requireNonBlank(upstreamActivationId, "upstreamActivationId"));
    }

    public static String event(
            AttributionScope scope,
            EventType eventType,
            String sourceEventId) {
        if (eventType == null) {
            throw new IllegalArgumentException("eventType must not be null");
        }
        return "evt_v1_" + digest(
                "game-attribution/event/v1",
                scope.tenantId(),
                scope.gameId(),
                scope.appId(),
                eventType.wireName(),
                requireNonBlank(sourceEventId, "sourceEventId"));
    }

    private static String digest(String... fields) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] bytes = requireNonBlank(field, "id component")
                        .getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
