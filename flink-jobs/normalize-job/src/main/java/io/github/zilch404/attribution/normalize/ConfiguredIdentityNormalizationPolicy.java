package io.github.zilch404.attribution.normalize;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ConfiguredIdentityNormalizationPolicy
        implements IdentityNormalizationPolicy {

    public static final String V1_NORMALIZATION_VERSION = "game-attribution-identity-v1";
    public static final String PLACEHOLDER_KEY_VERSION = "unkeyed-placeholder-v1";

    private final Map<String, IdentityNormalizationRule> rules;

    public ConfiguredIdentityNormalizationPolicy(
            Map<String, IdentityNormalizationRule> rules) {
        Objects.requireNonNull(rules, "rules");
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("rules must not be empty");
        }
        this.rules = rules.entrySet().stream().collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                        entry -> normalizeType(entry.getKey()),
                        entry -> Objects.requireNonNull(entry.getValue(), "rule")));
    }

    public static ConfiguredIdentityNormalizationPolicy gameAttributionV1() {
        return new ConfiguredIdentityNormalizationPolicy(Map.of(
                "openid", v1(Canonicalization.CASE_SENSITIVE_TRIMMED),
                "unionid", v1(Canonicalization.CASE_SENSITIVE_TRIMMED),
                "click_token", v1(Canonicalization.CASE_SENSITIVE_TRIMMED),
                "oaid", v1(Canonicalization.LOWERCASE_ALPHANUMERIC),
                "idfa", v1(Canonicalization.LOWERCASE_ALPHANUMERIC),
                "android_id", v1(Canonicalization.LOWERCASE_ALPHANUMERIC)));
    }

    @Override
    public Optional<IdentityNormalizationRule> ruleFor(String identityType) {
        return normalizedType(identityType).map(rules::get);
    }

    private static IdentityNormalizationRule v1(Canonicalization canonicalization) {
        return new IdentityNormalizationRule(
                canonicalization,
                V1_NORMALIZATION_VERSION,
                PLACEHOLDER_KEY_VERSION);
    }

    private static String normalizeType(String value) {
        return normalizedType(value).orElseThrow(() ->
                new IllegalArgumentException("identity type must match [a-z0-9_]{1,64}"));
    }

    private static Optional<String> normalizedType(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (!normalized.matches("[a-z0-9_]{1,64}")) {
            return Optional.empty();
        }
        return Optional.of(normalized);
    }
}
