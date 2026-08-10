package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.IdentityKey;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdentityNormalizerTest {
    private static final AttributionScope SCOPE =
            new AttributionScope("tenant", "game", "app");
    private final IdentityNormalizer normalizer = new IdentityNormalizer();

    @Test
    void normalizesEquivalentDeviceIdentifiersToTheSameHash() {
        IdentityKey upper = normalizer.normalize(
                SCOPE,
                "IDFA",
                "A0B1-C2D3",
                Canonicalization.LOWERCASE_ALPHANUMERIC);
        IdentityKey lower = normalizer.normalize(
                SCOPE,
                "idfa",
                "a0b1c2d3",
                Canonicalization.LOWERCASE_ALPHANUMERIC);

        assertThat(upper).isEqualTo(lower);
        assertThat(upper.valueHash()).hasSize(64);
    }

    @Test
    void preservesCaseForOpaquePlatformTokens() {
        IdentityKey upper = normalizer.normalize(
                SCOPE,
                "click_id",
                "Token-A",
                Canonicalization.CASE_SENSITIVE_TRIMMED);
        IdentityKey lower = normalizer.normalize(
                SCOPE,
                "click_id",
                "token-a",
                Canonicalization.CASE_SENSITIVE_TRIMMED);

        assertThat(upper).isNotEqualTo(lower);
    }

    @Test
    void rejectsAllZeroHotKeys() {
        assertThatThrownBy(() -> normalizer.normalize(
                SCOPE,
                "oaid",
                "0000-0000",
                Canonicalization.LOWERCASE_ALPHANUMERIC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid placeholder");
    }

    @Test
    void scopesHashesToTheTenantGameAndApp() {
        IdentityKey first = normalizer.normalize(
                SCOPE,
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);
        IdentityKey otherApp = normalizer.normalize(
                new AttributionScope("tenant", "game", "other-app"),
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);

        assertThat(first).isNotEqualTo(otherApp);
    }

    @Test
    void lengthPrefixesPreventScopeSeparatorCollisions() {
        IdentityKey first = normalizer.normalize(
                new AttributionScope("a\0b", "c", "app"),
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);
        IdentityKey second = normalizer.normalize(
                new AttributionScope("a", "b\0c", "app"),
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void v1RulesMatchPublishedHashVectorsAndCarryTheirVersions() {
        Map<String, String> expectedHashes = Map.of(
                "openid", "54dd8bd1ad5d9f114cb3ebf55f327db7111ca455401ed8fc6270b7a71f57932e",
                "unionid", "d87aa02842c2729d039f10c9cad796afb5ae6942a7db508309189d523053ee46",
                "click_token", "c5fd1d2c53169435d1698ced2ed3b2a27c06f1250a28d8927e77da6e8c9c60fc",
                "oaid", "c29358f35ad5f4bf188063d5268d4f8051ceb855d55a5e9821a1f21d9ef98000",
                "idfa", "d73a4327d1226a8ce483dbc18dc8138a7772242a88e535b60dcb8b90c726623f",
                "android_id", "2ad0afc5cb7872b72ed96bda5803462ec038e2a966e5107f789325e152bce2ba");
        ConfiguredIdentityNormalizationPolicy policy =
                ConfiguredIdentityNormalizationPolicy.gameAttributionV1();

        expectedHashes.forEach((type, expectedHash) -> {
            IdentityNormalizationRule rule = policy.ruleFor(type).orElseThrow();
            IdentityKey normalized = normalizer.normalize(
                    SCOPE, type, " Raw-Value_01 ", rule.canonicalization());

            assertThat(normalized.valueHash()).as(type).isEqualTo(expectedHash);
            assertThat(rule.normalizationVersion()).as(type).isEqualTo(
                    ConfiguredIdentityNormalizationPolicy.V1_NORMALIZATION_VERSION);
            assertThat(rule.keyVersion()).as(type).isEqualTo(
                    ConfiguredIdentityNormalizationPolicy.PLACEHOLDER_KEY_VERSION);
        });
    }

}
