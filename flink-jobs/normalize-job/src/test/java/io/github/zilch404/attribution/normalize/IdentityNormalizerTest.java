package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.IdentityKey;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    void processOutputKeepsEventCorrelationAndPolicyVersion() {
        RawIdentity raw = new RawIdentity(
                "event-1",
                SCOPE,
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);

        NormalizedIdentity normalized =
                new IdentityNormalizationProcessFunction().normalize(raw);

        assertThat(normalized.eventId()).isEqualTo(raw.eventId());
        assertThat(normalized.scope()).isEqualTo(SCOPE);
        assertThat(normalized.normalizationVersion())
                .isEqualTo(IdentityNormalizer.NORMALIZATION_VERSION);
    }

    @Test
    void flinkProcessFunctionIsSerializable() {
        assertThatCode(() -> InstantiationUtil.serializeObject(
                new IdentityNormalizationProcessFunction()))
                .doesNotThrowAnyException();
    }

    @Test
    void downstreamCollectorFailureIsNeverMisclassifiedAsInvalidIdentity() {
        RawIdentity raw = new RawIdentity(
                "event-1",
                SCOPE,
                "oaid",
                "device-1",
                Canonicalization.CASE_SENSITIVE_TRIMMED);
        Collector<NormalizedIdentity> failingCollector = new Collector<>() {
            @Override
            public void collect(NormalizedIdentity record) {
                throw new IllegalStateException("downstream failed");
            }

            @Override
            public void close() {
            }
        };

        assertThatThrownBy(() -> new IdentityNormalizationProcessFunction()
                .processElement(raw, null, failingCollector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("downstream failed");
    }
}
