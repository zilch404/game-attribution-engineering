package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaInputRecord;
import io.github.zilch404.attribution.model.SourcePosition;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventNormalizerTest {
    private static final String CLICK_TOPIC = "game-attribution.raw.click.v1";
    private static final String ACTIVATION_TOPIC = "game-attribution.raw.activation.v1";
    private final EventNormalizer normalizer = new EventNormalizer(
            ConfiguredIdentityNormalizationPolicy.gameAttributionV1(),
            Duration.ofMinutes(5));

    @Test
    void normalizesAReplayableClickAndInjectsKafkaSourcePosition() {
        KafkaInputRecord input = input(
                CLICK_TOPIC,
                "clk_v1_click-1",
                fixture("event-contract/v1/valid-click.json"));

        StandardClickEvent first = accepted(normalizer.normalizeClick(input, CLICK_TOPIC));
        StandardClickEvent replayed = accepted(normalizer.normalizeClick(input, CLICK_TOPIC));

        assertThat(replayed).isEqualTo(first);
        assertThat(first.metadata().sourcePosition())
                .isEqualTo(new SourcePosition(CLICK_TOPIC, 2, 42, 1_786_200_001_500L,
                        "CREATE_TIME"));
        assertThat(first.metadata().eventTime().toEpochMilli()).isEqualTo(1_786_200_000_000L);
        assertThat(first.metadata().ingestTime().toEpochMilli()).isEqualTo(1_786_200_001_000L);
        assertThat(first.sourceSystem()).isEqualTo("gdt_click_collector");
        assertThat(first.clientPlatform()).isEqualTo("android");
        assertThat(first.campaign().platform()).isEqualTo("gdt");
        assertThat(first.identities()).extracting(identity -> identity.type())
                .containsExactly("oaid", "openid");
        assertThat(first.identities()).allSatisfy(identity -> {
            assertThat(identity.valueHash()).matches("[0-9a-f]{64}");
            assertThat(identity.normalizationVersion())
                    .isEqualTo(ConfiguredIdentityNormalizationPolicy.V1_NORMALIZATION_VERSION);
            assertThat(identity.keyVersion()).isEqualTo(
                    ConfiguredIdentityNormalizationPolicy.PLACEHOLDER_KEY_VERSION);
        });
        assertThat(first.toCoreEvent().clickId()).isEqualTo("clk_v1_click-1");
    }

    @Test
    void normalizesActivationInstallAndOptionalDirectClickId() {
        KafkaInputRecord input = input(
                ACTIVATION_TOPIC,
                "act_v1_activation-1",
                fixture("event-contract/v1/valid-activation.json"));

        StandardActivationEvent event = accepted(
                normalizer.normalizeActivation(input, ACTIVATION_TOPIC));

        assertThat(event.activationId()).isEqualTo("act_v1_activation-1");
        assertThat(event.installId()).isEqualTo("install-1");
        assertThat(event.directClickId()).isEqualTo("clk_v1_direct-1");
        assertThat(event.identities()).singleElement().satisfies(identity ->
                assertThat(identity.type()).isEqualTo("idfa"));
    }

    @Test
    void identityInputOrderDoesNotChangeTheStandardEvent() {
        KafkaInputRecord first = input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [
                  {"type":"oaid","value":"A0-B1"},
                  {"type":"openid","value":"User-Token"}
                ]
                """));
        KafkaInputRecord reversed = input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [
                  {"type":"openid","value":"User-Token"},
                  {"type":"oaid","value":"A0-B1"}
                ]
                """));

        assertThat(accepted(normalizer.normalizeClick(first, CLICK_TOPIC)))
                .isEqualTo(accepted(normalizer.normalizeClick(reversed, CLICK_TOPIC)));
    }

    @Test
    void keepsValidIdentitiesAndReportsInvalidOnesWithoutRawValues() {
        KafkaInputRecord input = input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [
                  {"type":"oaid","value":"0000-0000"},
                  {"type":"openid","value":"raw-player-secret"}
                ]
                """));

        NormalizationOutcome.Accepted<StandardClickEvent> accepted =
                acceptedOutcome(normalizer.normalizeClick(input, CLICK_TOPIC));

        assertThat(accepted.event().identities()).singleElement().satisfies(identity ->
                assertThat(identity.type()).isEqualTo("openid"));
        assertThat(accepted.identityIssues()).singleElement().satisfies(issue -> {
            assertThat(issue.identityIndex()).isZero();
            assertThat(issue.code()).isEqualTo(IdentityIssueCode.INVALID_IDENTITY_VALUE);
            assertThat(issue.toString()).doesNotContain("0000", "raw-player-secret");
        });
    }

    @Test
    void rejectsAnEventWhenEveryIdentityIsInvalid() {
        KafkaInputRecord input = input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [{"type":"unknown_player_id","value":"raw-player-secret"}]
                """));

        RejectedRecord rejection = rejected(normalizer.normalizeClick(input, CLICK_TOPIC));

        assertThat(rejection.code()).isEqualTo(RejectionCode.ALL_IDENTITIES_INVALID);
        assertThat(rejection.toString()).doesNotContain("raw-player-secret", "unknown_player_id");
    }

    @Test
    void classifiesDeterministicWireContractFailures() {
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", "{]"),
                RejectionCode.MALFORMED_JSON);
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """).replaceFirst("\"event_id\":", "\"event_id\":\"duplicate\",\"event_id\":")),
                RejectionCode.DUPLICATE_JSON_FIELD);
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """) + " {}"),
                RejectionCode.TRAILING_TOKENS);
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """).replace("\"payload\":", "\"unexpected\":1,\"payload\":")),
                RejectionCode.UNKNOWN_FIELD);
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """).replace("\"schema_version\":1", "\"schema_version\":2")),
                RejectionCode.UNSUPPORTED_SCHEMA_VERSION);
        assertRejected(
                input(CLICK_TOPIC, "different-key", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """)),
                RejectionCode.KEY_PAYLOAD_MISMATCH);
        assertRejected(
                input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                        [{"type":"oaid","value":"device-1"}]
                        """).replace("\"eligible\":true", "\"eligible\":\"true\"")),
                RejectionCode.TYPE_MISMATCH);
    }

    @Test
    void rejectsNullAndInvalidUtf8WithoutPoisoningTheSourceDeserializer() {
        KafkaInputRecord nullKey = input(CLICK_TOPIC, null, clickJson("""
                [{"type":"oaid","value":"device-1"}]
                """));
        KafkaInputRecord invalidUtf8 = rawInput(
                CLICK_TOPIC,
                "clk_v1_click-1".getBytes(StandardCharsets.UTF_8),
                new byte[]{(byte) 0xc3, (byte) 0x28});
        KafkaInputRecord tombstone = rawInput(
                CLICK_TOPIC,
                "clk_v1_click-1".getBytes(StandardCharsets.UTF_8),
                null);

        assertRejected(nullKey, RejectionCode.NULL_KEY);
        assertRejected(invalidUtf8, RejectionCode.INVALID_UTF8);
        assertRejected(tombstone, RejectionCode.NULL_VALUE);
    }

    @Test
    void initializedNormalizerAndProcessFunctionsAreFlinkSerializable() {
        normalizer.normalizeClick(input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [{"type":"oaid","value":"device-1"}]
                """)), CLICK_TOPIC);

        assertThatCode(() -> InstantiationUtil.serializeObject(normalizer))
                .doesNotThrowAnyException();
        assertThatCode(() -> InstantiationUtil.serializeObject(
                new ClickNormalizationProcessFunction(
                        CLICK_TOPIC,
                        ConfiguredIdentityNormalizationPolicy.gameAttributionV1(),
                        Duration.ofMinutes(5))))
                .doesNotThrowAnyException();
    }

    @Test
    void downstreamCollectorFailureIsNotTurnedIntoARejectedRecord() {
        KafkaInputRecord input = input(CLICK_TOPIC, "clk_v1_click-1", clickJson("""
                [{"type":"oaid","value":"device-1"}]
                """));
        Collector<StandardClickEvent> failingCollector = new Collector<>() {
            @Override
            public void collect(StandardClickEvent record) {
                throw new IllegalStateException("downstream failed");
            }

            @Override
            public void close() {
            }
        };

        assertThatThrownBy(() -> new ClickNormalizationProcessFunction(
                        CLICK_TOPIC,
                        ConfiguredIdentityNormalizationPolicy.gameAttributionV1(),
                        Duration.ofMinutes(5))
                .processElement(input, null, failingCollector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("downstream failed");
    }

    private void assertRejected(KafkaInputRecord input, RejectionCode code) {
        assertThat(rejected(normalizer.normalizeClick(input, CLICK_TOPIC)).code()).isEqualTo(code);
    }

    @SuppressWarnings("unchecked")
    private static <T> NormalizationOutcome.Accepted<T> acceptedOutcome(
            NormalizationOutcome<T> outcome) {
        assertThat(outcome).isInstanceOf(NormalizationOutcome.Accepted.class);
        return (NormalizationOutcome.Accepted<T>) outcome;
    }

    private static <T> T accepted(NormalizationOutcome<T> outcome) {
        return acceptedOutcome(outcome).event();
    }

    private static RejectedRecord rejected(NormalizationOutcome<?> outcome) {
        assertThat(outcome).isInstanceOf(NormalizationOutcome.Rejected.class);
        return ((NormalizationOutcome.Rejected<?>) outcome).rejection();
    }

    private static KafkaInputRecord input(String topic, String key, String json) {
        return rawInput(
                topic,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                json.getBytes(StandardCharsets.UTF_8));
    }

    private static String fixture(String path) {
        try (InputStream input = EventNormalizerTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (input == null) {
                throw new IllegalStateException("missing fixture: " + path);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("cannot read fixture: " + path, exception);
        }
    }

    private static KafkaInputRecord rawInput(String topic, byte[] key, byte[] payload) {
        return new KafkaInputRecord(
                new SourcePosition(topic, 2, 42, 1_786_200_001_500L, "CREATE_TIME"),
                key,
                payload,
                List.of(),
                key == null ? -1 : key.length,
                payload == null ? -1 : payload.length,
                3);
    }

    private static String clickJson(String identities) {
        return """
                {
                  "schema_version":1,
                  "event_id":"evt_v1_click-1",
                  "event_type":"click",
                  "event_time":1786200000000,
                  "ingest_time":1786200001000,
                  "tenant_id":"tenant",
                  "game_id":"game",
                  "app_id":"app",
                  "source_system":"gdt_click_collector",
                  "client_platform":"android",
                  "payload":{
                    "click_id":"clk_v1_click-1",
                    "ad_platform":"gdt",
                    "identities":%s,
                    "campaign":{
                      "account_id":"account-1",
                      "campaign_id":"campaign-1",
                      "ad_group_id":"group-1",
                      "creative_id":"creative-1"
                    },
                    "eligible":true
                  }
                }
                """.formatted(identities);
    }

}
