package io.github.zilch404.attribution.normalize;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.github.zilch404.attribution.kafka.KafkaInputRecord;
import io.github.zilch404.attribution.kafka.KafkaOutputRecord;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.EventType;
import io.github.zilch404.attribution.model.SourcePosition;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import io.github.zilch404.attribution.model.StandardEventMetadata;
import io.github.zilch404.attribution.model.StandardIdentity;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NormalizationKafkaEncoderTest {
    private static final SchemaRegistry SCHEMA_REGISTRY =
            SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    private static final String RAW_CLICK_TOPIC = "game-attribution.raw.click.v1";
    private static final String RAW_ACTIVATION_TOPIC = "game-attribution.raw.activation.v1";
    private static final String STANDARD_CLICK_TOPIC = "game-attribution.standard.click.v1";
    private static final String STANDARD_ACTIVATION_TOPIC =
            "game-attribution.standard.activation.v1";
    private static final String REJECTED_TOPIC = "game-attribution.normalization.invalid.v1";
    private static final String IDENTITY_ISSUE_TOPIC =
            "game-attribution.normalization.identity-issue.v1";
    private static final String HASH_A = "a".repeat(64);
    private static final String HASH_B = "b".repeat(64);
    private static final String HASH_C = "c".repeat(64);

    private final NormalizationKafkaEncoder encoder = new NormalizationKafkaEncoder(
            new NormalizationOutputTopics(
                    STANDARD_CLICK_TOPIC,
                    STANDARD_ACTIVATION_TOPIC,
                    REJECTED_TOPIC,
                    IDENTITY_ISSUE_TOPIC));

    @Test
    void encodesClickAsExactStableV1BytesAndUsesClickIdAsKey() {
        StandardClickEvent event = new StandardClickEvent(
                metadata(EventType.CLICK, "evt_v1_click-1", RAW_CLICK_TOPIC),
                scope(),
                "gdt_click_collector",
                "android",
                "clk_v1_click-1",
                List.of(identity("openid", HASH_B), identity("oaid", HASH_A)),
                new CampaignRef("gdt", "account-1", "campaign-1", "group-1", "creative-1"),
                true);

        KafkaOutputRecord first = encoder.encodeClick(event);
        KafkaOutputRecord replayed = encoder.encodeClick(event);

        assertThat(first.topic()).isEqualTo(STANDARD_CLICK_TOPIC);
        assertThat(first.key()).containsExactly(utf8("clk_v1_click-1"));
        assertThat(first.payload()).containsExactly(replayed.payload());
        assertValidSchema("standard-click-v1.schema.json", first.payload());
        assertThat(utf8(first.payload())).isEqualTo("""
                {"schema_version":1,"event_id":"evt_v1_click-1","event_type":"click","event_time":1786200000000,"ingest_time":1786200001000,"tenant_id":"tenant","game_id":"game","app_id":"app","source_system":"gdt_click_collector","client_platform":"android","source_position":{"topic":"game-attribution.raw.click.v1","partition":2,"offset":42,"record_timestamp":1786200001500,"timestamp_type":"CREATE_TIME"},"payload":{"click_id":"clk_v1_click-1","identities":[{"type":"oaid","value_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","normalization_version":"identity-v1","key_version":"key-v1"},{"type":"openid","value_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","normalization_version":"identity-v1","key_version":"key-v1"}],"campaign":{"platform":"gdt","account_id":"account-1","campaign_id":"campaign-1","ad_group_id":"group-1","creative_id":"creative-1"},"eligible":true}}
                """.strip());
    }

    @Test
    void encodesActivationAsExactV1BytesAndUsesActivationIdAsKey() {
        StandardActivationEvent event = new StandardActivationEvent(
                metadata(EventType.ACTIVATION, "evt_v1_activation-1", RAW_ACTIVATION_TOPIC),
                scope(),
                "ios_activation_collector",
                "ios",
                "act_v1_activation-1",
                "install-1",
                "clk_v1_direct-1",
                List.of(identity("idfa", HASH_C)));

        KafkaOutputRecord record = encoder.encodeActivation(event);

        assertThat(record.topic()).isEqualTo(STANDARD_ACTIVATION_TOPIC);
        assertThat(record.key()).containsExactly(utf8("act_v1_activation-1"));
        assertValidSchema("standard-activation-v1.schema.json", record.payload());
        assertThat(utf8(record.payload())).isEqualTo("""
                {"schema_version":1,"event_id":"evt_v1_activation-1","event_type":"activation","event_time":1786200000000,"ingest_time":1786200001000,"tenant_id":"tenant","game_id":"game","app_id":"app","source_system":"ios_activation_collector","client_platform":"ios","source_position":{"topic":"game-attribution.raw.activation.v1","partition":2,"offset":42,"record_timestamp":1786200001500,"timestamp_type":"CREATE_TIME"},"payload":{"activation_id":"act_v1_activation-1","install_id":"install-1","direct_click_id":"clk_v1_direct-1","identities":[{"type":"idfa","value_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","normalization_version":"identity-v1","key_version":"key-v1"}]}}
                """.strip());
    }

    @Test
    void keepsAnEmptyDirectClickIdAsAnExplicitV1Field() {
        StandardActivationEvent event = new StandardActivationEvent(
                metadata(EventType.ACTIVATION, "evt_v1_activation-organic", RAW_ACTIVATION_TOPIC),
                scope(),
                "wechat_activation_collector",
                "wechat_mini_program",
                "act_v1_activation-organic",
                "install-organic",
                "",
                List.of(identity("openid", HASH_B)));

        String payload = utf8(encoder.encodeActivation(event).payload());

        assertValidSchema(
                "standard-activation-v1.schema.json",
                encoder.encodeActivation(event).payload());
        assertThat(payload)
                .contains("\"install_id\":\"install-organic\",\"direct_click_id\":\"\"")
                .contains("\"identities\"");
    }

    @Test
    void encodesRejectedRecordWithoutRawPayloadAndUsesSourcePositionKey() {
        RejectedRecord rejected = new RejectedRecord(
                sourcePosition(RAW_CLICK_TOPIC),
                321,
                RejectionCode.MALFORMED_JSON,
                "value");

        KafkaOutputRecord record = encoder.encodeRejectedRecord(rejected);

        assertThat(record.topic()).isEqualTo(REJECTED_TOPIC);
        assertThat(record.key()).containsExactly(utf8(RAW_CLICK_TOPIC + ":2:42"));
        assertValidSchema("normalization-rejected-v1.schema.json", record.payload());
        assertThat(utf8(record.payload())).isEqualTo("""
                {"schema_version":1,"record_type":"rejected_record","source_position":{"topic":"game-attribution.raw.click.v1","partition":2,"offset":42,"record_timestamp":1786200001500,"timestamp_type":"CREATE_TIME"},"payload_size":321,"code":"malformed_json","safe_field":"value"}
                """.strip());
        assertThat(utf8(record.payload()))
                .doesNotContain("raw-player-secret");
    }

    @Test
    void encodesIdentityIssueAsExactV1BytesAndUsesPositionAndIndexKey() {
        IdentityNormalizationIssue issue = new IdentityNormalizationIssue(
                sourcePosition(RAW_ACTIVATION_TOPIC),
                "evt_v1_activation-1",
                3,
                IdentityIssueCode.INVALID_IDENTITY_VALUE);

        KafkaOutputRecord record = encoder.encodeIdentityIssue(issue);

        assertThat(record.topic()).isEqualTo(IDENTITY_ISSUE_TOPIC);
        assertThat(record.key()).containsExactly(utf8(RAW_ACTIVATION_TOPIC + ":2:42:3"));
        assertValidSchema(
                "identity-normalization-issue-v1.schema.json",
                record.payload());
        assertThat(utf8(record.payload())).isEqualTo("""
                {"schema_version":1,"record_type":"identity_normalization_issue","source_position":{"topic":"game-attribution.raw.activation.v1","partition":2,"offset":42,"record_timestamp":1786200001500,"timestamp_type":"CREATE_TIME"},"event_id":"evt_v1_activation-1","identity_index":3,"code":"invalid_identity_value"}
                """.strip());
    }

    @Test
    void normalizedOutputNeverCarriesTheRawIdentityValue() {
        String rawIdentity = "raw-player-secret";
        String rawJson = """
                {
                  "schema_version":1,
                  "event_id":"evt_v1_click-privacy",
                  "event_type":"click",
                  "event_time":1786200000000,
                  "ingest_time":1786200001000,
                  "tenant_id":"tenant",
                  "game_id":"game",
                  "app_id":"app",
                  "source_system":"gdt_click_collector",
                  "client_platform":"android",
                  "payload":{
                    "click_id":"clk_v1_click-privacy",
                    "ad_platform":"gdt",
                    "identities":[{"type":"openid","value":"%s"}],
                    "campaign":{"account_id":"account-1"},
                    "eligible":true
                  }
                }
                """.formatted(rawIdentity);
        KafkaInputRecord input = new KafkaInputRecord(
                sourcePosition(RAW_CLICK_TOPIC),
                utf8("clk_v1_click-privacy"),
                utf8(rawJson),
                List.of(),
                utf8("clk_v1_click-privacy").length,
                utf8(rawJson).length,
                3);
        EventNormalizer normalizer = new EventNormalizer(
                ConfiguredIdentityNormalizationPolicy.gameAttributionV1(),
                Duration.ofMinutes(5));

        NormalizationOutcome<StandardClickEvent> outcome =
                normalizer.normalizeClick(input, RAW_CLICK_TOPIC);
        StandardClickEvent event = accepted(outcome);
        String encoded = utf8(encoder.encodeClick(event).payload());

        assertThat(encoded)
                .contains("\"value_hash\"")
                .doesNotContain(rawIdentity, "\"value\"");
    }

    @Test
    void rejectsUnsupportedSchemaVersionsAndInvalidTopicSets() {
        StandardClickEvent versionTwo = new StandardClickEvent(
                new StandardEventMetadata(
                        "evt_v2_click-1",
                        EventType.CLICK,
                        Instant.ofEpochMilli(1_786_200_000_000L),
                        Instant.ofEpochMilli(1_786_200_001_000L),
                        2,
                        sourcePosition(RAW_CLICK_TOPIC)),
                scope(),
                "gdt_click_collector",
                "android",
                "clk_v2_click-1",
                List.of(identity("oaid", HASH_A)),
                new CampaignRef("gdt", "account-1", "", "", ""),
                true);

        assertThatThrownBy(() -> encoder.encodeClick(versionTwo))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema version 1");
        assertThatThrownBy(() -> new NormalizationOutputTopics(
                STANDARD_CLICK_TOPIC,
                STANDARD_CLICK_TOPIC,
                REJECTED_TOPIC,
                IDENTITY_ISSUE_TOPIC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinct");
        assertThatThrownBy(() -> new NormalizationOutputTopics(
                "invalid topic",
                STANDARD_ACTIVATION_TOPIC,
                REJECTED_TOPIC,
                IDENTITY_ISSUE_TOPIC))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("valid Kafka topic");
    }

    private static StandardEventMetadata metadata(
            EventType type,
            String eventId,
            String sourceTopic) {
        return new StandardEventMetadata(
                eventId,
                type,
                Instant.ofEpochMilli(1_786_200_000_000L),
                Instant.ofEpochMilli(1_786_200_001_000L),
                1,
                sourcePosition(sourceTopic));
    }

    private static AttributionScope scope() {
        return new AttributionScope("tenant", "game", "app");
    }

    private static StandardIdentity identity(String type, String hash) {
        return new StandardIdentity(type, hash, "identity-v1", "key-v1");
    }

    private static SourcePosition sourcePosition(String topic) {
        return new SourcePosition(topic, 2, 42, 1_786_200_001_500L, "CREATE_TIME");
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String utf8(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static void assertValidSchema(String schemaResource, byte[] payload) {
        Schema schema = SCHEMA_REGISTRY.getSchema(
                SchemaLocation.of("classpath:event-contract/" + schemaResource));
        assertThat(schema.validate(utf8(payload), InputFormat.JSON))
                .as("wire payload must match %s", schemaResource)
                .isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static <T> T accepted(NormalizationOutcome<T> outcome) {
        assertThat(outcome).isInstanceOf(NormalizationOutcome.Accepted.class);
        return ((NormalizationOutcome.Accepted<T>) outcome).event();
    }
}
