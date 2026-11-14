package io.github.zilch404.attribution.normalize;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.github.zilch404.attribution.kafka.KafkaOutputRecord;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.SourcePosition;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import io.github.zilch404.attribution.model.StandardEventMetadata;
import io.github.zilch404.attribution.model.StandardIdentity;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Explicit version-one wire encoder for the four durable outputs of the normalization job.
 *
 * <p>The field order, field names and enum tokens in this class are part of the wire contract.
 * Domain records are deliberately not handed to a general-purpose object mapper: adding or
 * renaming a Java component must not silently change bytes written to Kafka.
 */
public final class NormalizationKafkaEncoder implements Serializable {
    private static final int WIRE_SCHEMA_VERSION = 1;
    private static final int MAX_IDENTITIES = 16;
    private static final long MIN_EVENT_TIME =
            Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
    private static final long MAX_EVENT_TIME =
            Instant.parse("2100-01-01T00:00:00Z").toEpochMilli();
    private static final Pattern ID =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");
    private static final Pattern TOKEN =
            Pattern.compile("[a-z0-9][a-z0-9_-]{0,63}");
    private static final Pattern VERSION =
            Pattern.compile("[a-z0-9][a-z0-9._-]{0,63}");
    private static final Set<String> TIMESTAMP_TYPES = Set.of(
            "CREATE_TIME", "LOG_APPEND_TIME", "NO_TIMESTAMP_TYPE");
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final NormalizationOutputTopics topics;

    public NormalizationKafkaEncoder(NormalizationOutputTopics topics) {
        this.topics = Objects.requireNonNull(topics, "topics");
    }

    public KafkaOutputRecord encodeClick(StandardClickEvent event) {
        Objects.requireNonNull(event, "event");
        validateClick(event);
        byte[] payload = encodeJson(json -> {
            writeStandardEnvelope(json, event.metadata(), event.scope(),
                    event.sourceSystem(), event.clientPlatform());
            json.writeObjectFieldStart("payload");
            json.writeStringField("click_id", event.clickId());
            writeIdentities(json, event.identities());
            writeCampaign(json, event.campaign());
            json.writeBooleanField("eligible", event.eligible());
            json.writeEndObject();
        });
        return output(topics.standardClickTopic(), event.clickId(), payload);
    }

    public KafkaOutputRecord encodeActivation(StandardActivationEvent event) {
        Objects.requireNonNull(event, "event");
        validateActivation(event);
        byte[] payload = encodeJson(json -> {
            writeStandardEnvelope(json, event.metadata(), event.scope(),
                    event.sourceSystem(), event.clientPlatform());
            json.writeObjectFieldStart("payload");
            json.writeStringField("activation_id", event.activationId());
            json.writeStringField("install_id", event.installId());
            json.writeStringField("direct_click_id", event.directClickId());
            writeIdentities(json, event.identities());
            json.writeEndObject();
        });
        return output(topics.standardActivationTopic(), event.activationId(), payload);
    }

    public KafkaOutputRecord encodeRejectedRecord(RejectedRecord rejectedRecord) {
        Objects.requireNonNull(rejectedRecord, "rejectedRecord");
        validateSourcePosition(rejectedRecord.sourcePosition());
        byte[] payload = encodeJson(json -> {
            json.writeNumberField("schema_version", WIRE_SCHEMA_VERSION);
            json.writeStringField("record_type", "rejected_record");
            writeSourcePosition(json, rejectedRecord.sourcePosition());
            json.writeNumberField("payload_size", rejectedRecord.payloadSize());
            json.writeStringField("code", rejectionCodeToken(rejectedRecord.code()));
            json.writeStringField("safe_field", rejectedRecord.safeField());
        });
        return output(
                topics.rejectedRecordTopic(),
                sourcePositionKey(rejectedRecord.sourcePosition()),
                payload);
    }

    public KafkaOutputRecord encodeIdentityIssue(IdentityNormalizationIssue issue) {
        Objects.requireNonNull(issue, "issue");
        validateSourcePosition(issue.sourcePosition());
        requirePrefixedId(issue.eventId(), "evt_v1_", "eventId");
        if (issue.identityIndex() >= MAX_IDENTITIES) {
            throw new IllegalArgumentException("identityIndex must be less than 16");
        }
        byte[] payload = encodeJson(json -> {
            json.writeNumberField("schema_version", WIRE_SCHEMA_VERSION);
            json.writeStringField("record_type", "identity_normalization_issue");
            writeSourcePosition(json, issue.sourcePosition());
            json.writeStringField("event_id", issue.eventId());
            json.writeNumberField("identity_index", issue.identityIndex());
            json.writeStringField("code", identityIssueCodeToken(issue.code()));
        });
        String key = sourcePositionKey(issue.sourcePosition()) + ":" + issue.identityIndex();
        return output(topics.identityIssueTopic(), key, payload);
    }

    private static void writeStandardEnvelope(
            JsonGenerator json,
            StandardEventMetadata metadata,
            AttributionScope scope,
            String sourceSystem,
            String clientPlatform) throws IOException {
        json.writeNumberField("schema_version", WIRE_SCHEMA_VERSION);
        json.writeStringField("event_id", metadata.eventId());
        json.writeStringField("event_type", metadata.eventType().wireName());
        json.writeNumberField("event_time", metadata.eventTime().toEpochMilli());
        json.writeNumberField("ingest_time", metadata.ingestTime().toEpochMilli());
        json.writeStringField("tenant_id", scope.tenantId());
        json.writeStringField("game_id", scope.gameId());
        json.writeStringField("app_id", scope.appId());
        json.writeStringField("source_system", sourceSystem);
        json.writeStringField("client_platform", clientPlatform);
        writeSourcePosition(json, metadata.sourcePosition());
    }

    private static void writeSourcePosition(JsonGenerator json, SourcePosition position)
            throws IOException {
        json.writeObjectFieldStart("source_position");
        json.writeStringField("topic", position.topic());
        json.writeNumberField("partition", position.partition());
        json.writeNumberField("offset", position.offset());
        json.writeNumberField("record_timestamp", position.recordTimestamp());
        json.writeStringField("timestamp_type", position.timestampType());
        json.writeEndObject();
    }

    private static void writeIdentities(JsonGenerator json, List<StandardIdentity> identities)
            throws IOException {
        json.writeArrayFieldStart("identities");
        for (StandardIdentity identity : identities) {
            json.writeStartObject();
            json.writeStringField("type", identity.type());
            json.writeStringField("value_hash", identity.valueHash());
            json.writeStringField("normalization_version", identity.normalizationVersion());
            json.writeStringField("key_version", identity.keyVersion());
            json.writeEndObject();
        }
        json.writeEndArray();
    }

    private static void writeCampaign(JsonGenerator json, CampaignRef campaign) throws IOException {
        json.writeObjectFieldStart("campaign");
        json.writeStringField("platform", campaign.platform());
        json.writeStringField("account_id", campaign.accountId());
        json.writeStringField("campaign_id", campaign.campaignId());
        json.writeStringField("ad_group_id", campaign.adGroupId());
        json.writeStringField("creative_id", campaign.creativeId());
        json.writeEndObject();
    }

    private static byte[] encodeJson(JsonFieldsWriter fieldsWriter) {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
             JsonGenerator json = JSON_FACTORY.createGenerator(buffer)) {
            json.writeStartObject();
            fieldsWriter.write(json);
            json.writeEndObject();
            json.flush();
            return buffer.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode normalization output", exception);
        }
    }

    private static KafkaOutputRecord output(String topic, String key, byte[] payload) {
        return new KafkaOutputRecord(topic, key.getBytes(StandardCharsets.UTF_8), payload);
    }

    private static String sourcePositionKey(SourcePosition position) {
        return position.topic() + ":" + position.partition() + ":" + position.offset();
    }

    private static void requireV1(StandardEventMetadata metadata) {
        if (metadata.schemaVersion() != WIRE_SCHEMA_VERSION) {
            throw new IllegalArgumentException(
                    "normalization Kafka encoder only supports schema version 1");
        }
    }

    private static void validateClick(StandardClickEvent event) {
        validateCommon(
                event.metadata(), event.scope(), event.sourceSystem(), event.clientPlatform());
        requirePrefixedId(event.clickId(), "clk_v1_", "clickId");
        validateIdentities(event.identities());
        requireToken(event.campaign().platform(), "campaign.platform");
        requireText(event.campaign().accountId(), 1, 128, "campaign.accountId");
        requireText(event.campaign().campaignId(), 0, 128, "campaign.campaignId");
        requireText(event.campaign().adGroupId(), 0, 128, "campaign.adGroupId");
        requireText(event.campaign().creativeId(), 0, 128, "campaign.creativeId");
    }

    private static void validateActivation(StandardActivationEvent event) {
        validateCommon(
                event.metadata(), event.scope(), event.sourceSystem(), event.clientPlatform());
        requirePrefixedId(event.activationId(), "act_v1_", "activationId");
        requireId(event.installId(), "installId");
        if (!event.directClickId().isEmpty()) {
            requirePrefixedId(event.directClickId(), "clk_v1_", "directClickId");
        }
        validateIdentities(event.identities());
    }

    private static void validateCommon(
            StandardEventMetadata metadata,
            AttributionScope scope,
            String sourceSystem,
            String clientPlatform) {
        requireV1(metadata);
        requirePrefixedId(metadata.eventId(), "evt_v1_", "eventId");
        long eventTime = metadata.eventTime().toEpochMilli();
        long ingestTime = metadata.ingestTime().toEpochMilli();
        if (eventTime < MIN_EVENT_TIME || eventTime >= MAX_EVENT_TIME
                || ingestTime < MIN_EVENT_TIME || ingestTime >= MAX_EVENT_TIME) {
            throw new IllegalArgumentException("event and ingest times must fit the v1 range");
        }
        validateSourcePosition(metadata.sourcePosition());
        requireId(scope.tenantId(), "tenantId");
        requireId(scope.gameId(), "gameId");
        requireId(scope.appId(), "appId");
        requireToken(sourceSystem, "sourceSystem");
        requireToken(clientPlatform, "clientPlatform");
    }

    private static void validateSourcePosition(SourcePosition position) {
        Objects.requireNonNull(position, "sourcePosition");
        if (position.topic().length() > 249
                || position.topic().equals(".")
                || position.topic().equals("..")
                || !position.topic().matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException("sourcePosition topic is not a Kafka topic");
        }
        if (!TIMESTAMP_TYPES.contains(position.timestampType())) {
            throw new IllegalArgumentException("sourcePosition timestampType is unsupported");
        }
    }

    private static void validateIdentities(List<StandardIdentity> identities) {
        if (identities.isEmpty() || identities.size() > MAX_IDENTITIES) {
            throw new IllegalArgumentException("identities must contain between 1 and 16 entries");
        }
        for (StandardIdentity identity : identities) {
            requireToken(identity.type(), "identity.type");
            if (!VERSION.matcher(identity.normalizationVersion()).matches()
                    || !VERSION.matcher(identity.keyVersion()).matches()) {
                throw new IllegalArgumentException("identity versions must be v1 wire tokens");
            }
        }
    }

    private static void requirePrefixedId(String value, String prefix, String field) {
        requireId(value, field);
        if (!value.startsWith(prefix) || value.length() == prefix.length()) {
            throw new IllegalArgumentException(field + " must use the v1 namespace prefix");
        }
    }

    private static void requireId(String value, String field) {
        if (value == null || !ID.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be a v1 wire identifier");
        }
    }

    private static void requireToken(String value, String field) {
        if (value == null || !TOKEN.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be a v1 wire token");
        }
    }

    private static void requireText(String value, int minimum, int maximum, String field) {
        if (value == null || value.length() < minimum || value.length() > maximum) {
            throw new IllegalArgumentException(field + " is outside the v1 length limit");
        }
    }

    private static String rejectionCodeToken(RejectionCode code) {
        return switch (code) {
            case NULL_KEY -> "null_key";
            case INVALID_KEY_UTF8 -> "invalid_key_utf8";
            case NULL_VALUE -> "null_value";
            case RECORD_TOO_LARGE -> "record_too_large";
            case INVALID_UTF8 -> "invalid_utf8";
            case MALFORMED_JSON -> "malformed_json";
            case DUPLICATE_JSON_FIELD -> "duplicate_json_field";
            case TRAILING_TOKENS -> "trailing_tokens";
            case MISSING_FIELD -> "missing_field";
            case UNKNOWN_FIELD -> "unknown_field";
            case TYPE_MISMATCH -> "type_mismatch";
            case UNSUPPORTED_EVENT_TYPE -> "unsupported_event_type";
            case UNSUPPORTED_SCHEMA_VERSION -> "unsupported_schema_version";
            case TOPIC_EVENT_TYPE_MISMATCH -> "topic_event_type_mismatch";
            case KEY_PAYLOAD_MISMATCH -> "key_payload_mismatch";
            case INVARIANT_VIOLATION -> "invariant_violation";
            case ALL_IDENTITIES_INVALID -> "all_identities_invalid";
        };
    }

    private static String identityIssueCodeToken(IdentityIssueCode code) {
        return switch (code) {
            case UNSUPPORTED_IDENTITY_TYPE -> "unsupported_identity_type";
            case INVALID_IDENTITY_VALUE -> "invalid_identity_value";
            case DUPLICATE_IDENTITY -> "duplicate_identity";
        };
    }

    @FunctionalInterface
    private interface JsonFieldsWriter {
        void write(JsonGenerator json) throws IOException;
    }
}
