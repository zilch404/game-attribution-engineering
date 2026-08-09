package io.github.zilch404.attribution.normalize;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.zilch404.attribution.kafka.KafkaInputRecord;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.EventType;
import io.github.zilch404.attribution.model.IdentityKey;
import io.github.zilch404.attribution.model.StandardActivationEvent;
import io.github.zilch404.attribution.model.StandardClickEvent;
import io.github.zilch404.attribution.model.StandardEventMetadata;
import io.github.zilch404.attribution.model.StandardIdentity;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class EventNormalizer implements Serializable {
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    static final int MAX_IDENTITIES = 16;
    private static final int MAX_IDENTITY_VALUE_LENGTH = 1024;
    private static final long MIN_EVENT_TIME = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
    private static final long MAX_EVENT_TIME = Instant.parse("2100-01-01T00:00:00Z").toEpochMilli();

    private static final Set<String> ENVELOPE_FIELDS = Set.of(
            "schema_version",
            "event_id",
            "event_type",
            "event_time",
            "ingest_time",
            "tenant_id",
            "game_id",
            "app_id",
            "source_system",
            "client_platform",
            "payload");
    private static final Set<String> CLICK_FIELDS = Set.of(
            "click_id", "ad_platform", "identities", "campaign", "eligible");
    private static final Set<String> ACTIVATION_FIELDS = Set.of(
            "activation_id", "install_id", "direct_click_id", "identities");
    private static final Set<String> CAMPAIGN_FIELDS = Set.of(
            "account_id", "campaign_id", "ad_group_id", "creative_id");
    private static final Set<String> IDENTITY_FIELDS = Set.of("type", "value");

    private final IdentityNormalizationPolicy identityPolicy;
    private final Duration maxFutureClockSkew;
    private final IdentityNormalizer identityNormalizer = new IdentityNormalizer();
    private transient JsonMapper jsonMapper;

    public EventNormalizer(
            IdentityNormalizationPolicy identityPolicy,
            Duration maxFutureClockSkew) {
        this.identityPolicy = Objects.requireNonNull(identityPolicy, "identityPolicy");
        this.maxFutureClockSkew = Objects.requireNonNull(
                maxFutureClockSkew, "maxFutureClockSkew");
        if (maxFutureClockSkew.isNegative() || maxFutureClockSkew.compareTo(Duration.ofDays(1)) > 0) {
            throw new IllegalArgumentException(
                    "maxFutureClockSkew must be between zero and one day");
        }
    }

    public NormalizationOutcome<StandardClickEvent> normalizeClick(
            KafkaInputRecord record,
            String expectedTopic) {
        Objects.requireNonNull(record, "record");
        try {
            ParsedEnvelope envelope = parseEnvelope(record, expectedTopic, EventType.CLICK);
            JsonNode payload = envelope.payload();
            rejectUnknownFields(payload, CLICK_FIELDS, "payload");

            String clickId = requireIdentifier(payload, "click_id", "payload.click_id");
            validateKafkaKey(record, clickId);
            String adPlatform = requireToken(
                    payload, "ad_platform", "payload.ad_platform");
            IdentityResult identityResult = normalizeIdentities(
                    payload,
                    envelope.scope(),
                    envelope.metadata().eventId(),
                    record.sourcePosition());

            JsonNode campaignNode = requireObject(payload, "campaign", "payload.campaign");
            rejectUnknownFields(campaignNode, CAMPAIGN_FIELDS, "payload.campaign");
            CampaignRef campaign = new CampaignRef(
                    adPlatform,
                    requireBoundedText(
                            campaignNode, "account_id", "payload.campaign.account_id", 128),
                    optionalBoundedText(
                            campaignNode, "campaign_id", "payload.campaign.campaign_id", 128),
                    optionalBoundedText(
                            campaignNode, "ad_group_id", "payload.campaign.ad_group_id", 128),
                    optionalBoundedText(
                            campaignNode, "creative_id", "payload.campaign.creative_id", 128));
            boolean eligible = requireBoolean(payload, "eligible", "payload.eligible");

            return new NormalizationOutcome.Accepted<>(
                    new StandardClickEvent(
                            envelope.metadata(),
                            envelope.scope(),
                            envelope.sourceSystem(),
                            envelope.clientPlatform(),
                            clickId,
                            identityResult.identities(),
                            campaign,
                            eligible),
                    identityResult.issues());
        } catch (DataRejection rejection) {
            return new NormalizationOutcome.Rejected<>(rejected(record, rejection));
        }
    }

    public NormalizationOutcome<StandardActivationEvent> normalizeActivation(
            KafkaInputRecord record,
            String expectedTopic) {
        Objects.requireNonNull(record, "record");
        try {
            ParsedEnvelope envelope = parseEnvelope(record, expectedTopic, EventType.ACTIVATION);
            JsonNode payload = envelope.payload();
            rejectUnknownFields(payload, ACTIVATION_FIELDS, "payload");

            String activationId = requireIdentifier(
                    payload, "activation_id", "payload.activation_id");
            validateKafkaKey(record, activationId);
            String installId = requireIdentifier(payload, "install_id", "payload.install_id");
            String directClickId = optionalIdentifier(
                    payload, "direct_click_id", "payload.direct_click_id");
            IdentityResult identityResult = normalizeIdentities(
                    payload,
                    envelope.scope(),
                    envelope.metadata().eventId(),
                    record.sourcePosition());

            return new NormalizationOutcome.Accepted<>(
                    new StandardActivationEvent(
                            envelope.metadata(),
                            envelope.scope(),
                            envelope.sourceSystem(),
                            envelope.clientPlatform(),
                            activationId,
                            installId,
                            directClickId,
                            identityResult.identities()),
                    identityResult.issues());
        } catch (DataRejection rejection) {
            return new NormalizationOutcome.Rejected<>(rejected(record, rejection));
        }
    }

    private ParsedEnvelope parseEnvelope(
            KafkaInputRecord record,
            String expectedTopic,
            EventType expectedType) {
        if (!record.sourcePosition().topic().equals(expectedTopic)) {
            throw reject(RejectionCode.TOPIC_EVENT_TYPE_MISMATCH, "source.topic");
        }
        byte[] value = record.payload();
        if (value == null) {
            throw reject(RejectionCode.NULL_VALUE, "value");
        }
        if (value.length > MAX_PAYLOAD_BYTES) {
            throw reject(RejectionCode.RECORD_TOO_LARGE, "value");
        }

        JsonNode root = parseJson(decodeUtf8(value));
        if (!root.isObject()) {
            throw reject(RejectionCode.TYPE_MISMATCH, "envelope");
        }

        int schemaVersion = requireInt(root, "schema_version", "schema_version");
        if (schemaVersion != 1) {
            throw reject(RejectionCode.UNSUPPORTED_SCHEMA_VERSION, "schema_version");
        }
        String eventType = requireBoundedText(root, "event_type", "event_type", 32);
        EventType parsedType;
        if (EventType.CLICK.wireName().equals(eventType)) {
            parsedType = EventType.CLICK;
        } else if (EventType.ACTIVATION.wireName().equals(eventType)) {
            parsedType = EventType.ACTIVATION;
        } else {
            throw reject(RejectionCode.UNSUPPORTED_EVENT_TYPE, "event_type");
        }
        if (parsedType != expectedType) {
            throw reject(RejectionCode.TOPIC_EVENT_TYPE_MISMATCH, "event_type");
        }
        rejectUnknownFields(root, ENVELOPE_FIELDS, "envelope");

        String eventId = requireIdentifier(root, "event_id", "event_id");
        long eventTimeMillis = requireLong(root, "event_time", "event_time");
        long ingestTimeMillis = requireLong(root, "ingest_time", "ingest_time");
        validateTimes(eventTimeMillis, ingestTimeMillis);
        AttributionScope scope = new AttributionScope(
                requireIdentifier(root, "tenant_id", "tenant_id"),
                requireIdentifier(root, "game_id", "game_id"),
                requireIdentifier(root, "app_id", "app_id"));
        String sourceSystem = requireToken(root, "source_system", "source_system");
        String clientPlatform = requireToken(root, "client_platform", "client_platform");
        JsonNode payload = requireObject(root, "payload", "payload");

        return new ParsedEnvelope(
                new StandardEventMetadata(
                        eventId,
                        parsedType,
                        Instant.ofEpochMilli(eventTimeMillis),
                        Instant.ofEpochMilli(ingestTimeMillis),
                        schemaVersion,
                        record.sourcePosition()),
                scope,
                sourceSystem,
                clientPlatform,
                payload);
    }

    private IdentityResult normalizeIdentities(
            JsonNode payload,
            AttributionScope scope,
            String eventId,
            io.github.zilch404.attribution.model.SourcePosition sourcePosition) {
        JsonNode identitiesNode = payload.get("identities");
        if (identitiesNode == null) {
            throw reject(RejectionCode.MISSING_FIELD, "payload.identities");
        }
        if (!identitiesNode.isArray()) {
            throw reject(RejectionCode.TYPE_MISMATCH, "payload.identities");
        }
        if (identitiesNode.isEmpty() || identitiesNode.size() > MAX_IDENTITIES) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, "payload.identities");
        }

        List<StandardIdentity> identities = new ArrayList<>();
        List<IdentityNormalizationIssue> issues = new ArrayList<>();
        Set<StandardIdentity> seen = new HashSet<>();
        for (int index = 0; index < identitiesNode.size(); index++) {
            JsonNode identityNode = identitiesNode.get(index);
            if (!identityNode.isObject()) {
                throw reject(RejectionCode.TYPE_MISMATCH, "payload.identities");
            }
            rejectUnknownFields(identityNode, IDENTITY_FIELDS, "payload.identities");
            JsonNode typeNode = identityNode.get("type");
            JsonNode valueNode = identityNode.get("value");
            if (typeNode == null || valueNode == null) {
                throw reject(RejectionCode.MISSING_FIELD, "payload.identities");
            }
            if (!typeNode.isTextual() || !valueNode.isTextual()) {
                throw reject(RejectionCode.TYPE_MISMATCH, "payload.identities");
            }
            String rawType = typeNode.textValue();
            String rawValue = valueNode.textValue();
            if (rawValue.length() > MAX_IDENTITY_VALUE_LENGTH) {
                issues.add(issue(
                        sourcePosition, eventId, index, IdentityIssueCode.INVALID_IDENTITY_VALUE));
                continue;
            }

            java.util.Optional<IdentityNormalizationRule> configuredRule =
                    identityPolicy.ruleFor(rawType);
            if (configuredRule.isEmpty()) {
                issues.add(issue(
                        sourcePosition, eventId, index, IdentityIssueCode.UNSUPPORTED_IDENTITY_TYPE));
                continue;
            }

            IdentityNormalizationRule rule = configuredRule.orElseThrow();
            IdentityKey key;
            try {
                key = identityNormalizer.normalize(
                        scope, rawType, rawValue, rule.canonicalization());
            } catch (IllegalArgumentException exception) {
                issues.add(issue(
                        sourcePosition, eventId, index, IdentityIssueCode.INVALID_IDENTITY_VALUE));
                continue;
            }
            StandardIdentity identity = new StandardIdentity(
                    key.type(),
                    key.valueHash(),
                    rule.normalizationVersion(),
                    rule.keyVersion());
            if (!seen.add(identity)) {
                issues.add(issue(
                        sourcePosition, eventId, index, IdentityIssueCode.DUPLICATE_IDENTITY));
                continue;
            }
            identities.add(identity);
        }
        if (identities.isEmpty()) {
            throw reject(RejectionCode.ALL_IDENTITIES_INVALID, "payload.identities");
        }
        return new IdentityResult(identities.stream().sorted().toList(), List.copyOf(issues));
    }

    private IdentityNormalizationIssue issue(
            io.github.zilch404.attribution.model.SourcePosition sourcePosition,
            String eventId,
            int index,
            IdentityIssueCode code) {
        return new IdentityNormalizationIssue(
                sourcePosition, eventId, index, code);
    }

    private void validateTimes(long eventTimeMillis, long ingestTimeMillis) {
        if (eventTimeMillis < MIN_EVENT_TIME || eventTimeMillis >= MAX_EVENT_TIME) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, "event_time");
        }
        if (ingestTimeMillis < MIN_EVENT_TIME || ingestTimeMillis >= MAX_EVENT_TIME) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, "ingest_time");
        }
        if (eventTimeMillis > ingestTimeMillis + maxFutureClockSkew.toMillis()) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, "event_time");
        }
    }

    private void validateKafkaKey(KafkaInputRecord record, String expectedKey) {
        byte[] key = record.key();
        if (key == null) {
            throw reject(RejectionCode.NULL_KEY, "key");
        }
        String decoded;
        try {
            decoded = decodeUtf8(key);
        } catch (DataRejection rejection) {
            throw reject(RejectionCode.INVALID_KEY_UTF8, "key");
        }
        if (!decoded.equals(expectedKey)) {
            throw reject(RejectionCode.KEY_PAYLOAD_MISMATCH, "key");
        }
    }

    private JsonNode parseJson(String json) {
        try (JsonParser parser = mapper().getFactory().createParser(json)) {
            JsonNode root = mapper().readTree(parser);
            if (parser.nextToken() != null) {
                throw reject(RejectionCode.TRAILING_TOKENS, "value");
            }
            if (root == null) {
                throw reject(RejectionCode.MALFORMED_JSON, "value");
            }
            return root;
        } catch (DataRejection rejection) {
            throw rejection;
        } catch (IOException exception) {
            String message = exception.getMessage();
            if (message != null && message.toLowerCase(java.util.Locale.ROOT)
                    .contains("duplicate field")) {
                throw reject(RejectionCode.DUPLICATE_JSON_FIELD, "value");
            }
            throw reject(RejectionCode.MALFORMED_JSON, "value");
        }
    }

    private JsonMapper mapper() {
        if (jsonMapper == null) {
            JsonFactory factory = JsonFactory.builder()
                    .streamReadConstraints(StreamReadConstraints.builder()
                            .maxNestingDepth(16)
                            .maxStringLength(4096)
                            .maxNumberLength(32)
                            .build())
                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                    .build();
            jsonMapper = JsonMapper.builder(factory).build();
        }
        return jsonMapper;
    }

    private static String decodeUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw reject(RejectionCode.INVALID_UTF8, "value");
        }
    }

    private static void rejectUnknownFields(
            JsonNode object,
            Set<String> allowed,
            String safeField) {
        object.fieldNames().forEachRemaining(field -> {
            if (!allowed.contains(field)) {
                throw reject(RejectionCode.UNKNOWN_FIELD, safeField);
            }
        });
    }

    private static JsonNode requireObject(JsonNode parent, String name, String safeField) {
        JsonNode value = parent.get(name);
        if (value == null) {
            throw reject(RejectionCode.MISSING_FIELD, safeField);
        }
        if (!value.isObject()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        return value;
    }

    private static String requireIdentifier(JsonNode parent, String name, String safeField) {
        String value = requireBoundedText(parent, name, safeField, 128);
        if (!value.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, safeField);
        }
        return value;
    }

    private static String optionalIdentifier(JsonNode parent, String name, String safeField) {
        if (!parent.has(name)) {
            return "";
        }
        return requireIdentifier(parent, name, safeField);
    }

    private static String requireToken(JsonNode parent, String name, String safeField) {
        String value = requireBoundedText(parent, name, safeField, 64);
        if (!value.matches("[a-z0-9][a-z0-9_-]{0,63}")) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, safeField);
        }
        return value;
    }

    private static String requireBoundedText(
            JsonNode parent,
            String name,
            String safeField,
            int maxLength) {
        JsonNode value = parent.get(name);
        if (value == null) {
            throw reject(RejectionCode.MISSING_FIELD, safeField);
        }
        if (!value.isTextual()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        String text = value.textValue();
        if (text.isBlank() || text.length() > maxLength) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, safeField);
        }
        return text;
    }

    private static String optionalBoundedText(
            JsonNode parent,
            String name,
            String safeField,
            int maxLength) {
        if (!parent.has(name)) {
            return "";
        }
        JsonNode value = parent.get(name);
        if (!value.isTextual()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        String text = value.textValue();
        if (text.length() > maxLength) {
            throw reject(RejectionCode.INVARIANT_VIOLATION, safeField);
        }
        return text;
    }

    private static int requireInt(JsonNode parent, String name, String safeField) {
        JsonNode value = parent.get(name);
        if (value == null) {
            throw reject(RejectionCode.MISSING_FIELD, safeField);
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        return value.intValue();
    }

    private static long requireLong(JsonNode parent, String name, String safeField) {
        JsonNode value = parent.get(name);
        if (value == null) {
            throw reject(RejectionCode.MISSING_FIELD, safeField);
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        return value.longValue();
    }

    private static boolean requireBoolean(JsonNode parent, String name, String safeField) {
        JsonNode value = parent.get(name);
        if (value == null) {
            throw reject(RejectionCode.MISSING_FIELD, safeField);
        }
        if (!value.isBoolean()) {
            throw reject(RejectionCode.TYPE_MISMATCH, safeField);
        }
        return value.booleanValue();
    }

    private static RejectedRecord rejected(KafkaInputRecord record, DataRejection rejection) {
        byte[] value = record.payload();
        byte[] safeValue = value == null ? new byte[0] : value;
        return new RejectedRecord(
                record.sourcePosition(),
                sha256(safeValue),
                safeValue.length,
                rejection.code,
                rejection.safeField);
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static DataRejection reject(RejectionCode code, String safeField) {
        return new DataRejection(code, safeField);
    }

    private record ParsedEnvelope(
            StandardEventMetadata metadata,
            AttributionScope scope,
            String sourceSystem,
            String clientPlatform,
            JsonNode payload) {
    }

    private record IdentityResult(
            List<StandardIdentity> identities,
            List<IdentityNormalizationIssue> issues) {
    }

    private static final class DataRejection extends RuntimeException {
        private final RejectionCode code;
        private final String safeField;

        private DataRejection(RejectionCode code, String safeField) {
            super(null, null, false, false);
            this.code = code;
            this.safeField = safeField;
        }
    }
}
