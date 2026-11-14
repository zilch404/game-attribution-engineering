package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaStartPosition;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Fully validated runtime configuration for the normalization job. */
public record NormalizeJobConfig(
        String bootstrapServers,
        String rawClickTopic,
        String rawActivationTopic,
        String clickGroupId,
        String activationGroupId,
        KafkaStartPosition startPosition,
        NormalizationOutputTopics outputTopics,
        String transactionalIdPrefix,
        String checkpointStorage,
        Duration checkpointInterval,
        Duration checkpointTimeout,
        Duration checkpointMinPause,
        Duration maxRecoveryDuration,
        Duration transactionTimeout,
        Duration maxFutureClockSkew,
        int parallelism,
        boolean allowPlaceholderIdentityHash,
        boolean allowLocalCheckpointStorage,
        Map<String, String> consumerProperties,
        Map<String, String> producerProperties) {

    private static final Duration MAX_CHECKPOINT_INTERVAL = Duration.ofHours(1);
    private static final Duration MAX_OPERATION_DURATION = Duration.ofDays(7);

    public NormalizeJobConfig {
        bootstrapServers = requireNonBlank(bootstrapServers, "bootstrapServers");
        rawClickTopic = requireNonBlank(rawClickTopic, "rawClickTopic");
        rawActivationTopic = requireNonBlank(rawActivationTopic, "rawActivationTopic");
        clickGroupId = requireNonBlank(clickGroupId, "clickGroupId");
        activationGroupId = requireNonBlank(activationGroupId, "activationGroupId");
        startPosition = Objects.requireNonNull(startPosition, "startPosition");
        outputTopics = Objects.requireNonNull(outputTopics, "outputTopics");
        transactionalIdPrefix = requireTransactionPrefix(transactionalIdPrefix);
        checkpointStorage = validateCheckpointStorage(
                checkpointStorage, allowLocalCheckpointStorage);
        checkpointInterval = requirePositiveBounded(
                checkpointInterval, "checkpointInterval", MAX_CHECKPOINT_INTERVAL);
        checkpointTimeout = requirePositiveBounded(
                checkpointTimeout, "checkpointTimeout", MAX_OPERATION_DURATION);
        checkpointMinPause = requireNonNegativeBounded(
                checkpointMinPause, "checkpointMinPause", MAX_CHECKPOINT_INTERVAL);
        maxRecoveryDuration = requireNonNegativeBounded(
                maxRecoveryDuration, "maxRecoveryDuration", MAX_OPERATION_DURATION);
        transactionTimeout = requirePositiveBounded(
                transactionTimeout, "transactionTimeout", MAX_OPERATION_DURATION);
        maxFutureClockSkew = requireNonNegativeBounded(
                maxFutureClockSkew, "maxFutureClockSkew", Duration.ofDays(1));
        if (parallelism < 1 || parallelism > 1_024) {
            throw new IllegalArgumentException("parallelism must be between 1 and 1024");
        }
        consumerProperties = copyProperties(consumerProperties, "consumerProperties");
        producerProperties = copyProperties(producerProperties, "producerProperties");

        if (rawClickTopic.equals(rawActivationTopic)) {
            throw new IllegalArgumentException("raw click and activation topics must be different");
        }
        if (clickGroupId.equals(activationGroupId)) {
            throw new IllegalArgumentException("click and activation group IDs must be different");
        }
        Set<String> topics = new HashSet<>();
        topics.add(rawClickTopic);
        topics.add(rawActivationTopic);
        topics.addAll(outputTopics.asList());
        if (topics.size() != 6) {
            throw new IllegalArgumentException("all raw and output topics must be different");
        }

        Duration requiredTransactionWindow;
        try {
            requiredTransactionWindow = checkpointTimeout.plus(maxRecoveryDuration);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("checkpoint and recovery durations overflow", exception);
        }
        if (transactionTimeout.compareTo(requiredTransactionWindow) <= 0) {
            throw new IllegalArgumentException(
                    "transactionTimeout must be greater than checkpointTimeout "
                            + "+ maxRecoveryDuration");
        }
    }

    /** Runtime safety gate: the current built-in policy is for learning fixtures only. */
    public void requireExplicitPlaceholderOptIn() {
        if (!allowPlaceholderIdentityHash) {
            throw new IllegalStateException(
                    "the built-in identity hash is an unkeyed learning placeholder; "
                            + "set --allow-placeholder-identity-hash=true only for non-production data");
        }
    }

    @Override
    public String toString() {
        return "NormalizeJobConfig["
                + "bootstrapServers=" + bootstrapServers
                + ", rawClickTopic=" + rawClickTopic
                + ", rawActivationTopic=" + rawActivationTopic
                + ", clickGroupId=" + clickGroupId
                + ", activationGroupId=" + activationGroupId
                + ", startPosition=" + startPosition
                + ", outputTopics=" + outputTopics
                + ", transactionalIdPrefix=" + transactionalIdPrefix
                + ", checkpointStorage=" + checkpointStorage
                + ", checkpointInterval=" + checkpointInterval
                + ", checkpointTimeout=" + checkpointTimeout
                + ", checkpointMinPause=" + checkpointMinPause
                + ", maxRecoveryDuration=" + maxRecoveryDuration
                + ", transactionTimeout=" + transactionTimeout
                + ", maxFutureClockSkew=" + maxFutureClockSkew
                + ", parallelism=" + parallelism
                + ", allowPlaceholderIdentityHash=" + allowPlaceholderIdentityHash
                + ", allowLocalCheckpointStorage=" + allowLocalCheckpointStorage
                + ", consumerPropertyKeys=" + consumerProperties.keySet()
                + ", producerPropertyKeys=" + producerProperties.keySet()
                + ']';
    }

    private static String requireTransactionPrefix(String value) {
        String prefix = requireNonBlank(value, "transactionalIdPrefix");
        if (!prefix.matches("[A-Za-z0-9][A-Za-z0-9._-]{2,127}")) {
            throw new IllegalArgumentException(
                    "transactionalIdPrefix must be a stable deployment token of 3 to 128 characters");
        }
        return prefix;
    }

    private static String validateCheckpointStorage(String value, boolean allowLocal) {
        String storage = requireNonBlank(value, "checkpointStorage");
        URI uri;
        try {
            uri = URI.create(storage);
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("checkpointStorage must be a valid URI");
        }
        if (!uri.isAbsolute() || uri.getScheme() == null) {
            throw new IllegalArgumentException("checkpointStorage must be an absolute URI");
        }
        if (uri.getRawUserInfo() != null
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw new IllegalArgumentException(
                    "checkpointStorage must not contain credentials or query parameters");
        }
        if ("file".equalsIgnoreCase(uri.getScheme()) && !allowLocal) {
            throw new IllegalArgumentException(
                    "file checkpoint storage is local-only; explicitly set "
                            + "--allow-local-checkpoint-storage=true for learning environments");
        }
        return storage;
    }

    private static Duration requirePositiveBounded(
            Duration value,
            String field,
            Duration maximum) {
        Objects.requireNonNull(value, field);
        if (value.isZero() || value.isNegative() || value.compareTo(maximum) > 0) {
            throw new IllegalArgumentException(
                    field + " must be positive and no greater than " + maximum);
        }
        return value;
    }

    private static Duration requireNonNegativeBounded(
            Duration value,
            String field,
            Duration maximum) {
        Objects.requireNonNull(value, field);
        if (value.isNegative() || value.compareTo(maximum) > 0) {
            throw new IllegalArgumentException(
                    field + " must not be negative or greater than " + maximum);
        }
        return value;
    }

    private static Map<String, String> copyProperties(
            Map<String, String> properties,
            String field) {
        Objects.requireNonNull(properties, field);
        properties.forEach((key, value) -> {
            requireNonBlank(key, field + " key");
            Objects.requireNonNull(value, field + " value for " + key);
        });
        return Map.copyOf(properties);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
