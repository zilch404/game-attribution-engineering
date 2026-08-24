package io.github.zilch404.attribution.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validated settings for an exactly-once Kafka sink. */
public final class KafkaSinkSettings {
    private static final int MAX_TRANSACTIONAL_ID_PREFIX_BYTES = 128;
    private static final Set<String> FACTORY_MANAGED_PROPERTIES = Set.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.RETRIES_CONFIG,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);

    private final String bootstrapServers;
    private final String transactionalIdPrefix;
    private final Duration transactionTimeout;
    private final Map<String, String> producerProperties;

    public KafkaSinkSettings(
            String bootstrapServers,
            String transactionalIdPrefix,
            Duration transactionTimeout) {
        this(bootstrapServers, transactionalIdPrefix, transactionTimeout, Map.of());
    }

    public KafkaSinkSettings(
            String bootstrapServers,
            String transactionalIdPrefix,
            Duration transactionTimeout,
            Map<String, String> producerProperties) {
        this.bootstrapServers = requireNonBlank(bootstrapServers, "bootstrapServers");
        this.transactionalIdPrefix = requireValidTransactionalIdPrefix(transactionalIdPrefix);
        this.transactionTimeout = requireValidTransactionTimeout(transactionTimeout);
        this.producerProperties = copyProducerProperties(producerProperties);
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public String transactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    public Duration transactionTimeout() {
        return transactionTimeout;
    }

    public Map<String, String> producerProperties() {
        return producerProperties;
    }

    @Override
    public String toString() {
        return "KafkaSinkSettings["
                + "bootstrapServers=" + bootstrapServers
                + ", transactionalIdPrefix=" + transactionalIdPrefix
                + ", transactionTimeout=" + transactionTimeout
                + ", producerPropertyKeys=" + producerProperties.keySet()
                + ']';
    }

    private static Duration requireValidTransactionTimeout(Duration transactionTimeout) {
        Objects.requireNonNull(transactionTimeout, "transactionTimeout");
        final long timeoutMillis;
        try {
            timeoutMillis = transactionTimeout.toMillis();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("transactionTimeout is too large", exception);
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("transactionTimeout must be at least one millisecond");
        }
        if (timeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "transactionTimeout must not exceed " + Integer.MAX_VALUE + " milliseconds");
        }
        return transactionTimeout;
    }

    private static String requireValidTransactionalIdPrefix(String transactionalIdPrefix) {
        requireNonBlank(transactionalIdPrefix, "transactionalIdPrefix");
        if (transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                > MAX_TRANSACTIONAL_ID_PREFIX_BYTES) {
            throw new IllegalArgumentException(
                    "transactionalIdPrefix must not exceed "
                            + MAX_TRANSACTIONAL_ID_PREFIX_BYTES
                            + " UTF-8 bytes");
        }
        return transactionalIdPrefix;
    }

    private static Map<String, String> copyProducerProperties(Map<String, String> properties) {
        Objects.requireNonNull(properties, "producerProperties");
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = requireNonBlank(property.getKey(), "producer property key");
            Objects.requireNonNull(property.getValue(), "producer property value for " + key);
            if (FACTORY_MANAGED_PROPERTIES.contains(key)) {
                throw new IllegalArgumentException(
                        "producerProperties must not override factory-managed property: " + key);
            }
        }
        return Map.copyOf(properties);
    }

    private static String requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
