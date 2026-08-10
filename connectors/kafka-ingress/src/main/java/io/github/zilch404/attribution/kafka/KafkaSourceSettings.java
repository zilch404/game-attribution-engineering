package io.github.zilch404.attribution.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validated Kafka source configuration with unambiguous startup behavior. */
public final class KafkaSourceSettings {
    private static final Set<String> FACTORY_MANAGED_PROPERTIES = Set.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ConsumerConfig.CLIENT_ID_CONFIG,
            ConsumerConfig.GROUP_ID_CONFIG,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "client.id.prefix",
            "commit.offsets.on.checkpoint");

    private final String bootstrapServers;
    private final List<String> topics;
    private final String groupId;
    private final KafkaStartPosition startPosition;
    private final Map<String, String> consumerProperties;

    public KafkaSourceSettings(
            String bootstrapServers,
            List<String> topics,
            String groupId,
            KafkaStartPosition startPosition) {
        this(bootstrapServers, topics, groupId, startPosition, Map.of());
    }

    public KafkaSourceSettings(
            String bootstrapServers,
            List<String> topics,
            String groupId,
            KafkaStartPosition startPosition,
            Map<String, String> consumerProperties) {
        this.bootstrapServers = requireNonBlank(bootstrapServers, "bootstrapServers");
        this.topics = copyTopics(topics);
        this.groupId = requireNonBlank(groupId, "groupId");
        this.startPosition = Objects.requireNonNull(startPosition, "startPosition");
        this.consumerProperties = copyConsumerProperties(consumerProperties);
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public List<String> topics() {
        return topics;
    }

    public String groupId() {
        return groupId;
    }

    public KafkaStartPosition startPosition() {
        return startPosition;
    }

    public Map<String, String> consumerProperties() {
        return consumerProperties;
    }

    @Override
    public String toString() {
        return "KafkaSourceSettings["
                + "bootstrapServers=" + bootstrapServers
                + ", topics=" + topics
                + ", groupId=" + groupId
                + ", startPosition=" + startPosition
                + ", consumerPropertyKeys=" + consumerProperties.keySet()
                + ']';
    }

    private static List<String> copyTopics(List<String> topics) {
        Objects.requireNonNull(topics, "topics");
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }

        List<String> copy = List.copyOf(topics);
        Set<String> unique = new HashSet<>();
        for (String topic : copy) {
            requireNonBlank(topic, "topic");
            if (!unique.add(topic)) {
                throw new IllegalArgumentException("topics must not contain duplicates: " + topic);
            }
        }
        return copy;
    }

    private static Map<String, String> copyConsumerProperties(Map<String, String> properties) {
        Objects.requireNonNull(properties, "consumerProperties");
        for (Map.Entry<String, String> property : properties.entrySet()) {
            String key = requireNonBlank(property.getKey(), "consumer property key");
            Objects.requireNonNull(property.getValue(), "consumer property value for " + key);
            if (FACTORY_MANAGED_PROPERTIES.contains(key)) {
                throw new IllegalArgumentException(
                        "consumerProperties must not override factory-managed property: " + key);
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
