package io.github.zilch404.attribution.normalize;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Kafka topics written by the normalization job's version-one output contract. */
public record NormalizationOutputTopics(
        String standardClickTopic,
        String standardActivationTopic,
        String rejectedRecordTopic,
        String identityIssueTopic) implements Serializable {

    private static final int MAX_KAFKA_TOPIC_LENGTH = 249;

    public NormalizationOutputTopics {
        standardClickTopic = requireKafkaTopic(standardClickTopic, "standardClickTopic");
        standardActivationTopic = requireKafkaTopic(
                standardActivationTopic, "standardActivationTopic");
        rejectedRecordTopic = requireKafkaTopic(rejectedRecordTopic, "rejectedRecordTopic");
        identityIssueTopic = requireKafkaTopic(identityIssueTopic, "identityIssueTopic");

        List<String> topics = List.of(
                standardClickTopic,
                standardActivationTopic,
                rejectedRecordTopic,
                identityIssueTopic);
        Set<String> distinctTopics = new HashSet<>(topics);
        if (distinctTopics.size() != topics.size()) {
            throw new IllegalArgumentException("normalization output topics must be distinct");
        }
    }

    public List<String> asList() {
        return List.of(
                standardClickTopic,
                standardActivationTopic,
                rejectedRecordTopic,
                identityIssueTopic);
    }

    private static String requireKafkaTopic(String topic, String name) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        if (topic.length() > MAX_KAFKA_TOPIC_LENGTH) {
            throw new IllegalArgumentException(name + " must not exceed 249 characters");
        }
        if (topic.equals(".") || topic.equals("..")
                || !topic.matches("[a-zA-Z0-9._-]+")) {
            throw new IllegalArgumentException(name + " is not a valid Kafka topic name");
        }
        return topic;
    }
}
