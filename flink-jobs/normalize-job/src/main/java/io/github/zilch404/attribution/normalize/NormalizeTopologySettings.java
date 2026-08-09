package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaSourceSettings;

import java.time.Duration;
import java.util.Objects;

public record NormalizeTopologySettings(
        KafkaSourceSettings clickSource,
        KafkaSourceSettings activationSource,
        Duration maxFutureClockSkew) {

    public NormalizeTopologySettings {
        clickSource = requireSingleTopic(clickSource, "clickSource");
        activationSource = requireSingleTopic(activationSource, "activationSource");
        if (clickSource.topics().get(0).equals(activationSource.topics().get(0))) {
            throw new IllegalArgumentException("click and activation topics must be different");
        }
        if (clickSource.groupId().equals(activationSource.groupId())) {
            throw new IllegalArgumentException(
                    "click and activation sources must use different consumer groups");
        }
        maxFutureClockSkew = requireNonNegative(
                maxFutureClockSkew, "maxFutureClockSkew");
    }

    public String clickTopic() {
        return clickSource.topics().get(0);
    }

    public String activationTopic() {
        return activationSource.topics().get(0);
    }

    private static KafkaSourceSettings requireSingleTopic(
            KafkaSourceSettings settings,
            String field) {
        Objects.requireNonNull(settings, field);
        if (settings.topics().size() != 1) {
            throw new IllegalArgumentException(field + " must contain exactly one topic");
        }
        return settings;
    }

    private static Duration requireNonNegative(Duration value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isNegative()) {
            throw new IllegalArgumentException(field + " must not be negative");
        }
        return value;
    }
}
