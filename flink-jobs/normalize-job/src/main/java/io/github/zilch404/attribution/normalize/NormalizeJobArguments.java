package io.github.zilch404.attribution.normalize;

import io.github.zilch404.attribution.kafka.KafkaStartPosition;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Strict command-line adapter; secrets belong in a mounted properties file, not argv. */
public final class NormalizeJobArguments {
    private static final long MAX_KAFKA_PROPERTIES_FILE_BYTES = 1024 * 1024;
    private static final int MAX_KAFKA_PROPERTIES = 256;
    private static final int MAX_KAFKA_PROPERTY_NAME_LENGTH = 256;
    private static final int MAX_KAFKA_PROPERTY_VALUE_LENGTH = 16 * 1024;
    private static final Set<String> ALLOWED_ARGUMENTS = Set.of(
            "bootstrap-servers",
            "raw-click-topic",
            "raw-activation-topic",
            "click-group-id",
            "activation-group-id",
            "start-position",
            "start-timestamp-millis",
            "standard-click-topic",
            "standard-activation-topic",
            "invalid-topic",
            "identity-issue-topic",
            "transactional-id-prefix",
            "checkpoint-storage",
            "checkpoint-interval-ms",
            "checkpoint-timeout-ms",
            "checkpoint-min-pause-ms",
            "max-recovery-duration-ms",
            "transaction-timeout-ms",
            "max-future-clock-skew-ms",
            "parallelism",
            "allow-placeholder-identity-hash",
            "allow-local-checkpoint-storage",
            "kafka-properties-file");

    private NormalizeJobArguments() {
    }

    public static NormalizeJobConfig parse(String[] arguments) throws IOException {
        Map<String, String> values = parsePairs(arguments);
        rejectUnknownArguments(values.keySet());
        KafkaProperties kafkaProperties = loadKafkaProperties(
                values.get("kafka-properties-file"));

        return new NormalizeJobConfig(
                required(values, "bootstrap-servers"),
                required(values, "raw-click-topic"),
                required(values, "raw-activation-topic"),
                required(values, "click-group-id"),
                required(values, "activation-group-id"),
                parseStartPosition(values),
                new NormalizationOutputTopics(
                        required(values, "standard-click-topic"),
                        required(values, "standard-activation-topic"),
                        required(values, "invalid-topic"),
                        required(values, "identity-issue-topic")),
                required(values, "transactional-id-prefix"),
                required(values, "checkpoint-storage"),
                duration(values, "checkpoint-interval-ms", 30_000),
                duration(values, "checkpoint-timeout-ms", 120_000),
                duration(values, "checkpoint-min-pause-ms", 10_000),
                duration(values, "max-recovery-duration-ms", 600_000),
                duration(values, "transaction-timeout-ms", 900_000),
                duration(values, "max-future-clock-skew-ms", 300_000),
                positiveInt(values, "parallelism"),
                bool(values, "allow-placeholder-identity-hash", false),
                bool(values, "allow-local-checkpoint-storage", false),
                kafkaProperties.consumer(),
                kafkaProperties.producer());
    }

    private static Map<String, String> parsePairs(String[] arguments) {
        if (arguments == null) {
            throw new IllegalArgumentException("arguments must not be null");
        }
        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            String token = arguments[index];
            if (token == null || !token.startsWith("--") || token.length() == 2) {
                throw new IllegalArgumentException(
                        "argument at index " + index + " must use --name=value or --name value");
            }

            String body = token.substring(2);
            int separator = body.indexOf('=');
            String name;
            String value;
            if (separator >= 0) {
                name = body.substring(0, separator);
                value = body.substring(separator + 1);
            } else {
                name = body;
                if (++index >= arguments.length || arguments[index].startsWith("--")) {
                    throw new IllegalArgumentException("missing value for --" + name);
                }
                value = arguments[index];
            }
            if (name.isBlank() || value.isBlank()) {
                throw new IllegalArgumentException("argument names and values must not be blank");
            }
            if (values.putIfAbsent(name, value) != null) {
                throw new IllegalArgumentException("duplicate argument: --" + name);
            }
        }
        return Map.copyOf(values);
    }

    private static void rejectUnknownArguments(Set<String> names) {
        Set<String> unknown = new HashSet<>(names);
        unknown.removeAll(ALLOWED_ARGUMENTS);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("unknown arguments: " + unknown);
        }
    }

    private static KafkaStartPosition parseStartPosition(Map<String, String> values) {
        String start = values.getOrDefault("start-position", "committed");
        String timestamp = values.get("start-timestamp-millis");
        return switch (start) {
            case "committed" -> {
                rejectUnusedTimestamp(timestamp, start);
                yield KafkaStartPosition.committed();
            }
            case "committed-or-earliest" -> {
                rejectUnusedTimestamp(timestamp, start);
                yield KafkaStartPosition.committedOrEarliest();
            }
            case "earliest" -> {
                rejectUnusedTimestamp(timestamp, start);
                yield KafkaStartPosition.earliest();
            }
            case "timestamp" -> {
                if (timestamp == null) {
                    throw new IllegalArgumentException(
                            "--start-timestamp-millis is required for timestamp start position");
                }
                yield KafkaStartPosition.timestamp(parseLong(timestamp, "start-timestamp-millis"));
            }
            default -> throw new IllegalArgumentException(
                    "start-position must be committed, committed-or-earliest, earliest, or timestamp");
        };
    }

    private static void rejectUnusedTimestamp(String timestamp, String start) {
        if (timestamp != null) {
            throw new IllegalArgumentException(
                    "--start-timestamp-millis is only valid with timestamp, not " + start);
        }
    }

    private static Duration duration(
            Map<String, String> values,
            String name,
            long defaultMillis) {
        return Duration.ofMillis(parseLong(
                values.getOrDefault(name, Long.toString(defaultMillis)), name));
    }

    private static int positiveInt(Map<String, String> values, String name) {
        long value = parseLong(required(values, name), name);
        if (value < 1 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be a positive integer");
        }
        return (int) value;
    }

    private static long parseLong(String value, String name) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be an integer", exception);
        }
    }

    private static boolean bool(
            Map<String, String> values,
            String name,
            boolean defaultValue) {
        String value = values.get(name);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }

    private static String required(Map<String, String> values, String name) {
        String value = values.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing required argument: --" + name);
        }
        return value;
    }

    private static KafkaProperties loadKafkaProperties(String filename) throws IOException {
        if (filename == null) {
            return new KafkaProperties(Map.of(), Map.of());
        }
        Path path = Path.of(filename);
        if (Files.size(path) > MAX_KAFKA_PROPERTIES_FILE_BYTES) {
            throw new IllegalArgumentException("Kafka properties file must not exceed 1 MiB");
        }
        Properties loaded = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            loaded.load(input);
        }
        if (loaded.size() > MAX_KAFKA_PROPERTIES) {
            throw new IllegalArgumentException(
                    "Kafka properties file must not contain more than 256 properties");
        }

        Map<String, String> common = new HashMap<>();
        Map<String, String> consumer = new HashMap<>();
        Map<String, String> producer = new HashMap<>();
        for (String name : loaded.stringPropertyNames()) {
            String value = loaded.getProperty(name);
            if (name.length() > MAX_KAFKA_PROPERTY_NAME_LENGTH
                    || value.length() > MAX_KAFKA_PROPERTY_VALUE_LENGTH) {
                throw new IllegalArgumentException(
                        "Kafka property names and values exceed the supported size limit");
            }
            if (name.startsWith("common.")) {
                common.put(name.substring("common.".length()), value);
            } else if (name.startsWith("consumer.")) {
                consumer.put(name.substring("consumer.".length()), value);
            } else if (name.startsWith("producer.")) {
                producer.put(name.substring("producer.".length()), value);
            } else {
                throw new IllegalArgumentException(
                        "Kafka property must start with common., consumer., or producer.: " + name);
            }
        }
        rejectBlankPropertyNames(common);
        rejectBlankPropertyNames(consumer);
        rejectBlankPropertyNames(producer);

        Map<String, String> mergedConsumer = new HashMap<>(common);
        mergedConsumer.putAll(consumer);
        Map<String, String> mergedProducer = new HashMap<>(common);
        mergedProducer.putAll(producer);
        return new KafkaProperties(Map.copyOf(mergedConsumer), Map.copyOf(mergedProducer));
    }

    private static void rejectBlankPropertyNames(Map<String, String> properties) {
        if (properties.containsKey("")) {
            throw new IllegalArgumentException("Kafka property name after prefix must not be blank");
        }
    }

    private record KafkaProperties(
            Map<String, String> consumer,
            Map<String, String> producer) {
    }
}
