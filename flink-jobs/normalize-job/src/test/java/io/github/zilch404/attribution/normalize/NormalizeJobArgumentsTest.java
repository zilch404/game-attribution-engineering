package io.github.zilch404.attribution.normalize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NormalizeJobArgumentsTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void parsesAnExplicitLearningDeployment() throws IOException {
        NormalizeJobConfig config = NormalizeJobArguments.parse(validArguments());

        assertThat(config.parallelism()).isEqualTo(2);
        assertThat(config.startPosition().kind().name()).isEqualTo("COMMITTED");
        assertThat(config.outputTopics().identityIssueTopic())
                .isEqualTo("game-attribution.normalization.identity-issue.v1");
        config.requireExplicitPlaceholderOptIn();

        assertThat(NormalizeJobArguments.parse(without(
                        validArguments(), "--start-position=committed"))
                        .startPosition().kind().name())
                .isEqualTo("COMMITTED");
    }

    @Test
    void defaultsToFailingClosedForThePlaceholderHash() throws IOException {
        String[] arguments = without(validArguments(),
                "--allow-placeholder-identity-hash=true");

        NormalizeJobConfig config = NormalizeJobArguments.parse(arguments);

        assertThatThrownBy(config::requireExplicitPlaceholderOptIn)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("unkeyed learning placeholder");
    }

    @Test
    void requiresDurableCheckpointStorageUnlessLocalUseIsExplicit() {
        String[] arguments = replace(
                without(validArguments(), "--allow-local-checkpoint-storage=true"),
                "--checkpoint-storage=s3://flink/checkpoints/normalize-v1",
                "--checkpoint-storage=file:///tmp/normalize-checkpoints");

        assertThatThrownBy(() -> NormalizeJobArguments.parse(arguments))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("local-only");
    }

    @Test
    void rejectsATransactionWindowThatCannotCoverCheckpointAndRecovery() {
        String[] arguments = replace(
                validArguments(),
                "--transaction-timeout-ms=900000",
                "--transaction-timeout-ms=720000");

        assertThatThrownBy(() -> NormalizeJobArguments.parse(arguments))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkpointTimeout + maxRecoveryDuration");
    }

    @Test
    void rejectsTopicLoopsAndDuplicateArguments() {
        assertThatThrownBy(() -> NormalizeJobArguments.parse(replace(
                        validArguments(),
                        "--standard-click-topic=game-attribution.standard.click.v1",
                        "--standard-click-topic=game-attribution.raw.click.v1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topics");

        String[] duplicated = java.util.Arrays.copyOf(
                validArguments(), validArguments().length + 1);
        duplicated[duplicated.length - 1] = "--parallelism=3";
        assertThatThrownBy(() -> NormalizeJobArguments.parse(duplicated))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate argument");
    }

    @Test
    void rejectsUnknownArgumentsAndCredentialsInCheckpointUris() {
        assertThatThrownBy(() -> NormalizeJobArguments.parse(append(
                        validArguments(),
                        "--quietly-ignore-this=true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown arguments");

        String[] credentialUri = replace(
                validArguments(),
                "--checkpoint-storage=s3://flink/checkpoints/normalize-v1",
                "--checkpoint-storage=s3://access:secret@flink/checkpoints/normalize-v1");
        assertThatThrownBy(() -> NormalizeJobArguments.parse(credentialUri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not contain credentials")
                .hasMessageNotContaining("access")
                .hasMessageNotContaining("secret");
    }

    @Test
    void loadsScopedKafkaPropertiesWithoutRenderingSecretValues() throws IOException {
        Path properties = temporaryDirectory.resolve("kafka.properties");
        Files.writeString(properties, """
                common.security.protocol=SASL_SSL
                consumer.fetch.max.bytes=1048576
                producer.sasl.jaas.config=secret-player-token
                """);
        String[] arguments = append(
                validArguments(),
                "--kafka-properties-file=" + properties);

        NormalizeJobConfig config = NormalizeJobArguments.parse(arguments);

        assertThat(config.consumerProperties())
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("fetch.max.bytes", "1048576");
        assertThat(config.producerProperties())
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.jaas.config", "secret-player-token");
        assertThat(config.toString())
                .contains("sasl.jaas.config")
                .doesNotContain("secret-player-token");
    }

    @Test
    void requiresTimestampOnlyForTimestampStartMode() throws IOException {
        assertThatThrownBy(() -> NormalizeJobArguments.parse(append(
                        validArguments(),
                        "--start-timestamp-millis=1700000000000")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only valid");

        String[] timestampArguments = append(
                replace(
                        validArguments(),
                        "--start-position=committed",
                        "--start-position=timestamp"),
                "--start-timestamp-millis=1700000000000");
        assertThat(NormalizeJobArguments.parse(timestampArguments)
                        .startPosition().timestampMillis())
                .isEqualTo(1_700_000_000_000L);
    }

    private static String[] validArguments() {
        return new String[] {
                "--bootstrap-servers=kafka:9092",
                "--raw-click-topic=game-attribution.raw.click.v1",
                "--raw-activation-topic=game-attribution.raw.activation.v1",
                "--click-group-id=normalize-click-v1",
                "--activation-group-id=normalize-activation-v1",
                "--start-position=committed",
                "--standard-click-topic=game-attribution.standard.click.v1",
                "--standard-activation-topic=game-attribution.standard.activation.v1",
                "--invalid-topic=game-attribution.normalization.invalid.v1",
                "--identity-issue-topic=game-attribution.normalization.identity-issue.v1",
                "--transactional-id-prefix=gae-normalize-learning-v1-",
                "--checkpoint-storage=s3://flink/checkpoints/normalize-v1",
                "--checkpoint-timeout-ms=120000",
                "--max-recovery-duration-ms=600000",
                "--transaction-timeout-ms=900000",
                "--parallelism=2",
                "--allow-placeholder-identity-hash=true",
                "--allow-local-checkpoint-storage=true"
        };
    }

    private static String[] without(String[] source, String value) {
        return java.util.Arrays.stream(source)
                .filter(candidate -> !candidate.equals(value))
                .toArray(String[]::new);
    }

    private static String[] replace(String[] source, String oldValue, String newValue) {
        return java.util.Arrays.stream(source)
                .map(candidate -> candidate.equals(oldValue) ? newValue : candidate)
                .toArray(String[]::new);
    }

    private static String[] append(String[] source, String value) {
        String[] appended = java.util.Arrays.copyOf(source, source.length + 1);
        appended[appended.length - 1] = value;
        return appended;
    }
}
