package io.github.zilch404.attribution.normalize;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public final class IdentityNormalizationProcessFunction
        extends ProcessFunction<RawIdentity, NormalizedIdentity> {
    public static final OutputTag<InvalidIdentity> INVALID_IDENTITIES =
            new OutputTag<>("invalid-identities") { };

    private final IdentityNormalizer normalizer = new IdentityNormalizer();

    @Override
    public void processElement(
            RawIdentity value,
            ProcessFunction<RawIdentity, NormalizedIdentity>.Context context,
            Collector<NormalizedIdentity> output) {
        Objects.requireNonNull(value, "value");
        NormalizedIdentity normalized;
        try {
            normalized = normalize(value);
        } catch (IllegalArgumentException exception) {
            context.output(
                    INVALID_IDENTITIES,
                    new InvalidIdentity(value.eventId(), value.type(), exception.getMessage()));
            return;
        }
        output.collect(normalized);
    }

    NormalizedIdentity normalize(RawIdentity value) {
        return new NormalizedIdentity(
                value.eventId(),
                value.scope(),
                normalizer.normalize(
                        value.scope(),
                        value.type(),
                        value.rawValue(),
                        value.canonicalization()),
                IdentityNormalizer.NORMALIZATION_VERSION);
    }
}
