package io.github.zilch404.attribution.job;

import io.github.zilch404.attribution.core.DecisionDraft;
import io.github.zilch404.attribution.core.EvaluationDisposition;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public final class DecisionRoutingProcessFunction
        extends ProcessFunction<DecisionDraft, DecisionDraft> {
    public static final OutputTag<DecisionDraft> RETRY_DRAFTS =
            new OutputTag<>("attribution-retry") { };
    public static final OutputTag<DecisionDraft> QUARANTINED_DRAFTS =
            new OutputTag<>("attribution-quarantine") { };
    public static final OutputTag<DecisionDraft> NOOP_AUDIT_DRAFTS =
            new OutputTag<>("attribution-noop-audit") { };

    public static DecisionRoute routeOf(DecisionDraft draft) {
        Objects.requireNonNull(draft, "draft");
        return switch (draft.disposition()) {
            case MATERIALIZE -> DecisionRoute.MATERIALIZE;
            case RETRY_ONLY -> DecisionRoute.RETRY;
            case QUARANTINE -> DecisionRoute.QUARANTINE;
            case NOOP -> DecisionRoute.AUDIT_ONLY;
        };
    }

    @Override
    public void processElement(
            DecisionDraft draft,
            ProcessFunction<DecisionDraft, DecisionDraft>.Context context,
            Collector<DecisionDraft> materializationOutput) {
        switch (routeOf(draft)) {
            case MATERIALIZE -> materializationOutput.collect(draft);
            case RETRY -> context.output(RETRY_DRAFTS, draft);
            case QUARANTINE -> context.output(QUARANTINED_DRAFTS, draft);
            case AUDIT_ONLY -> context.output(NOOP_AUDIT_DRAFTS, draft);
        }
    }
}
