package io.github.zilch404.attribution.core;

public interface AttributionEngine {
    String engineVersion();

    DecisionDraft evaluate(EvaluationCommand command);
}
