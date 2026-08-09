package io.github.zilch404.attribution.normalize;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public sealed interface NormalizationOutcome<T> extends Serializable {

    record Accepted<T>(T event, List<IdentityNormalizationIssue> identityIssues)
            implements NormalizationOutcome<T> {
        public Accepted {
            event = Objects.requireNonNull(event, "event");
            identityIssues = List.copyOf(identityIssues);
        }
    }

    record Rejected<T>(RejectedRecord rejection) implements NormalizationOutcome<T> {
        public Rejected {
            rejection = Objects.requireNonNull(rejection, "rejection");
        }
    }
}
