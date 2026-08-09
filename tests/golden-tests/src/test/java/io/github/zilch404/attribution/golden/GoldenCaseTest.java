package io.github.zilch404.attribution.golden;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.zilch404.attribution.core.AttributionOutcome;
import io.github.zilch404.attribution.core.AttributionRule;
import io.github.zilch404.attribution.core.CandidateEvidence;
import io.github.zilch404.attribution.core.CandidateReadStatus;
import io.github.zilch404.attribution.core.CandidateSnapshot;
import io.github.zilch404.attribution.core.DecisionReason;
import io.github.zilch404.attribution.core.DecisionStatus;
import io.github.zilch404.attribution.core.EvaluationCommand;
import io.github.zilch404.attribution.core.LastTouchAttributionEngine;
import io.github.zilch404.attribution.core.RuleKey;
import io.github.zilch404.attribution.model.ActivationEvent;
import io.github.zilch404.attribution.model.AttributionScope;
import io.github.zilch404.attribution.model.CampaignRef;
import io.github.zilch404.attribution.model.ClickEvent;
import io.github.zilch404.attribution.model.IdentityKey;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GoldenCaseTest {
    private static final AttributionScope SCOPE = new AttributionScope("tenant", "game", "app");
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();

    @TestFactory
    Stream<DynamicTest> everyGoldenCaseIsDeterministic() throws IOException {
        List<CaseSpec> cases;
        try (InputStream input = getClass().getResourceAsStream("/golden/attribution-cases.json")) {
            if (input == null) {
                throw new IllegalStateException("golden case resource is missing");
            }
            cases = MAPPER.readValue(input, new TypeReference<>() { });
        }

        return cases.stream().map(spec -> DynamicTest.dynamicTest(spec.name(), () -> verify(spec)));
    }

    private static void verify(CaseSpec spec) {
        AttributionRule rule = new AttributionRule(
                new RuleKey("golden-last-touch", 1),
                Duration.ofDays(7),
                spec.identityPriority());
        ActivationEvent activation = new ActivationEvent(
                "activation-event-" + spec.name(),
                "activation-" + spec.name(),
                SCOPE,
                spec.activationTime(),
                identities(spec.activationIdentities()));
        List<ClickEvent> clicks = spec.clicks().stream()
                .map(GoldenCaseTest::toClick)
                .toList();

        for (int seed = 0; seed < 25; seed++) {
            List<ClickEvent> shuffled = new ArrayList<>(clicks);
            java.util.Collections.shuffle(shuffled, new Random(seed));
            CandidateSnapshot snapshot = new CandidateSnapshot(
                    shuffled,
                    evidence(spec, shuffled.size()));
            var decision = new LastTouchAttributionEngine().evaluate(
                    new EvaluationCommand(activation, snapshot, rule, null));

            assertThat(decision.result().outcome()).as("outcome seed=%s", seed)
                    .isEqualTo(spec.expected().outcome());
            assertThat(decision.result().status()).as("status seed=%s", seed)
                    .isEqualTo(spec.expected().status());
            assertThat(decision.result().matchedClickId()).as("click seed=%s", seed)
                    .isEqualTo(spec.expected().matchedClickId());
            assertThat(decision.result().reason()).as("reason seed=%s", seed)
                    .isEqualTo(spec.expected().reason());
        }
    }

    private static CandidateEvidence evidence(CaseSpec spec, int returnedCount) {
        boolean hasCoverage = switch (spec.readStatus()) {
            case AUTHORITATIVE, PARTIAL, TRUNCATED -> true;
            default -> false;
        };
        return new CandidateEvidence(
                spec.readStatus(),
                "golden-" + spec.name(),
                hasCoverage ? spec.activationTime().minus(Duration.ofDays(7)) : null,
                hasCoverage ? spec.activationTime() : null,
                hasCoverage ? spec.activationTime().plusSeconds(60) : null,
                spec.readStatus() == CandidateReadStatus.TRUNCATED,
                returnedCount);
    }

    private static ClickEvent toClick(ClickSpec click) {
        boolean eligible = click.eligible() == null || click.eligible();
        return new ClickEvent(
                "click-event-" + click.clickId(),
                click.clickId(),
                SCOPE,
                click.clickTime(),
                identities(click.identities()),
                new CampaignRef(click.platform(), "", "campaign", "", ""),
                eligible);
    }

    private static Set<IdentityKey> identities(Map<String, String> values) {
        return values.entrySet().stream()
                .map(entry -> new IdentityKey(entry.getKey(), entry.getValue()))
                .collect(Collectors.toUnmodifiableSet());
    }

    private record CaseSpec(
            String name,
            Instant activationTime,
            Map<String, String> activationIdentities,
            List<String> identityPriority,
            CandidateReadStatus readStatus,
            List<ClickSpec> clicks,
            ExpectedDecision expected) {
    }

    private record ClickSpec(
            String clickId,
            Instant clickTime,
            Map<String, String> identities,
            String platform,
            Boolean eligible) {
    }

    private record ExpectedDecision(
            AttributionOutcome outcome,
            DecisionStatus status,
            String matchedClickId,
            DecisionReason reason) {
    }
}
