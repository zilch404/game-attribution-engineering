package io.github.zilch404.attribution.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StandardEventTest {
    private static final AttributionScope SCOPE =
            new AttributionScope("tenant", "game", "app");
    private static final SourcePosition POSITION =
            new SourcePosition("game-attribution.raw.click.v1", 2, 42, 1_786_200_001_500L,
                    "CREATE_TIME");

    @Test
    void stableIdsMatchThePublishedCrossLanguageVectors() {
        assertThat(StableEventIds.click(SCOPE, "gdt", "upstream-click-42"))
                .isEqualTo("clk_v1_erHBUSuikXfGAWCs16Ut1Xb4ub4ITtMBBz3f_nESAsM");
        assertThat(StableEventIds.activation(SCOPE, "android", "install-42"))
                .isEqualTo("act_v1_BVm-aMgcCiy9ZdyLGMDI0fVXh1POSuYAu9oXxardMnA");
        assertThat(StableEventIds.event(SCOPE, EventType.CLICK, "upstream-event-42"))
                .isEqualTo("evt_v1_LM-pOIpbzscVt9EFS0ITydgsurz1u4ZpYSxwoM7gcso");
    }

    @Test
    void standardClickSortsIdentitiesAndConvertsToTheNarrowCoreModel() {
        StandardIdentity oaid = identity("oaid", "b".repeat(64));
        StandardIdentity idfa = identity("idfa", "a".repeat(64));
        StandardClickEvent event = new StandardClickEvent(
                metadata(EventType.CLICK),
                SCOPE,
                "gdt_click_collector",
                "android",
                "clk_v1_click",
                List.of(oaid, idfa),
                new CampaignRef("gdt", "account", "campaign", "group", "creative"),
                true);

        assertThat(event.identities()).containsExactly(idfa, oaid);
        assertThat(event.toCoreEvent().eventId()).isEqualTo(event.metadata().eventId());
        assertThat(event.toCoreEvent().identities())
                .containsExactlyInAnyOrder(idfa.toIdentityKey(), oaid.toIdentityKey());
    }

    @Test
    void standardEventsRejectDuplicateCanonicalIdentities() {
        StandardIdentity identity = identity("oaid", "a".repeat(64));

        assertThatThrownBy(() -> new StandardActivationEvent(
                metadata(EventType.ACTIVATION),
                SCOPE,
                "game_sdk",
                "android",
                "act_v1_activation",
                "install-1",
                "",
                List.of(identity, identity)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates");
    }

    private static StandardEventMetadata metadata(EventType type) {
        return new StandardEventMetadata(
                "evt_v1_event",
                type,
                Instant.ofEpochMilli(1_786_200_000_000L),
                Instant.ofEpochMilli(1_786_200_001_000L),
                1,
                POSITION);
    }

    private static StandardIdentity identity(String type, String hash) {
        return new StandardIdentity(type, hash, "normalization-v1", "key-v1");
    }
}
