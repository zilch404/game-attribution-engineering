package io.github.zilch404.attribution.model;

import java.io.Serializable;
import java.util.List;

public sealed interface StandardEvent extends Serializable
        permits StandardClickEvent, StandardActivationEvent {

    StandardEventMetadata metadata();

    AttributionScope scope();

    String sourceSystem();

    String clientPlatform();

    List<StandardIdentity> identities();
}
