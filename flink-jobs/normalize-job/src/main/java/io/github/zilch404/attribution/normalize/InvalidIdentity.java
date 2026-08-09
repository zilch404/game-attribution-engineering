package io.github.zilch404.attribution.normalize;

import java.io.Serializable;

public record InvalidIdentity(String eventId, String type, String reason) implements Serializable {
}
