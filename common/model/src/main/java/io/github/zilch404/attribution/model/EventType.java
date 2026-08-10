package io.github.zilch404.attribution.model;

public enum EventType {
    CLICK("click"),
    ACTIVATION("activation");

    private final String wireName;

    EventType(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }
}
