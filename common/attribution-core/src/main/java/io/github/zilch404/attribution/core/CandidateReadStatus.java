package io.github.zilch404.attribution.core;

public enum CandidateReadStatus {
    AUTHORITATIVE,
    PARTIAL,
    TRUNCATED,
    INDEX_NOT_READY,
    NO_SUPPORTED_IDENTITY,
    TIMED_OUT,
    FAILED,
    PERMANENT_FAILURE;

    public boolean isAuthoritative() {
        return this == AUTHORITATIVE;
    }
}
