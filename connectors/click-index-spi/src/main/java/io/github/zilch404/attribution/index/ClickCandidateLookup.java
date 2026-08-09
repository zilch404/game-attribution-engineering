package io.github.zilch404.attribution.index;

import io.github.zilch404.attribution.core.CandidateSnapshot;

import java.util.concurrent.CompletionStage;

/**
 * Reads the complete bounded candidate set required by an attribution evaluation.
 *
 * <p>An adapter that applies a row limit must read {@code maxCandidates + 1}. If an extra
 * candidate exists after candidates from all queried identities are merged and deduplicated,
 * it must return {@code TRUNCATED}; it may return {@code AUTHORITATIVE} only after proving that
 * no additional candidate exists for the requested identities and time range. Client request
 * deadlines must be shorter than the surrounding Flink Async I/O timeout so abandoned requests
 * cannot accumulate after operator timeouts.</p>
 */
public interface ClickCandidateLookup extends AutoCloseable {
    CompletionStage<CandidateSnapshot> findCandidates(ClickCandidateQuery query);

    @Override
    default void close() throws Exception {
        // Most clients are lifecycle-free; production adapters can override.
    }
}
