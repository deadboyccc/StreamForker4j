package dev.dead.common;

import java.util.*;

/**
 * Immutable bag of results, read back with the same typed {@link Key} used to register them.
 */
public final class Results {
    private final Map<Key<?>, Object> values;

    public Results(Map<Key<?>, Object> values) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    @SuppressWarnings("unchecked") // builders only ever store an R under a Key<R>
    public <R> R get(Key<R> key) {
        Objects.requireNonNull(key, "key");
        if (!values.containsKey(key)) {
            throw new NoSuchElementException("No result for key '" + key + "'");
        }
        return (R) values.get(key);
    }
}
