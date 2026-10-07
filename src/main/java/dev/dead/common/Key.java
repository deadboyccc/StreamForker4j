package dev.dead.common;

/**
 * Typed, identity-based key: the result type travels with the key, so callers need no casts.
 */
public final class Key<R> {
    private final String name;

    private Key(String name) {
        this.name = name;
    }

    public static <R> Key<R> of(String name) {
        return new Key<>(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
