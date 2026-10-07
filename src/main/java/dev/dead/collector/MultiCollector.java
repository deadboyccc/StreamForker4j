package dev.dead.collector;

import dev.dead.common.Key;
import dev.dead.common.Results;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Runs several Collectors over ONE traversal, in the calling thread: the N-ary, keyed
 * generalisation of {@link java.util.stream.Collectors#teeing}. Works on parallel streams too.
 */
public final class MultiCollector<T> {

    private final Map<Key<?>, Part<T>> parts = new LinkedHashMap<>();

    public <R> MultiCollector<T> add(Key<R> key, Collector<? super T, ?, R> collector) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(collector, "collector");
        if (parts.putIfAbsent(key, Part.of(collector)) != null) {
            throw new IllegalArgumentException("Duplicate key '" + key + "'");
        }
        return this;
    }

    public Collector<T, ?, Results> build() {
        var keys = List.copyOf(parts.keySet());
        var registered = List.copyOf(parts.values());
        int count = registered.size();
        return Collector.of(
                () -> {
                    var containers = new Object[count];
                    for (int i = 0; i < count; i++) {
                        containers[i] = registered.get(i).supplier().get();
                    }
                    return containers;
                },
                (containers, element) -> {
                    for (int i = 0; i < count; i++) {
                        registered.get(i).accumulator().accept(containers[i], element);
                    }
                },
                (left, right) -> {
                    for (int i = 0; i < count; i++) {
                        left[i] = registered.get(i).combiner().apply(left[i], right[i]);
                    }
                    return left;
                },
                containers -> {
                    var values = new LinkedHashMap<Key<?>, Object>();
                    for (int i = 0; i < count; i++) {
                        values.put(keys.get(i), registered.get(i).finisher().apply(containers[i]));
                    }
                    return new Results(values);
                });
    }

    /**
     * A registered collector with its container type erased (A is captured exactly once, in of()).
     */
    private record Part<T>(Supplier<Object> supplier, BiConsumer<Object, T> accumulator,
                           BinaryOperator<Object> combiner, Function<Object, Object> finisher) {
        @SuppressWarnings("unchecked") // a container always comes from this same collector's supplier
        static <T, A> Part<T> of(Collector<? super T, A, ?> collector) {
            return new Part<>(collector.supplier()::get,
                    (container, element) -> collector.accumulator().accept((A) container, element),
                    (left, right) -> collector.combiner().apply((A) left, (A) right),
                    container -> collector.finisher().apply((A) container));
        }
    }
}
