package dev.dead.reactor;

import dev.dead.common.Key;
import dev.dead.common.Results;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Runs several reactive operations over ONE upstream subscription.
 * Reactor's publish(Function) shares the source among the operations and coordinates demand.
 */
public final class ReactiveStreamForker<T> {

    private final Flux<T> source;
    private final Map<Key<?>, Function<Flux<T>, ? extends Mono<?>>> forks = new LinkedHashMap<>();

    private ReactiveStreamForker(Flux<T> source) {
        this.source = Objects.requireNonNull(source);
    }

    public static <T> ReactiveStreamForker<T> from(Flux<T> source) {
        return new ReactiveStreamForker<>(source);
    }

    public <R> ReactiveStreamForker<T> fork(Key<R> key, Function<Flux<T>, Mono<R>> operation) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(operation, "operation");
        if (forks.putIfAbsent(key, operation) != null) {
            throw new IllegalArgumentException("Duplicate key '" + key + "'");
        }
        return this;
    }

    /**
     * Returns a cold Mono that subscribes to the source once and emits all fork results.
     * Each operation must produce exactly one result; use {@code Mono<Optional<R>>} when
     * an operation can have no value.
     */
    public Mono<Results> run() {
        var registered = new ArrayList<>(forks.entrySet());
        if (registered.isEmpty()) {
            return Mono.just(new Results(Map.of()));
        }

        return source.publish(shared -> {
            var results = new ArrayList<Mono<?>>(registered.size());
            for (var entry : registered) {
                var key = entry.getKey();
                var result = entry.getValue().apply(shared)
                        .switchIfEmpty(Mono.error(
                                new IllegalStateException("Fork '" + key + "' completed without a result")));
                results.add(result);
            }
            return Mono.zip(results, values -> {
                var collected = new LinkedHashMap<Key<?>, Object>();
                for (int i = 0; i < registered.size(); i++) {
                    collected.put(registered.get(i).getKey(), values[i]);
                }
                return new Results(collected);
            });
        }).single();
    }
}
