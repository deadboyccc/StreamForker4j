package dev.dead.reactor;

import dev.dead.common.Key;
import dev.dead.common.Results;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveStreamForkerTest {

    private static final Key<Long> COUNT = Key.of("count");
    private static final Key<Integer> FIRST = Key.of("first");

    @Test
    void multicastsOneSubscriptionToAllOperations() {
        var subscriptions = new AtomicInteger();
        var count = Key.<Long>of("count");
        var names = Key.<String>of("names");

        Results results = ReactiveStreamForker.from(Flux.range(1, 5)
                        .doOnSubscribe(ignored -> subscriptions.incrementAndGet()))
                .fork(count, Flux::count)
                .fork(names, flux -> flux.map(String::valueOf).collectList().map(values -> String.join(",", values)))
                .run()
                .block();

        assertEquals(1, subscriptions.get());
        assertEquals(5L, results.get(count));
        assertEquals("1,2,3,4,5", results.get(names));
    }

    @Test
    void supportsShortCircuitedForksAndEmptySources() {
        Results results = ReactiveStreamForker.from(Flux.range(0, 1000))
                .fork(FIRST, Flux::next)
                .fork(COUNT, Flux::count)
                .run()
                .block();

        assertEquals(0, results.get(FIRST));
        assertEquals(1000L, results.get(COUNT));

        var optional = Key.<Optional<Integer>>of("firstOptional");
        Results emptyResults = ReactiveStreamForker.from(Flux.<Integer>empty())
                .fork(optional, flux -> flux.next().map(Optional::of).defaultIfEmpty(Optional.empty()))
                .run()
                .block();
        assertEquals(Optional.empty(), emptyResults.get(optional));
    }

    @Test
    void rejectsForksThatCompleteWithoutAValue() {
        var empty = Key.<Integer>of("empty");
        var failure = assertThrows(RuntimeException.class, () ->
                ReactiveStreamForker.from(Flux.<Integer>empty())
                        .fork(empty, flux -> flux.next())
                        .run()
                        .block());

        assertEquals("Fork 'empty' completed without a result", failure.getMessage());
    }

    @Test
    void rejectsDuplicateKeys() {
        var forker = ReactiveStreamForker.from(Flux.just(1)).fork(COUNT, Flux::count);

        assertThrows(IllegalArgumentException.class, () -> forker.fork(COUNT, Flux::count));
    }
}
