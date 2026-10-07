package dev.dead.modern;

import dev.dead.common.Key;
import dev.dead.common.Results;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamForkerTest {

    private static final Key<Long> COUNT = Key.of("count");
    private static final Key<Integer> FIRST = Key.of("first");

    @Test
    void handlesEmptyStreamsAndBatchBoundaries() {
        Key<Long> size = Key.of("size");
        for (int count : new int[]{0, 1, 511, 512, 513, 1024, 4103}) {
            Results results = StreamForker.from(IntStream.range(0, count).boxed())
                    .fork(size, Stream::count)
                    .run();
            assertEquals((long) count, results.get(size), "stream size " + count);
        }

    }

    @Test
    void supportsNullElementsAndShortCircuitedForks() {
        Results nullResults = StreamForker.from(Stream.of("a", null, "b"))
                .fork(COUNT, Stream::count)
                .run();
        assertEquals(3L, nullResults.get(COUNT));

        Results earlyResults = StreamForker.from(Stream.iterate(0, value -> value + 1).limit(10_000))
                .fork(FIRST, stream -> stream.findFirst().orElseThrow())
                .fork(COUNT, Stream::count)
                .run();
        assertEquals(0, earlyResults.get(FIRST));
        assertEquals(10_000L, earlyResults.get(COUNT));
    }

    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @Test
    void stopsFeedingAndReportsForkFailures() {
        var failureKey = Key.<Integer>of("failure");

        var failure = assertThrows(CompletionException.class, () ->
                StreamForker.from(Stream.iterate(0, value -> value + 1))
                        .fork(failureKey, stream -> {
                            throw new IllegalStateException("expected");
                        })
                        .run());
        assertEquals("Fork 'failure' failed", failure.getMessage());
        assertEquals(IllegalStateException.class, failure.getCause().getClass());
        assertEquals(IllegalStateException.class, failure.getCause().getClass());
    }

    @Test
    void rejectsDuplicateKeys() {
        var forker = StreamForker.from(Stream.of(1)).fork(COUNT, Stream::count);

        assertThrows(IllegalArgumentException.class, () -> forker.fork(COUNT, Stream::count));
    }
}
