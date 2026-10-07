package dev.dead.collector;

import dev.dead.common.Key;
import dev.dead.common.Results;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.summingLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MultiCollectorTest {

    private static final Key<Long> SUM = Key.of("sum");
    private static final Key<Long> COUNT = Key.of("count");

    @Test
    void collectsMultipleResultsInOnePass() {
        Results results = IntStream.rangeClosed(1, 100)
                .boxed()
                .collect(new MultiCollector<Integer>()
                        .add(SUM, summingLong(Integer::longValue))
                        .add(COUNT, counting())
                        .build());

        assertEquals(5050L, results.get(SUM));
        assertEquals(100L, results.get(COUNT));
    }

    @Test
    void combinesPartialResultsForParallelStreams() {
        Results results = IntStream.range(0, 100_000)
                .parallel()
                .boxed()
                .collect(new MultiCollector<Integer>()
                        .add(SUM, summingLong(Integer::longValue))
                        .add(COUNT, counting())
                        .build());

        assertEquals(4_999_950_000L, results.get(SUM));
        assertEquals(100_000L, results.get(COUNT));
    }

    @Test
    void rejectsDuplicateKeys() {
        var collector = new MultiCollector<Integer>().add(SUM, summingLong(Integer::longValue));

        assertThrows(IllegalArgumentException.class, () -> collector.add(SUM, counting()));
    }

    @Test
    void rejectsUnknownResultKeys() {
        Results results = IntStream.empty().boxed()
                .collect(new MultiCollector<Integer>().add(SUM, summingLong(Integer::longValue)).build());

        assertThrows(java.util.NoSuchElementException.class, () -> results.get(COUNT));
    }
}
