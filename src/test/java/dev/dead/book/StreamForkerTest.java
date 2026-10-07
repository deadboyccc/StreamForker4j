package dev.dead.book;

import dev.dead.common.Dish;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamForkerTest {

    @Test
    void computesSeveralResultsFromOneTraversal() {
        var results = new StreamForker<>(Dish.MENU.stream())
                .fork("names", stream -> stream.map(Dish::name).collect(joining(", ")))
                .fork("calories", stream -> stream.mapToInt(Dish::calories).sum())
                .fork("byType", stream -> stream.collect(groupingBy(Dish::type)))
                .getResults();

        assertEquals("pork, beef, chicken, french fries, rice, season fruit, pizza, prawns, salmon",
                results.get("names"));
        assertEquals(4200, (int) results.get("calories"));
        assertEquals(Map.of(
                Dish.Type.MEAT, List.of(Dish.MENU.get(0), Dish.MENU.get(1), Dish.MENU.get(2)),
                Dish.Type.OTHER, List.of(Dish.MENU.get(3), Dish.MENU.get(4), Dish.MENU.get(5), Dish.MENU.get(6)),
                Dish.Type.FISH, List.of(Dish.MENU.get(7), Dish.MENU.get(8))), results.get("byType"));
    }

    @Test
    void wrapsOperationFailuresWhenReadingResult() {
        var results = new StreamForker<Integer>(java.util.stream.Stream.of(1, 2, 3))
                .fork("failure", stream -> stream.map(value -> {
                    throw new IllegalStateException("expected");
                }).count())
                .getResults();

        var failure = assertThrows(RuntimeException.class, () -> results.get("failure"));
        assertEquals(java.util.concurrent.ExecutionException.class, failure.getCause().getClass());
        assertEquals(IllegalStateException.class, failure.getCause().getCause().getClass());
    }
}
