package dev.dead;

import dev.dead.book.StreamForker;
import dev.dead.collector.MultiCollector;
import dev.dead.common.Dish;
import dev.dead.common.Key;
import dev.dead.common.Results;
import dev.dead.reactor.ReactiveStreamForker;
import reactor.core.publisher.Flux;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.*;

/**
 * The same four operations on the same menu, computed with each method.
 */
public class Demo {

    // Typed keys shared by the collector and modern implementations.
    static final Key<String> NAMES = Key.of("shortMenu");
    static final Key<Integer> CALORIES = Key.of("totalCalories");
    static final Key<Optional<Dish>> TOP = Key.of("mostCaloricDish");
    static final Key<Map<Dish.Type, List<Dish>>> BY_TYPE = Key.of("dishesByType");

    public static void main(String[] args) {
        List<Dish> menu = Dish.MENU;
        Comparator<Dish> byCalories = comparingInt(Dish::calories);

        // ---- Method 1: the book's StreamForker (Object keys, unchecked get) --------------------
        StreamForker.Results bookResults = new StreamForker<Dish>(menu.stream())
                .fork("shortMenu", s -> s.map(Dish::name).collect(joining(", ")))
                .fork("totalCalories", s -> s.mapToInt(Dish::calories).sum())
                .fork("mostCaloricDish", s -> s.max(byCalories).orElseThrow())
                .fork("dishesByType", s -> s.collect(groupingBy(Dish::type)))
                .getResults();
        Summary book = new Summary(bookResults.get("shortMenu"), bookResults.get("totalCalories"),
                bookResults.get("mostCaloricDish"), bookResults.get("dishesByType"));

        // ---- Method 2: MultiCollector (one pass, calling thread, typed keys) --------------------
        Results collected = menu.stream().collect(new MultiCollector<Dish>()
                .add(NAMES, mapping(Dish::name, joining(", ")))
                .add(CALORIES, summingInt(Dish::calories))
                .add(TOP, maxBy(byCalories))
                .add(BY_TYPE, groupingBy(Dish::type))
                .build());
        Summary multiCollector = new Summary(collected.get(NAMES), collected.get(CALORIES),
                collected.get(TOP).orElseThrow(), collected.get(BY_TYPE));

        // ---- Method 2b: the JDK built-in, nested Collectors.teeing -----------------------------
        Summary teeing = menu.stream().collect(teeing(
                teeing(mapping(Dish::name, joining(", ")), summingInt(Dish::calories), Map::entry),
                teeing(maxBy(byCalories), groupingBy(Dish::type), Map::entry),
                (namesAndCalories, topAndTypes) -> new Summary(
                        namesAndCalories.getKey(), namesAndCalories.getValue(),
                        topAndTypes.getKey().orElseThrow(), topAndTypes.getValue())));

        // ---- Method 3: modern StreamForker (concurrent, bounded, typed) -------------------------
        Results modernResults = dev.dead.modern.StreamForker.from(menu.stream())
                .fork(NAMES, s -> s.map(Dish::name).collect(joining(", ")))
                .fork(CALORIES, s -> s.mapToInt(Dish::calories).sum())
                .fork(TOP, s -> s.max(byCalories))
                .fork(BY_TYPE, s -> s.collect(groupingBy(Dish::type)))
                .run();
        Summary modern = new Summary(modernResults.get(NAMES), modernResults.get(CALORIES),
                modernResults.get(TOP).orElseThrow(), modernResults.get(BY_TYPE));

        // ---- Method 4: Reactor publish(Function) multicast (reactive, typed) --------------------
        Results reactorResults = ReactiveStreamForker.from(Flux.fromIterable(menu))
                .fork(NAMES, s -> s.map(Dish::name).collectList()
                        .map(names -> String.join(", ", names)))
                .fork(CALORIES, s -> s.map(Dish::calories).reduce(0, Integer::sum))
                .fork(TOP, s -> s.reduce((first, second) ->
                                byCalories.compare(first, second) >= 0 ? first : second)
                        .map(Optional::of).defaultIfEmpty(Optional.empty()))
                .fork(BY_TYPE, s -> s.collect(groupingBy(Dish::type)))
                .run()
                .block();
        Summary reactor = new Summary(reactorResults.get(NAMES), reactorResults.get(CALORIES),
                reactorResults.get(TOP).orElseThrow(), reactorResults.get(BY_TYPE));

        System.out.println("Short menu:        " + book.names());
        System.out.println("Total calories:    " + book.totalCalories());
        System.out.println("Most caloric dish: " + book.mostCaloric().name());
        System.out.println("Dishes by type:    " + book.byType());
        System.out.println();
        System.out.println("method 1 (book)          == method 2 (MultiCollector): " + book.equals(multiCollector));
        System.out.println("method 2b (teeing)       == method 2 (MultiCollector): " + teeing.equals(multiCollector));
        System.out.println("method 3 (modern forker) == method 2 (MultiCollector): " + modern.equals(multiCollector));
        System.out.println("method 4 (Reactor)       == method 2 (MultiCollector): " + reactor.equals(multiCollector));
    }

    /**
     * Common result shape, so the methods can be compared with equals().
     */
    record Summary(String names, int totalCalories, Dish mostCaloric,
                   Map<Dish.Type, List<Dish>> byType) {
    }
}
