package dev.dead;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

/**
 * Listing C.6: four operations, one traversal of the menu.
 */
public class Demo {
    static void main(String[] args) {
        Stream<Dish> menuStream = Dish.MENU.stream();

        StreamForker.Results results = new StreamForker<Dish>(menuStream)
                .fork("shortMenu", s -> s.map(Dish::getName).collect(joining(", ")))
                .fork("totalCalories", s -> s.mapToInt(Dish::getCalories).sum())
                .fork("mostCaloricDish", s -> s.reduce((d1, d2) -> d1.getCalories() > d2.getCalories() ? d1 : d2).get())
                .fork("dishesByType", s -> s.collect(groupingBy(Dish::getType)))
                .getResults();                                   // returns once the source is fully pushed

        // get(...) blocks until that particular operation has finished
        String shortMenu = results.get("shortMenu");
        int totalCalories = results.get("totalCalories");
        Dish mostCaloricDish = results.get("mostCaloricDish");
        Map<Dish.Type, List<Dish>> byType = results.get("dishesByType");

        System.out.println("Short menu: " + shortMenu);
        System.out.println("Total calories: " + totalCalories);
        System.out.println("Most caloric dish: " + mostCaloricDish);
        System.out.println("Dishes by type: " + byType);
    }
}