import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Helpers for generating random values.
 */
public class Arbitrary {
  static <T> T element(Random random, List<T> elements) {
    return elements.get(random.nextInt(0, elements.size()));
  }

  static <T> Supplier<T> odds(Random random,
      ArrayList<? extends WithOdds<? extends Supplier<? extends T>>> arbs) {
    final int oddsTotal = arbs.stream().mapToInt(a -> a.odds()).sum();
    return (() -> {
      var pick = random.nextInt(0, oddsTotal) + 1;
      var pos = 0;
      for (var arb : arbs) {
        pos += arb.odds();
        if (pos >= pick) {
          return arb.value().get();
        }
      }
      throw new IllegalStateException("couldn't pick an arbitrary (shouldn't happen)");
    });
  }
}


record WithOdds<T>(int odds, T value) {
  static <T> WithOdds<T> of(int odds, T value) {
    return new WithOdds<T>(odds, value);
  }
}
