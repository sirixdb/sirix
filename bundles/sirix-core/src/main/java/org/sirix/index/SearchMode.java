package org.sirix.index;

import javax.annotation.Nonnull;
import java.util.Comparator;

/**
 * The search mode in a datastructure.
 *
 * @author Johannes Lichtenberger
 *
 */
public enum SearchMode {
  /** Greater than the specified key. */
  GREATER {
    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, @Nonnull K secondKey) {
      return secondKey.compareTo(firstKey) > 0 ? 0 : -1;
    }

    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, K secondKey,
        Comparator<? super K> comparator) {
      return comparator.compare(secondKey, firstKey) > 0 ? 0 : -1;
    }
  },

  /** Less than the specified key. */
  LESS {
    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, @Nonnull K secondKey) {
      return secondKey.compareTo(firstKey) < 0 ? 0 : -1;
    }

    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, K secondKey,
        Comparator<? super K> comparator) {
      return comparator.compare(secondKey, firstKey) < 0 ? 0 : -1;
    }
  },

  /** Greater or equal than the specified key. */
  GREATER_OR_EQUAL {
    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, @Nonnull K secondKey) {
      return secondKey.compareTo(firstKey) >= 0 ? 0 : -1;
    }

    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, K secondKey,
        Comparator<? super K> comparator) {
      return comparator.compare(secondKey, firstKey) >= 0 ? 0 : -1;
    }
  },

  /** Less or equal than the specified key. */
  LESS_OR_EQUAL {
    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, @Nonnull K secondKey) {
      return secondKey.compareTo(firstKey) <= 0 ? 0 : -1;
    }

    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, K secondKey,
        Comparator<? super K> comparator) {
      return comparator.compare(secondKey, firstKey) <= 0 ? 0 : -1;
    }
  },

  /** Equal to the specified key. */
  EQUAL {
    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, @Nonnull K secondKey) {
      return firstKey.compareTo(secondKey);
    }

    @Override
    public <K extends Comparable<? super K>> int compare(K firstKey, K secondKey,
        Comparator<? super K> comparator) {
      return comparator.compare(firstKey, secondKey);
    }
  };

  /**
   * Compare two keys.
   *
   * @param firstKey the search key
   * @param secondKey the potential result key
   * @return {@code 0} if it's in the search space, else {@code -1} or {@code +1}
   */
  public abstract <K extends Comparable<? super K>> int compare(final K firstKey,
      final K secondKey);

  public abstract <K extends Comparable<? super K>> int compare(final K firstKey, final K secondKey,
      final Comparator<? super K> comparator);
}
