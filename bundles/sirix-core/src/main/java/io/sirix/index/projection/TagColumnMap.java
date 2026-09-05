/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Builds the tag-to-column map a string region's dictionary ids are resolved through — ONE conflict
 * rule, shared by the writer that stamps ids on a page and the reader that has to resolve them again.
 *
 * <h2>Why the rule has to be shared</h2>
 *
 * A string-region tag is a path class: a path node key that fits an int. A projection column claims
 * every path class its field path resolves to, and two columns whose field paths overlap (a
 * descendant step, a repeated field) claim the same tag. There is no right column for such a tag: an
 * id minted into one column's dictionary and resolved against the other's reads back a value that
 * is plausible and wrong. The writer and the reader used to decide this independently — the writer
 * kept the LAST claim, the reader dropped the tag — so a page the writer had encoded with ids was a
 * page the reader could not resolve at all.
 *
 * <p>
 * Here a CONTESTED tag is withheld from the map, permanently: a third claim cannot restore it, and a
 * rebuilt map (the writer rebuilds from every claim so far on each refresh) reaches the same verdict
 * because both claims are still among its inputs. Both sides feed the same claims — each field
 * path's path classes, per column, from the path summary — so they build the same map, and both
 * leave a contested tag's values as bytes. A tag can only ever be contested from its first
 * appearance: a path node matches a field path or it does not, decided when the node is created, so
 * no uncontested tag turns contested under pages already encoded against its column.
 * </p>
 *
 * <p>
 * Not thread-safe: build on one thread, publish the result of {@link #build()}.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class TagColumnMap {

  /** What {@link #build()}'s map answers for a tag it does not hold. Never a valid column. */
  public static final int NO_COLUMN = -1;

  /**
   * A tag claimed by two different columns; withheld from {@link #build()} for good. Deliberately the
   * same value as {@link #NO_COLUMN}: to every consumer of the built map the two mean one thing —
   * this tag has no column, so its values keep their bytes.
   */
  private static final int CONTESTED = NO_COLUMN;

  /** Answer for a tag nobody has claimed; distinct from every column and from {@link #CONTESTED}. */
  private static final int UNCLAIMED = -2;

  private final Int2IntOpenHashMap columnByTag;

  private int contestedCount;

  /** A builder expecting about {@code expectedTags} distinct tags. */
  public TagColumnMap(final int expectedTags) {
    if (expectedTags < 0) {
      throw new IllegalArgumentException("expectedTags must not be negative: " + expectedTags);
    }
    this.columnByTag = new Int2IntOpenHashMap(expectedTags);
    this.columnByTag.defaultReturnValue(UNCLAIMED);
  }

  /**
   * Record that {@code column} resolves path class {@code pathNodeKey}.
   *
   * @param pathNodeKey the path class; a key outside the int tag range is ignored, because no string
   *        region can carry it as a tag and so no page needs an answer for it
   * @param column the claiming column, at least 0
   * @return whether the key is a tag this map answers for (claimed or contested); {@code false} when
   *         it was ignored
   */
  public boolean claim(final long pathNodeKey, final int column) {
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    if (pathNodeKey <= 0L || pathNodeKey > Integer.MAX_VALUE) {
      return false;
    }
    final int tag = (int) pathNodeKey;
    final int previous = columnByTag.get(tag);
    if (previous == UNCLAIMED) {
      columnByTag.put(tag, column);
    } else if (previous != column && previous != CONTESTED) {
      columnByTag.put(tag, CONTESTED);
      contestedCount++;
    }
    return true;
  }

  /** Whether {@code tag} has been claimed by two different columns. */
  public boolean isContested(final int tag) {
    return columnByTag.get(tag) == CONTESTED;
  }

  /** Tags claimed by two different columns so far. */
  public int contestedCount() {
    return contestedCount;
  }

  /** Tags with exactly one claiming column so far — the size of {@link #build()}'s result. */
  public int resolvedCount() {
    return columnByTag.size() - contestedCount;
  }

  /**
   * Every uncontested tag mapped to its column, as an unmodifiable map that is safe to publish to
   * threads that only read it. A contested tag is absent, which every consumer reads as "this tag
   * keeps its bytes"; the map's default return value is {@link #NO_COLUMN}, so even a consumer that
   * skips {@code containsKey} cannot read column 0 for a tag that has none.
   */
  public Int2IntMap build() {
    final int resolved = resolvedCount();
    final Int2IntOpenHashMap out = new Int2IntOpenHashMap(resolved);
    out.defaultReturnValue(NO_COLUMN);
    if (resolved == 0) {
      return Int2IntMaps.unmodifiable(out);
    }
    for (final ObjectIterator<Int2IntMap.Entry> it = columnByTag.int2IntEntrySet().fastIterator(); it.hasNext();) {
      final Int2IntMap.Entry entry = it.next();
      final int column = entry.getIntValue();
      if (column != CONTESTED) {
        out.put(entry.getIntKey(), column);
      }
    }
    return Int2IntMaps.unmodifiable(out);
  }
}
