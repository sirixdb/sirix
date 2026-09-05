/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * The DECODE view a load needs over its own, still-unsealed segment dictionaries.
 *
 * <p>
 * A load commits more than once. Every commit before the last writes document pages whose string
 * values are already ids, while the dictionary those ids name is sealed only at the FINAL commit —
 * so between the two, a page the load itself wrote cannot be read back through the persisted
 * directory, because there is nothing there yet. The writer does read such pages back: a cursor
 * moving to a record whose page has left the intent log, a versioning combine over a page's
 * fragments. Refusing them would make a load unable to read what it just wrote.
 * </p>
 *
 * <p>
 * The values are in memory the whole time — the lane is holding them precisely so it can seal them —
 * so this view answers from there. It is the write side's twin of
 * {@link SegmentScopedReadDictionaries}, which answers the same questions from the persisted
 * directory once a revision is finished, and it is live for exactly as long as the lane is.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentLaneReadView implements GlobalStringDictionaries {

  private final SegmentScopedDictionaries dictionaries;

  public SegmentLaneReadView(final SegmentScopedDictionaries dictionaries) {
    this.dictionaries = requireNonNull(dictionaries, "dictionaries must not be null");
  }

  @Override
  public boolean hasDictionary(final int tag) {
    return dictionaries.tags().containsKey(tag);
  }

  @Override
  public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
    return columnOf(tag) >= 0 && segmentOf(dictionaryKey) >= 0
        && dictionaries.entryCount(segmentOf(dictionaryKey), columnOf(tag)) >= recordedEntryCount;
  }

  @Override
  public int idOf(final int tag, final byte[] value, final int offset, final int length) {
    // Decode direction only. Minting belongs to the page's own SegmentView, which is bound to the
    // page's segment; this view is bound to none and would have to guess one.
    return ID_ABSENT;
  }

  @Override
  public byte @Nullable [] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount,
      final int id) {
    final int column = columnOf(tag);
    final int segment = segmentOf(dictionaryKey);
    if (column < 0 || segment < 0 || id <= 0 || id > recordedEntryCount) {
      return null;
    }
    // The page's own recorded count still bounds the id, exactly as it does on the persisted route:
    // an id above what the page saw is one the page cannot have written.
    return dictionaries.valueAt(segment, column, id);
  }

  @Override
  public long dictionaryKey(final int tag) {
    return 0L; // a segment anchor belongs to a PAGE; this view never mints one
  }

  @Override
  public int dictionaryEntryCount(final int tag) {
    return 0; // likewise: the count that matters is the one the page recorded
  }

  private int columnOf(final int tag) {
    final Int2IntMap tags = dictionaries.tags();
    return tags.containsKey(tag)
        ? tags.get(tag)
        : -1;
  }

  /** The page's anchor is its segment plus one, because 0 is the "no dictionary" sentinel. */
  private static int segmentOf(final long dictionaryKey) {
    return dictionaryKey <= 0L || dictionaryKey > Integer.MAX_VALUE
        ? -1
        : (int) (dictionaryKey - 1);
  }
}
