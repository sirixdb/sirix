/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.page.NamePage;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

/**
 * Writes ONE sealed segment's dictionary through the load's own writer, mid-load.
 *
 * <p>
 * The last primitive {@code docs/SEGMENT_SCOPED_DICTIONARIES.md} needs, and it cannot be
 * {@link PrePassDictionaryBuilder}: that one commits per generation, which is correct for a pre-pass
 * running before the shred and wrong for a seal happening inside a load that owns the transaction.
 * The shape here is the mid-load one {@code ProjectionIndexBuilder.flushStreamingDictionaryGeneration}
 * already uses — intern into a {@link GlobalValueDictionaryWriter}, flush it against the writer's
 * current name page, release — with no commit of its own.
 * </p>
 *
 * <h2>The id alignment, asserted rather than assumed</h2>
 *
 * A page recorded ids that {@link SegmentScopedDictionaries} minted; this writes a dictionary whose
 * ids {@link GlobalValueDictionaryWriter#intern} mints. They agree because both are 1-based, assigned
 * in insertion order, one per distinct value ({@code nextId() == entryCount + 1}) — but they are two
 * independently written pieces of code, and if they ever disagree every page of the segment resolves
 * to the WRONG value with no exception anywhere. So the agreement is checked on every value, and a
 * divergence fails the load rather than producing a dictionary that reads plausibly and wrongly.
 *
 * <h2>Why a forward index is written</h2>
 *
 * These ids are arrival-ordered, so {@code orderedPrefixCount} is 0, and
 * {@code ValueDictionaryHeaderNode} permits a zero forward root only on a FULLY ordered dictionary.
 * The forward radix is therefore written even though nothing probes value→id after a seal — a
 * structural requirement of the format, not of the read path, priced at ~65 B/entry (≈ 15 MB for a
 * 231k-entry segment). Relaxing that invariant for a decode-only dictionary is a separate, deliberate
 * change.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionaryFlusher {

  private SegmentDictionaryFlusher() {
    throw new AssertionError("no instances");
  }

  /**
   * Intern the segment's values in id order, checking each against the id the segment assigned it.
   * Package-private so the ALIGNMENT GUARD — the part that can silently mis-resolve a whole segment —
   * is testable without a storage writer.
   *
   * @return how many values were interned
   */
  static int internInIdOrder(final GlobalValueDictionaryWriter generation, final int column,
      final Iterator<byte[]> values) {
    int expected = 0;
    while (values.hasNext()) {
      final byte[] value = values.next();
      if (value == null) {
        throw new IllegalStateException("segment dictionary column " + column + " has a hole at id " + (expected + 1)
            + "; the segment was read before every page of it had been encoded");
      }
      expected++;
      final int assigned = generation.intern(value, 0, value.length);
      if (assigned != expected) {
        // Two id spaces that must agree: the one the pages recorded and the one this writer mints. A
        // divergence would make every page of the segment resolve to a plausible WRONG value,
        // silently, which is why it is checked per value rather than at the end.
        throw new IllegalStateException("segment dictionary column " + column + " id divergence: the segment "
            + "assigned id " + expected + " but the dictionary writer assigned " + assigned
            + " (a duplicate value in the segment's id order, or a different insertion order)");
      }
    }
    return expected;
  }

  /**
   * Intern {@code values} in the order they are given and write them as a fresh dictionary.
   *
   * @param values the segment's distinct values in ID ORDER — {@link SegmentScopedDictionaries#valuesOf}
   * @param budgetBytes the writer's admission budget for this segment's values
   * @return the header key the sealed dictionary was written under, for {@link SegmentDictionaryAnchors}
   * @throws IllegalStateException if an interned value does not take the id the segment assigned it
   */
  public static long write(final StorageEngineWriter storageEngineWriter, final int column,
      final Iterator<byte[]> values, final long budgetBytes) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    requireNonNull(values, "values must not be null");
    final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(column, budgetBytes);
    try {
      final int expected = internInIdOrder(generation, column, values);
      if (expected == 0) {
        return SegmentDictionaryAnchors.NO_HEADER_KEY;
      }
      final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
      final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(storageEngineWriter);
      return generation.flush(namePage, databaseType, storageEngineWriter, storageEngineWriter.getLog());
    } finally {
      generation.release();
    }
  }
}
