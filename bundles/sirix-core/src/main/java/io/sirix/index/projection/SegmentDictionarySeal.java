/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.NamePage;
import it.unimi.dsi.fastutil.ints.IntArrays;

import static java.util.Objects.requireNonNull;

/**
 * Seals ONE {@code (segment, column)} dictionary: the values a segment minted, written in collation
 * order with a rank table from the ids the pages carry to the positions the storage holds.
 *
 * <p>
 * This is {@code docs/SEGMENT_DICTIONARY_DESIGN.md} §6, the rank pass per segment. The pages of a
 * segment were encoded before its value set was known, so their ids are MINTS — arrival order,
 * dense from 1 ({@link SegmentScopedDictionaries}). Serving wants collation order: the front-coded
 * value blocks only pay for sorted neighbours, the separator array only exists over a sorted run,
 * and a range predicate is a position range only in sorted storage. The seal reconciles the two by
 * sorting once and persisting the permutation ({@link GlobalValueDictionary#attachRankTable}), so
 * the pages keep what they wrote and the storage is what serving wants. When the mints already ARE
 * the ranks the table is skipped: ids are then positions, which is strictly cheaper to serve, and
 * the header says so by carrying no table.
 * </p>
 *
 * <h2>The order of the four writes is load-bearing</h2>
 *
 * <ol>
 * <li>the values, in rank order, as chained rank-ordered generations of
 * {@link GlobalValueDictionaryWriter#MAX_DISTINCT_ENTRIES_PER_APPEND} — the interner's safe size,
 * so a slot with more distinct values than that spans several generations whose ordered prefix
 * keeps extending ({@code ordered = rankOrdered && base.isFullyOrdered()});</li>
 * <li>the separator array ({@link GlobalValueDictionary#buildBlockIndex}), which reads values by
 * STORAGE position through the id route and therefore must run while ids still are positions — it
 * refuses to run after the table exists;</li>
 * <li>the rank table, unless the permutation is the identity;</li>
 * <li>the directory entry, by the caller, which also frees the mint map.</li>
 * </ol>
 *
 * <h2>What is checked, and why per value</h2>
 *
 * The sorted stream must be STRICTLY ascending: an equal neighbour means the mint map issued two
 * ids for one value, and the dictionary would then hold a value twice under two positions, one of
 * which no probe can ever find. The interner's answer is checked against the expected local rank on
 * every value for the same reason — a duplicate comes back as its earlier id without raising the
 * count. Both refusals happen before the first record of the affected generation is written (the
 * check runs as the values are interned, the flush comes after), so a refused seal leaves no
 * half-written run.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentDictionarySeal {

  /** Header key of a seal that wrote nothing: the slot minted no value in this segment. */
  public static final long NO_HEADER_KEY = 0L;

  /** Values per chained generation: the interner's safe per-append size. */
  static final int GENERATION_ENTRIES = GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND;

  private SegmentDictionarySeal() {
    throw new AssertionError("no instances");
  }

  /**
   * The outcome of one seal: where the dictionary was written and how many mints it covers.
   *
   * @param headerKey the dictionary header's record key; {@link #NO_HEADER_KEY} when nothing was
   *        written
   * @param entryCount how many values the dictionary holds, {@code == valuesById.length}
   * @param rankTableKey the rank table's first record key, {@code 0} when the mints were already
   *        ranks and no table was needed
   */
  public record Sealed(long headerKey, int entryCount, long rankTableKey) {
    public static final Sealed NOTHING = new Sealed(NO_HEADER_KEY, 0, 0L);

    public boolean wroteNothing() {
      return headerKey == NO_HEADER_KEY;
    }
  }

  /**
   * Rank the mints: {@code mintsByRank[r - 1]} is the mint whose value sorts at position {@code r}.
   * Package-private so the sort and its strictness check are testable without a storage writer.
   *
   * @param valuesById the segment's values in MINT order, index {@code i} holding mint {@code i + 1};
   *        no entry may be null
   * @return the mints in collation order of their values
   * @throws IllegalStateException on a null entry or two mints with equal values
   */
  static int[] rankMints(final byte[][] valuesById) {
    final int count = valuesById.length;
    final int[] mintsByRank = new int[count];
    for (int i = 0; i < count; i++) {
      if (valuesById[i] == null) {
        throw new IllegalStateException(
            "mint " + (i + 1) + " has no value: the segment was read before every page of" + " it had been encoded");
      }
      mintsByRank[i] = i + 1;
    }
    IntArrays.quickSort(mintsByRank, (left, right) -> {
      final byte[] leftValue = valuesById[left - 1];
      final byte[] rightValue = valuesById[right - 1];
      return ValueDictionaryEntryNode.compareUtf16Range(leftValue, 0, leftValue.length, rightValue, 0,
          rightValue.length);
    });
    for (int rank = 1; rank < count; rank++) {
      final byte[] previous = valuesById[mintsByRank[rank - 1] - 1];
      final byte[] next = valuesById[mintsByRank[rank] - 1];
      if (ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, next, 0, next.length) >= 0) {
        final int lower = Math.min(mintsByRank[rank - 1], mintsByRank[rank]);
        final int higher = Math.max(mintsByRank[rank - 1], mintsByRank[rank]);
        throw new IllegalStateException("mints " + lower + " and " + higher
            + " hold the same value: the segment dictionary issued two ids for one value");
      }
    }
    return mintsByRank;
  }

  /**
   * The inverse permutation in {@link GlobalValueDictionary#attachRankTable}'s contract:
   * {@code rankByMint[m]} is the position of mint {@code m}, index 0 unused.
   */
  static int[] invert(final int[] mintsByRank) {
    final int[] rankByMint = new int[mintsByRank.length + 1];
    for (int rank = 1; rank <= mintsByRank.length; rank++) {
      rankByMint[mintsByRank[rank - 1]] = rank;
    }
    return rankByMint;
  }

  /** Whether every mint is its own rank, in which case no table is written. */
  static boolean isIdentity(final int[] mintsByRank) {
    for (int rank = 1; rank <= mintsByRank.length; rank++) {
      if (mintsByRank[rank - 1] != rank) {
        return false;
      }
    }
    return true;
  }

  /**
   * Seal one {@code (segment, column)} dictionary through the load's own writer, mid-load, with no
   * commit of its own.
   *
   * @param storageEngineWriter the transaction's writer; the records go through its intent log
   * @param column the projection column, for messages
   * @param valuesById the segment's values in MINT order —
   *        {@link SegmentScopedDictionaries#valuesById}, read before the segment is released
   * @return where the dictionary was written; {@link Sealed#NOTHING} when {@code valuesById} is empty
   * @throws IllegalStateException if the values are not distinct or a generation's interner disagrees
   *         with the expected rank
   */
  public static Sealed write(final StorageEngineWriter storageEngineWriter, final int column,
      final byte[][] valuesById) {
    requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    requireNonNull(valuesById, "valuesById must not be null");
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    final int count = valuesById.length;
    if (count == 0) {
      return Sealed.NOTHING;
    }
    final int[] mintsByRank = rankMints(valuesById);
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(storageEngineWriter);
    final TransactionIntentLog log = storageEngineWriter.getLog();

    long headerKey = NO_HEADER_KEY;
    for (int from = 0; from < count; from += GENERATION_ENTRIES) {
      final int to = Math.min(count, from + GENERATION_ENTRIES);
      final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter();
      try {
        // Declared before the first value, where the writer enforces it: no forward hash index, and
        // the header records the run as ordered -- which the strictness check above made true.
        generation.markRankOrdered();
        for (int rank = from; rank < to; rank++) {
          final byte[] value = valuesById[mintsByRank[rank] - 1];
          final int local = generation.intern(value, 0, value.length);
          final int expected = rank - from + 1;
          if (local != expected) {
            throw new IllegalStateException("segment dictionary column " + column + " generation interner answered "
                + local + " for rank " + (rank + 1) + ", expected local rank " + expected);
          }
        }
        if (headerKey == NO_HEADER_KEY) {
          headerKey = generation.flush(namePage, databaseType, storageEngineWriter, log);
        } else {
          final ValueDictionaryHeaderNode base = GlobalValueDictionary.header(headerKey, storageEngineWriter);
          if (base == null || !base.isFullyOrdered()) {
            throw new IllegalStateException(
                "segment dictionary column " + column + " cannot chain onto header " + headerKey + ": " + base);
          }
          generation.flushAppend(base, namePage, databaseType, storageEngineWriter, log);
        }
      } finally {
        generation.release();
      }
    }
    // Positions first: the separators are cut between storage positions and read through the id
    // route, which translates mints only once a table exists. buildBlockIndex refuses the other order.
    GlobalValueDictionary.buildBlockIndex(headerKey, namePage, databaseType, storageEngineWriter, log);
    final long rankTableKey = isIdentity(mintsByRank)
        ? 0L
        : GlobalValueDictionary.attachRankTable(headerKey, invert(mintsByRank), namePage, databaseType,
            storageEngineWriter, log);
    return new Sealed(headerKey, count, rankTableKey);
  }
}
