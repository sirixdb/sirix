package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.page.NamePage;

/**
 * Appends a rank-ordered value stream into one resource-wide dictionary, one bounded generation at
 * a time, holding NO probe front.
 *
 * <p>
 * This exists because the obvious reuse is wrong. The streaming path's
 * {@code flushStreamingDictionaryGeneration} promotes its writer into a {@code
 * StreamingGlobalDictionary} on first use, and that constructor builds a resident probe front and
 * seeds it with every entry — which would reintroduce, entry by entry, the
 * {@code D × (22 + avgLen)} structure the rank pass exists to remove (5.86 GB for the four
 * ClickBench string columns). A merged sorted stream emits each distinct value exactly ONCE by
 * construction, so there is nothing to probe against: the pass does not merely bound that
 * structure, it has no use for it.
 * </p>
 *
 * <p>
 * <b>Why it commits per generation.</b> Every {@code put} stages a dirty page in the transaction's
 * intent log, which is not released until commit; over a whole column that would be the entire
 * rewritten dictionary held at once. A generation is already the boundary {@code flushAppend}
 * defines — each is an immutable segment — so committing there is free of extra machinery, and a
 * crash between generations leaves a shorter but internally consistent dictionary whose
 * {@code orderedPrefixCount} still describes exactly the run that was written.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class RankPassDictionaryAppender {

  /** Runs after each generation is flushed, so the caller owns the commit policy. */
  @FunctionalInterface
  interface GenerationBoundary {
    /**
     * @param generation how many generations have been flushed so far
     * @return the writer to use from now on, which a committing implementation must refresh
     */
    StorageEngineWriter onGenerationFlushed(int generation);
  }

  private final int column;

  private final DatabaseType databaseType;

  private final GenerationBoundary boundary;

  private StorageEngineWriter storageEngineWriter;

  private GlobalValueDictionaryWriter generation;

  private long headerKey;

  private int generations;

  private int entryCount;

  /** Reused across the whole stream: one copy per DISTINCT value, never a fresh array per value. */
  private byte[] previousValue = new byte[256];

  private int previousLength = -1;

  RankPassDictionaryAppender(final int column, final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final GenerationBoundary boundary) {
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    this.column = column;
    this.databaseType = java.util.Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.storageEngineWriter = java.util.Objects.requireNonNull(storageEngineWriter, "writer must not be null");
    this.boundary = java.util.Objects.requireNonNull(boundary, "boundary must not be null");
    this.generation = newGeneration();
  }

  /**
   * Accepts the next value of a STRICTLY ASCENDING stream and returns the rank it was minted as.
   *
   * <p>
   * The ascending precondition is CHECKED rather than assumed: it is the single property the whole
   * design rests on — ids that sort like their values — and a merge defect that emitted two values
   * out of order would otherwise produce a dictionary whose header claims an order it does not have,
   * which every ordering arm downstream would then trust.
   * </p>
   *
   * @param value the value's UTF-8 bytes
   * @param offset start of the value
   * @param length length of the value
   * @return the rank, counting from 1
   */
  int accept(final byte[] value, final int offset, final int length) {
    if (previousLength >= 0
        && ValueDictionaryEntryNode.compareUtf16Range(previousValue, 0, previousLength, value, offset, length) >= 0) {
      throw new IllegalStateException(
          "rank pass received a value that does not follow its predecessor in collation order at rank "
              + (entryCount + 1));
    }
    if (previousValue.length < length) {
      previousValue = new byte[Math.max(length, previousValue.length * 2)];
    }
    System.arraycopy(value, offset, previousValue, 0, length);
    previousLength = length;

    final int localId = generation.intern(value, offset, length);
    // A duplicate would come back as its earlier id without raising the count, which is the one way
    // a merge defect could silently collapse two ranks into one.
    if (localId != generation.entryCount()) {
      throw new IllegalStateException("the value stream is not distinct: intern answered " + localId
          + " while the generation holds " + generation.entryCount());
    }
    entryCount++;
    if (generation.entryCount() == GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND) {
      rotate();
    }
    return entryCount;
  }

  /** Flushes whatever the open generation holds. Idempotent once the stream is exhausted. */
  void finish() {
    if (generation != null && generation.entryCount() > 0) {
      rotate();
    }
    if (generation != null) {
      generation.release();
      generation = null;
    }
  }

  long headerKey() {
    return headerKey;
  }

  int entryCount() {
    return entryCount;
  }

  int generations() {
    return generations;
  }

  private void rotate() {
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    final TransactionIntentLog log = storageEngineWriter.getLog();
    if (headerKey == 0L) {
      headerKey = generation.flush(namePage, databaseType, storageEngineWriter, log);
    } else {
      final ValueDictionaryHeaderNode base = GlobalValueDictionary.header(headerKey, storageEngineWriter);
      if (base == null) {
        throw new IllegalStateException("the dictionary header vanished between generations");
      }
      headerKey = generation.flushAppend(base, namePage, databaseType, storageEngineWriter, log);
    }
    generation.release();
    generations++;
    storageEngineWriter = boundary.onGenerationFlushed(generations);
    generation = newGeneration();
  }

  private GlobalValueDictionaryWriter newGeneration() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
    // Declared BEFORE the first value, which is also where the writer enforces it: the claim
    // suppresses the forward hash index and records the ordered boundary, and it would be a lie if
    // anything had already been interned in another order.
    writer.markRankOrdered();
    return writer;
  }

  int column() {
    return column;
  }
}
