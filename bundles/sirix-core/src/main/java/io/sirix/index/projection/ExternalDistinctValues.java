package io.sirix.index.projection;

import io.sirix.node.ValueDictionaryEntryNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import static java.util.Objects.requireNonNull;

/**
 * Collects one column's DISTINCT values under a fixed byte budget and hands them back ASCENDING in
 * the engine's collation — the corpus half of the dictionary pre-pass.
 *
 * <p>
 * A resource-wide rank-ordered dictionary is built by streaming each distinct value exactly once, in
 * order ({@link PrePassDictionaryBuilder}). Producing that stream is the half that cannot be done in
 * memory at scale: one ClickBench column's distinct set is 18.3 million values and 3.4 GB of bytes at
 * 100M rows, so a {@code List<byte[]>} of them — what the harness route required its caller to
 * supply — is not a candidate. This is the ordinary external answer: buffer into an arena, sort and
 * SPILL a deduplicated ascending run when the arena fills, then merge the runs.
 * </p>
 *
 * <h2>Why dedup happens in the sort, not on add</h2>
 *
 * A hash set keyed by value would deduplicate earlier and buffer more distinct values per byte of
 * budget, at the cost of one object (or one probe front) per distinct value — the very structure the
 * rank pass exists to remove. Appending blindly and deduplicating in the sort keeps the resident
 * cost to the arena bytes plus eight bytes of index per BUFFERED value, and duplicates that survive
 * into a run are dropped by the merge. The price is that a column whose values repeat heavily spills
 * more often than it strictly must; the output is identical either way.
 *
 * <h2>Ordering</h2>
 *
 * {@link ValueDictionaryEntryNode#compareUtf16Range} — the comparator the dictionary's own ordering
 * invariant is expressed in, so this class's output is accepted by the appender by construction
 * rather than by coincidence.
 *
 * <p>
 * NOT thread-safe: one collector per column per pass.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ExternalDistinctValues implements AutoCloseable {

  /** Arena bytes a collector buffers before it spills, when the caller names no budget. */
  public static final long DEFAULT_BUDGET_BYTES = 64L << 20;

  /** Smallest budget worth running: below this the run count grows faster than the merge tolerates. */
  public static final long MIN_BUDGET_BYTES = 1 << 12;

  /** Values below this length are sorted by insertion rather than by partitioning. */
  private static final int INSERTION_SORT_THRESHOLD = 24;

  private final long budgetBytes;

  private final Path spillDirectory;

  /** Spilled runs, each ascending and internally deduplicated. */
  private final List<Path> runs = new ArrayList<>();

  private byte[] arena;

  /** Bytes used in {@link #arena}. */
  private int arenaUsed;

  /** Parallel to {@link #lengths}: where each buffered value starts in the arena. */
  private int[] offsets = new int[1024];

  private int[] lengths = new int[1024];

  private int count;

  /** Values appended over the collector's life, duplicates included. */
  private long appended;

  /** Distinct values yielded so far by {@link #ascending()}; exact once it is exhausted. */
  private long distinct;

  private boolean drained;

  /**
   * @param spillDirectory directory for spilled runs; created on the first spill, and every run this
   *        collector writes is deleted by {@link #close()}
   * @param budgetBytes arena bytes buffered before a spill, at least {@value #MIN_BUDGET_BYTES}
   */
  public ExternalDistinctValues(final Path spillDirectory, final long budgetBytes) {
    this.spillDirectory = requireNonNull(spillDirectory, "spillDirectory must not be null");
    if (budgetBytes < MIN_BUDGET_BYTES) {
      throw new IllegalArgumentException("budgetBytes must be at least " + MIN_BUDGET_BYTES + ": " + budgetBytes);
    }
    this.budgetBytes = budgetBytes;
    this.arena = new byte[(int) Math.min(budgetBytes, 1 << 16)];
  }

  /** A collector at {@link #DEFAULT_BUDGET_BYTES}. */
  public ExternalDistinctValues(final Path spillDirectory) {
    this(spillDirectory, DEFAULT_BUDGET_BYTES);
  }

  /**
   * Append one value. Duplicates are accepted and removed later; the caller need not know whether it
   * has seen the value before, which is the point of the class.
   *
   * @param value backing array, not retained
   * @param offset first byte of the value
   * @param length value length in bytes
   */
  public void add(final byte[] value, final int offset, final int length) {
    requireNonNull(value, "value must not be null");
    if (offset < 0 || length < 0 || offset > value.length - length) {
      throw new IndexOutOfBoundsException("offset " + offset + " length " + length + " over " + value.length);
    }
    if (drained) {
      throw new IllegalStateException("values were already drained by ascending()");
    }
    if (count > 0 && (long) arenaUsed + length > budgetBytes) {
      spill();
    }
    if (count == offsets.length) {
      offsets = grow(offsets, count);
      lengths = grow(lengths, count);
    }
    if (arenaUsed + length > arena.length) {
      growArena(arenaUsed + length);
    }
    System.arraycopy(value, offset, arena, arenaUsed, length);
    offsets[count] = arenaUsed;
    lengths[count] = length;
    count++;
    arenaUsed += length;
    appended++;
  }

  private static int[] grow(final int[] array, final int used) {
    final int[] grown = new int[Math.max(16, array.length << 1)];
    System.arraycopy(array, 0, grown, 0, used);
    return grown;
  }

  private void growArena(final int needed) {
    int capacity = Math.max(arena.length, 1 << 12);
    while (capacity < needed) {
      capacity = capacity > (Integer.MAX_VALUE - 8) >> 1
          ? Integer.MAX_VALUE - 8
          : capacity << 1;
    }
    final byte[] grown = new byte[capacity];
    System.arraycopy(arena, 0, grown, 0, arenaUsed);
    arena = grown;
  }

  /** Values appended, duplicates included. */
  public long appended() {
    return appended;
  }

  /** Runs spilled so far — zero means everything fitted the budget (test observability). */
  public int spilledRuns() {
    return runs.size();
  }

  /** Distinct values yielded so far; the column's exact distinct count once the iterator is spent. */
  public long distinct() {
    return distinct;
  }

  /**
   * The distinct values, ASCENDING under {@link ValueDictionaryEntryNode#compareUtf16Range}. Drains
   * the collector: it accepts no further {@link #add}, and the iterator must be consumed before
   * {@link #close()} deletes the runs it reads.
   *
   * <p>
   * Each value is a freshly allocated array, so the caller may retain it; nothing else this class
   * holds survives the iteration.
   * </p>
   */
  public Iterator<byte[]> ascending() {
    if (drained) {
      throw new IllegalStateException("ascending() was already called");
    }
    drained = true;
    if (runs.isEmpty()) {
      sortBuffered();
      return new CountingIterator(new BufferedIterator());
    }
    if (count > 0) {
      spill();
    }
    return new CountingIterator(new MergingIterator(runs));
  }

  /** Sort, deduplicate and write the buffered values as one ascending run; empties the arena. */
  private void spill() {
    sortBuffered();
    final Path run;
    try {
      Files.createDirectories(spillDirectory);
      run = Files.createTempFile(spillDirectory, "distinct-", ".run");
      try (DataOutputStream out = new DataOutputStream(
          new BufferedOutputStream(Files.newOutputStream(run), 1 << 20))) {
        for (int i = 0; i < count; i++) {
          if (i > 0 && equalAt(i - 1, i)) {
            continue;
          }
          out.writeInt(lengths[i]);
          out.write(arena, offsets[i], lengths[i]);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("could not spill a distinct-value run", e);
    }
    runs.add(run);
    arenaUsed = 0;
    count = 0;
  }

  private boolean equalAt(final int left, final int right) {
    return ValueDictionaryEntryNode.compareUtf16Range(arena, offsets[left], lengths[left], arena, offsets[right],
        lengths[right]) == 0;
  }

  private int compareAt(final int left, final int right) {
    return ValueDictionaryEntryNode.compareUtf16Range(arena, offsets[left], lengths[left], arena, offsets[right],
        lengths[right]);
  }

  /** Sorts the buffered (offset, length) pairs in place — no boxing, no per-value object. */
  private void sortBuffered() {
    quickSort(0, count - 1);
  }

  private void quickSort(final int low, final int high) {
    int from = low;
    int to = high;
    while (from < to) {
      if (to - from < INSERTION_SORT_THRESHOLD) {
        insertionSort(from, to);
        return;
      }
      final int pivot = partition(from, to);
      // Recurse into the smaller half and loop on the larger: stack depth stays logarithmic even
      // when the data defeats the pivot choice.
      if (pivot - from < to - pivot) {
        quickSort(from, pivot - 1);
        from = pivot + 1;
      } else {
        quickSort(pivot + 1, to);
        to = pivot - 1;
      }
    }
  }

  private void insertionSort(final int low, final int high) {
    for (int i = low + 1; i <= high; i++) {
      final int offset = offsets[i];
      final int length = lengths[i];
      int j = i - 1;
      while (j >= low && ValueDictionaryEntryNode.compareUtf16Range(arena, offsets[j], lengths[j], arena, offset,
          length) > 0) {
        offsets[j + 1] = offsets[j];
        lengths[j + 1] = lengths[j];
        j--;
      }
      offsets[j + 1] = offset;
      lengths[j + 1] = length;
    }
  }

  private int partition(final int low, final int high) {
    final int middle = low + ((high - low) >>> 1);
    // Median of three, then park the pivot at high - 1 so the scan needs no bounds test.
    if (compareAt(middle, low) < 0) {
      swap(middle, low);
    }
    if (compareAt(high, low) < 0) {
      swap(high, low);
    }
    if (compareAt(high, middle) < 0) {
      swap(high, middle);
    }
    swap(middle, high - 1);
    final int pivot = high - 1;
    int left = low;
    int right = high - 1;
    while (true) {
      while (compareAt(++left, pivot) < 0) {
        // advance
      }
      while (compareAt(--right, pivot) > 0) {
        // advance
      }
      if (left >= right) {
        break;
      }
      swap(left, right);
    }
    swap(left, high - 1);
    return left;
  }

  private void swap(final int left, final int right) {
    final int offset = offsets[left];
    final int length = lengths[left];
    offsets[left] = offsets[right];
    lengths[left] = lengths[right];
    offsets[right] = offset;
    lengths[right] = length;
  }

  @Override
  public void close() {
    for (final Path run : runs) {
      try {
        Files.deleteIfExists(run);
      } catch (final IOException e) {
        throw new UncheckedIOException("could not delete a spilled run", e);
      }
    }
    runs.clear();
    arena = new byte[0];
    offsets = new int[0];
    lengths = new int[0];
    count = 0;
    arenaUsed = 0;
  }

  /** Counts what it yields, so the caller learns the exact distinct count for free. */
  private final class CountingIterator implements Iterator<byte[]> {
    private final Iterator<byte[]> delegate;

    CountingIterator(final Iterator<byte[]> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public byte[] next() {
      final byte[] value = delegate.next();
      distinct++;
      return value;
    }
  }

  /** The everything-fitted case: the sorted arena, duplicates skipped. */
  private final class BufferedIterator implements Iterator<byte[]> {
    private int index;

    @Override
    public boolean hasNext() {
      return index < count;
    }

    @Override
    public byte[] next() {
      if (index >= count) {
        throw new NoSuchElementException();
      }
      final byte[] value = new byte[lengths[index]];
      System.arraycopy(arena, offsets[index], value, 0, lengths[index]);
      final int taken = index;
      index++;
      while (index < count && equalAt(taken, index)) {
        index++;
      }
      return value;
    }
  }

  /** k-way merge over the spilled runs, dropping values equal to the one just yielded. */
  private static final class MergingIterator implements Iterator<byte[]> {
    private final PriorityQueue<RunCursor> queue;

    private byte[] previous;

    private byte[] pending;

    MergingIterator(final List<Path> runs) {
      queue = new PriorityQueue<>(Math.max(1, runs.size()),
          (left, right) -> ValueDictionaryEntryNode.compareUtf16Range(left.head, 0, left.head.length, right.head, 0,
              right.head.length));
      for (final Path run : runs) {
        final RunCursor cursor = new RunCursor(run);
        if (cursor.advance()) {
          queue.add(cursor);
        } else {
          cursor.close();
        }
      }
      advance();
    }

    private void advance() {
      pending = null;
      while (!queue.isEmpty()) {
        final RunCursor cursor = queue.poll();
        final byte[] value = cursor.head;
        if (cursor.advance()) {
          queue.add(cursor);
        } else {
          cursor.close();
        }
        if (previous != null && ValueDictionaryEntryNode.compareUtf16Range(previous, 0, previous.length, value, 0,
            value.length) == 0) {
          continue; // the same value, reached through another run
        }
        previous = value;
        pending = value;
        return;
      }
    }

    @Override
    public boolean hasNext() {
      return pending != null;
    }

    @Override
    public byte[] next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      final byte[] value = pending;
      advance();
      return value;
    }
  }

  /** One spilled run, read forward. */
  private static final class RunCursor {
    private final DataInputStream in;

    private byte[] head;

    RunCursor(final Path run) {
      try {
        in = new DataInputStream(new BufferedInputStream(Files.newInputStream(run), 1 << 20));
      } catch (final IOException e) {
        throw new UncheckedIOException("could not open a spilled run", e);
      }
    }

    boolean advance() {
      try {
        final int length = in.readInt();
        final byte[] value = new byte[length];
        in.readFully(value);
        head = value;
        return true;
      } catch (final EOFException end) {
        head = null;
        return false;
      } catch (final IOException e) {
        throw new UncheckedIOException("could not read a spilled run", e);
      }
    }

    void close() {
      try {
        in.close();
      } catch (final IOException e) {
        throw new UncheckedIOException("could not close a spilled run", e);
      }
    }
  }
}
