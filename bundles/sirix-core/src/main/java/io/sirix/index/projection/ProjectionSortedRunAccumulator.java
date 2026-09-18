/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * Packed, heap-bounded external sort of one initial build of a sorted projection view.
 *
 * <p>Keys are copied into grow-only byte blocks; one primitive long per row names its block and
 * offset, and sorting moves only those longs. The blocks and the reference array together never
 * exceed {@code -Dsirix.projection.sortedRun.budgetBytes} (default {@code min(heap/16, 512 MiB)}):
 * before an append would cross it, the current run is sorted and spilled, through the resource's
 * byte handlers, to a run file in this build's directory under the resource's spill directory (see
 * {@link ProjectionSortedRunSpill}), and its blocks are reused. Persisting merges the spilled runs
 * with one bounded read buffer per run and encodes leaves as keys stream past, so the heap holds at
 * most one leaf's keys beyond those buffers. A build that fits the budget never touches the file
 * system. An I/O failure while spilling or merging fails the build with a {@link SirixIOException}
 * naming the spill directory and its cause. The owning build releases the run, including every
 * spill file and the build's directory, after the transaction has published its sorted directory or
 * aborted.</p>
 */
final class ProjectionSortedRunAccumulator implements ProjectionSortedLeaf.KeySource {

  static final String BUDGET_PROPERTY = "sirix.projection.sortedRun.budgetBytes";

  private static final int MIN_BLOCK_BYTES = 1 << 16;
  private static final int MAX_BLOCK_BYTES = 8 << 20;
  private static final int MAX_KEY_BYTES = 0xFFFF;
  private static final int INITIAL_REFERENCES = 4096;
  private static final int IO_BUFFER_BYTES = 1 << 17;

  private final ProjectionSortKeyCodec.Layout layout;
  private final long budgetBytes;
  private final ProjectionSortedRunSpill spillTarget;
  private byte[][] blocks = new byte[8][];
  private int blockCount;
  private int blockUsed;
  private long allocatedBlockBytes;
  private long[] references = new long[INITIAL_REFERENCES];
  private int count;
  private long spilledRows;
  private boolean sorted;
  private final ArrayList<Path> runs = new ArrayList<>();
  private @Nullable Path runDirectory;
  private byte @Nullable [] ioBuffer;

  /**
   * A run bounded by the configured heap budget that spills to {@code spill}.
   *
   * @param layout the sorted view's key layout
   * @param spill the resource's spill directory and byte handlers
   */
  ProjectionSortedRunAccumulator(final ProjectionSortKeyCodec.Layout layout, final ProjectionSortedRunSpill spill) {
    this(layout, defaultBudgetBytes(), spill);
  }

  /**
   * A run with an explicit heap budget that spills, unencoded, below {@code spillDirectory}.
   *
   * @param layout the sorted view's key layout
   * @param budgetBytes positive heap ceiling for resident keys and references
   * @param spillDirectory the directory build directories are created in
   */
  ProjectionSortedRunAccumulator(final ProjectionSortKeyCodec.Layout layout, final long budgetBytes,
      final Path spillDirectory) {
    this(layout, budgetBytes, new ProjectionSortedRunSpill(spillDirectory, new ByteHandlerPipeline()));
  }

  /**
   * A run with an explicit heap budget that spills to {@code spill}.
   *
   * @param layout the sorted view's key layout
   * @param budgetBytes positive heap ceiling for resident keys and references
   * @param spill the spill directory and byte handlers
   */
  ProjectionSortedRunAccumulator(final ProjectionSortKeyCodec.Layout layout, final long budgetBytes,
      final ProjectionSortedRunSpill spill) {
    this.layout = Objects.requireNonNull(layout, "layout");
    if (budgetBytes <= 0) {
      throw new IllegalArgumentException(BUDGET_PROPERTY + " must be positive: " + budgetBytes);
    }
    this.budgetBytes = budgetBytes;
    this.spillTarget = Objects.requireNonNull(spill, "spill");
  }

  static long defaultBudgetBytes() {
    final String configured = System.getProperty(BUDGET_PROPERTY);
    if (configured != null) {
      final long parsed = Long.parseLong(configured.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException(BUDGET_PROPERTY + " must be positive: " + parsed);
      }
      return parsed;
    }
    return Math.min(Runtime.getRuntime().maxMemory() / 16, 512L << 20);
  }

  /** Rows appended so far, spilled or resident. */
  long rowCount() {
    return spilledRows + count;
  }

  /** Sorted runs written to disk so far. */
  int spilledRunCount() {
    return runs.size();
  }

  /** This build's directory of spilled runs, or {@code null} before the first spill and after release. */
  @Nullable Path runDirectory() {
    return runDirectory;
  }

  /** Heap held by resident keys and their references; never above the budget once a run spilled. */
  long residentBytes() {
    return allocatedBlockBytes + (long) references.length * Long.BYTES;
  }

  void append(final byte[] key, final int length) {
    Objects.requireNonNull(key, "key");
    if (sorted) {
      throw new IllegalStateException("sorted projection run is already sorted");
    }
    if (length < 0 || length > key.length || length > MAX_KEY_BYTES) {
      throw new IllegalArgumentException("sorted projection key length is outside its page format");
    }
    if (count == Integer.MAX_VALUE) {
      throw new IllegalStateException("sorted projection run exhausted row references");
    }
    final int needed = length + Short.BYTES;
    if (count == references.length) {
      final int grown = references.length <= Integer.MAX_VALUE / 2
          ? references.length << 1
          : Integer.MAX_VALUE;
      if (count > 0 && allocatedBlockBytes + (long) grown * Long.BYTES > budgetBytes) {
        spill();
      } else {
        references = Arrays.copyOf(references, grown);
      }
    }
    ensureBlock(needed);
    final byte[] block = blocks[blockCount - 1];
    final int offset = blockUsed;
    block[offset] = (byte) length;
    block[offset + 1] = (byte) (length >>> 8);
    System.arraycopy(key, 0, block, offset + Short.BYTES, length);
    blockUsed += needed;
    references[count++] = ((long) (blockCount - 1) << Integer.SIZE) | (offset & 0xFFFF_FFFFL);
  }

  void sort() {
    if (sorted) {
      return;
    }
    LongArrays.quickSort(references, 0, count, this::compare);
    for (int i = 1; i < count; i++) {
      if (compare(references[i - 1], references[i]) >= 0) {
        throw new IllegalStateException("sorted projection run contains a duplicate row key");
      }
    }
    sorted = true;
  }

  /** Persist sorted data leaves and publish their sparse directory last. */
  long persist(final ProjectionIndexHOTStorage storage) {
    Objects.requireNonNull(storage, "storage");
    final ProjectionSortedDirectory.Builder directory = new ProjectionSortedDirectory.Builder(storage, layout);
    if (runs.isEmpty()) {
      sort();
      int from = 0;
      while (from < count) {
        from += appendLeaf(directory, this, from, Math.min(ProjectionSortedLeaf.MAX_ROWS, count - from));
      }
      directory.finish();
      return count;
    }
    if (count > 0) {
      spill();
    }
    final long rows = merge(directory);
    directory.finish();
    deleteSpillFiles();
    return rows;
  }

  void release() {
    blocks = new byte[0][];
    references = new long[0];
    blockCount = 0;
    blockUsed = 0;
    allocatedBlockBytes = 0;
    count = 0;
    spilledRows = 0;
    sorted = true;
    ioBuffer = null;
    deleteSpillFiles();
  }

  @Override
  public int keyCount() {
    return sorted
        ? count
        : 0;
  }

  @Override
  public byte[] keyBlock(final int position) {
    return blockOf(referenceAt(position));
  }

  @Override
  public int keyOffset(final int position) {
    return ((int) referenceAt(position)) + Short.BYTES;
  }

  @Override
  public int keyLength(final int position) {
    return keyLength(referenceAt(position));
  }

  private long referenceAt(final int sortedPosition) {
    if (!sorted || sortedPosition < 0 || sortedPosition >= count) {
      throw new IllegalStateException("sorted projection run is not positioned on a sorted row");
    }
    return references[sortedPosition];
  }

  private byte[] blockOf(final long reference) {
    return blocks[(int) (reference >>> Integer.SIZE)];
  }

  private int keyLength(final long reference) {
    final byte[] block = blockOf(reference);
    final int offset = (int) reference;
    return (block[offset] & 0xFF) | (block[offset + 1] & 0xFF) << 8;
  }

  private int compare(final long left, final long right) {
    final byte[] leftBlock = blockOf(left);
    final byte[] rightBlock = blockOf(right);
    final int leftOffset = ((int) left) + Short.BYTES;
    final int rightOffset = ((int) right) + Short.BYTES;
    return Arrays.compareUnsigned(leftBlock, leftOffset, leftOffset + keyLength(left),
        rightBlock, rightOffset, rightOffset + keyLength(right));
  }

  /** Encode the largest prefix of {@code count} keys that fits one bounded leaf; return how many. */
  private static int appendLeaf(final ProjectionSortedDirectory.Builder directory,
      final ProjectionSortedLeaf.KeySource source, final int from, final int count) {
    int rows = count;
    ProjectionSortedLeaf leaf;
    do {
      leaf = ProjectionSortedLeaf.encodeSortedRun(source, from, rows);
      if (leaf == null) {
        rows >>>= 1;
      }
    } while (leaf == null && rows > 0);
    if (leaf == null) {
      throw new IllegalStateException("sorted projection key exceeds a bounded data leaf");
    }
    directory.append(leaf);
    return rows;
  }

  /** Room for {@code needed} bytes, reusing blocks kept from a spilled run before allocating. */
  private void ensureBlock(final int needed) {
    if (blockCount > 0 && blocks[blockCount - 1].length - blockUsed >= needed) {
      return;
    }
    if (blockCount < blocks.length && blocks[blockCount] != null && blocks[blockCount].length >= needed) {
      blockCount++;
      blockUsed = 0;
      return;
    }
    final int previous = blockCount == 0
        ? 0
        : blocks[blockCount - 1].length;
    final int preferred = Math.max(MIN_BLOCK_BYTES, previous >= MAX_BLOCK_BYTES
        ? MAX_BLOCK_BYTES
        : previous << 1);
    final byte[] replaced = blockCount < blocks.length
        ? blocks[blockCount]
        : null;
    final long headroom = budgetBytes - residentBytes() + (replaced == null
        ? 0
        : replaced.length);
    if (count > 0 && needed > headroom) {
      spill();
      ensureBlock(needed);
      return;
    }
    final int size = (int) Math.max(needed, Math.min(preferred, headroom));
    if (blockCount == blocks.length) {
      blocks = Arrays.copyOf(blocks, blocks.length << 1);
    }
    if (replaced != null) {
      allocatedBlockBytes -= replaced.length;
    }
    blocks[blockCount++] = new byte[size];
    allocatedBlockBytes += size;
    blockUsed = 0;
  }

  /** Sort the resident run, write it as one length-prefixed key file, and keep its blocks for reuse. */
  private void spill() {
    sort();
    try {
      Path directory = runDirectory;
      if (directory == null) {
        directory = spillTarget.createBuildDirectory();
        runDirectory = directory;
      }
      final Path file = directory.resolve("run-" + runs.size() + ".keys");
      runs.add(file);
      try (OutputStream output = spillTarget.openWriter(file)) {
        final byte[] buffer = ioBuffer();
        int used = 0;
        for (int i = 0; i < count; i++) {
          final long reference = references[i];
          final int length = keyLength(reference);
          if (buffer.length - used < length + Short.BYTES) {
            output.write(buffer, 0, used);
            used = 0;
          }
          buffer[used] = (byte) length;
          buffer[used + 1] = (byte) (length >>> 8);
          System.arraycopy(blockOf(reference), ((int) reference) + Short.BYTES, buffer, used + Short.BYTES, length);
          used += length + Short.BYTES;
        }
        if (used > 0) {
          output.write(buffer, 0, used);
        }
      }
    } catch (final IOException e) {
      throw spillFailure("cannot spill a sorted run to", e);
    }
    spilledRows += count;
    count = 0;
    blockCount = 0;
    blockUsed = 0;
    sorted = false;
  }

  private byte[] ioBuffer() {
    byte[] buffer = ioBuffer;
    if (buffer == null) {
      buffer = new byte[IO_BUFFER_BYTES];
      ioBuffer = buffer;
    }
    return buffer;
  }

  /** A load-failing spill error that names the spill location, the cause and the remedies. */
  private SirixIOException spillFailure(final String action, final IOException cause) {
    final Path directory = runDirectory;
    return new SirixIOException("Sorted projection view " + action + " " + (directory == null
        ? spillTarget.directory()
        : directory) + ": " + cause + ". A full or unwritable disk fails the load as any write does; free space"
        + " there, point -D" + ProjectionSortedRunSpill.SPILL_DIRECTORY_PROPERTY + " at another directory, or raise -D"
        + BUDGET_PROPERTY + " to keep more of the build on the heap.", cause);
  }

  /** K-way merge of the spilled runs straight into bounded leaves; duplicates fail the build. */
  private long merge(final ProjectionSortedDirectory.Builder directory) {
    final RunReader[] readers = new RunReader[runs.size()];
    Throwable failure = null;
    try {
      return merge(directory, readers);
    } catch (final IOException e) {
      final SirixIOException wrapped = spillFailure("cannot merge the sorted runs in", e);
      failure = wrapped;
      throw wrapped;
    } catch (final RuntimeException | Error e) {
      failure = e;
      throw e;
    } finally {
      closeReaders(readers, failure);
    }
  }

  private long merge(final ProjectionSortedDirectory.Builder directory, final RunReader[] readers)
      throws IOException {
    final int runCount = readers.length;
    final int[] heap = new int[runCount];
    int size = 0;
    long rows = 0;
    for (int i = 0; i < runCount; i++) {
      readers[i] = new RunReader(spillTarget.openReader(runs.get(i)));
      if (readers[i].next()) {
        heap[size++] = i;
      }
    }
    for (int parent = (size >>> 1) - 1; parent >= 0; parent--) {
      siftDown(heap, readers, parent, size);
    }
    final Stage stage = new Stage();
    byte[] previous = new byte[128];
    int previousLength = -1;
    while (size > 0) {
      final RunReader reader = readers[heap[0]];
      if (previousLength >= 0 && Arrays.compareUnsigned(previous, 0, previousLength, reader.key, 0,
          reader.length) >= 0) {
        throw new IllegalStateException("sorted projection run contains a duplicate row key");
      }
      if (reader.length > previous.length) {
        previous = new byte[Math.max(reader.length, previous.length << 1)];
      }
      System.arraycopy(reader.key, 0, previous, 0, reader.length);
      previousLength = reader.length;
      stage.add(reader.key, reader.length);
      rows++;
      if (stage.keyCount() == ProjectionSortedLeaf.MAX_ROWS) {
        stage.drop(appendLeaf(directory, stage, 0, ProjectionSortedLeaf.MAX_ROWS));
      }
      if (!reader.next()) {
        heap[0] = heap[--size];
      }
      siftDown(heap, readers, 0, size);
    }
    while (stage.keyCount() > 0) {
      stage.drop(appendLeaf(directory, stage, 0,
          Math.min(ProjectionSortedLeaf.MAX_ROWS, stage.keyCount())));
    }
    if (rows != spilledRows) {
      throw new IllegalStateException("sorted projection merge read " + rows + " of " + spilledRows + " rows");
    }
    return rows;
  }

  /** Close every opened run; a close failure is suppressed into {@code primary} or fails loudly. */
  private void closeReaders(final RunReader[] readers, final @Nullable Throwable primary) {
    IOException closeFailure = null;
    for (final RunReader reader : readers) {
      if (reader == null) {
        continue;
      }
      try {
        reader.close();
      } catch (final IOException e) {
        if (closeFailure == null) {
          closeFailure = e;
        } else {
          closeFailure.addSuppressed(e);
        }
      }
    }
    if (closeFailure == null) {
      return;
    }
    if (primary != null) {
      primary.addSuppressed(closeFailure);
      return;
    }
    throw spillFailure("cannot close the sorted runs in", closeFailure);
  }

  private static void siftDown(final int[] heap, final RunReader[] readers, final int root, final int size) {
    final int value = heap[root];
    int parent = root;
    for (int child = (parent << 1) + 1; child < size; child = (parent << 1) + 1) {
      if (child + 1 < size && readers[heap[child + 1]].compareTo(readers[heap[child]]) < 0) {
        child++;
      }
      if (readers[value].compareTo(readers[heap[child]]) <= 0) {
        break;
      }
      heap[parent] = heap[child];
      parent = child;
    }
    heap[parent] = value;
  }

  /** Delete every run file and this build's directory, then stop protecting the directory. */
  private void deleteSpillFiles() {
    final Path directory = runDirectory;
    IOException failure = null;
    for (final Path run : runs) {
      try {
        Files.deleteIfExists(run);
      } catch (final IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    runs.clear();
    runDirectory = null;
    if (directory != null) {
      try {
        Files.deleteIfExists(directory);
      } catch (final IOException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      } finally {
        ProjectionSortedRunSpill.retireBuildDirectory(directory);
      }
    }
    if (failure != null) {
      throw new SirixIOException("Sorted projection view cannot delete its sorted runs in " + directory + ": "
          + failure + ". They are removed the next time the resource is opened.", failure);
    }
  }

  /** One spilled run read sequentially through a bounded buffer; its current key is reused. */
  private static final class RunReader {
    private final InputStream input;
    private final byte[] buffer = new byte[IO_BUFFER_BYTES];
    private int position;
    private int limit;
    private byte[] key = new byte[128];
    private int length;
    private boolean endOfFile;

    RunReader(final InputStream input) {
      this.input = input;
    }

    boolean next() throws IOException {
      if (!ensure(Short.BYTES)) {
        if (position < limit) {
          throw new IOException("truncated sorted projection run");
        }
        return false;
      }
      length = (buffer[position] & 0xFF) | (buffer[position + 1] & 0xFF) << 8;
      position += Short.BYTES;
      if (!ensure(length)) {
        throw new IOException("truncated sorted projection run");
      }
      if (length > key.length) {
        key = new byte[Math.max(length, key.length << 1)];
      }
      System.arraycopy(buffer, position, key, 0, length);
      position += length;
      return true;
    }

    int compareTo(final RunReader other) {
      return Arrays.compareUnsigned(key, 0, length, other.key, 0, other.length);
    }

    private boolean ensure(final int bytes) throws IOException {
      while (limit - position < bytes && !endOfFile) {
        if (position > 0) {
          System.arraycopy(buffer, position, buffer, 0, limit - position);
          limit -= position;
          position = 0;
        }
        final int read = input.read(buffer, limit, buffer.length - limit);
        if (read < 0) {
          endOfFile = true;
        } else {
          limit += read;
        }
      }
      return limit - position >= bytes;
    }

    void close() throws IOException {
      input.close();
    }
  }

  /** Staging window of merged keys awaiting leaf encoding; holds at most one leaf's rows. */
  private static final class Stage implements ProjectionSortedLeaf.KeySource {
    private byte[] arena = new byte[1 << 16];
    private final int[] offsets = new int[ProjectionSortedLeaf.MAX_ROWS];
    private final int[] lengths = new int[ProjectionSortedLeaf.MAX_ROWS];
    private int count;
    private int used;

    void add(final byte[] key, final int length) {
      if (used + length > arena.length) {
        arena = Arrays.copyOf(arena, Math.max(used + length, arena.length << 1));
      }
      System.arraycopy(key, 0, arena, used, length);
      offsets[count] = used;
      lengths[count++] = length;
      used += length;
    }

    /** Discard the first {@code rows} keys, compacting any survivors to the front. */
    void drop(final int rows) {
      if (rows == count) {
        count = 0;
        used = 0;
        return;
      }
      final int shift = offsets[rows];
      System.arraycopy(arena, shift, arena, 0, used - shift);
      for (int i = rows; i < count; i++) {
        offsets[i - rows] = offsets[i] - shift;
        lengths[i - rows] = lengths[i];
      }
      count -= rows;
      used -= shift;
    }

    @Override
    public int keyCount() {
      return count;
    }

    @Override
    public byte[] keyBlock(final int position) {
      return arena;
    }

    @Override
    public int keyOffset(final int position) {
      return offsets[position];
    }

    @Override
    public int keyLength(final int position) {
      return lengths[position];
    }
  }
}
