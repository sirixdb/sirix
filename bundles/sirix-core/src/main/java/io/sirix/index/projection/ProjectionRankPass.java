package io.sirix.index.projection;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.ValueDictionaryEntryNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Rewrites one {@code COLUMN_KIND_STRING_DICT} column of an existing projection index as a
 * {@code COLUMN_KIND_STRING_GLOBAL} column whose ids are RANKS — assigned in UTF-16 collation order
 * of their values.
 *
 * <p>
 * <b>Why this is a post-pass and not part of the load.</b> Rank order is a property of the whole
 * value set. A streaming mint assigns id {@code k+1} to the {@code k+1}-th distinct value it
 * happens to SEE, and no amount of bookkeeping turns that into a rank without knowing every value
 * that follows. So the pass runs over an index that is already built, which is also the cheapest
 * place for it: the load path is left byte-identical, and a pass that fails leaves a perfectly good
 * per-leaf projection behind.
 * </p>
 *
 * <p>
 * <b>Four stages, and what each one costs.</b>
 * </p>
 * <ol>
 * <li><b>S1 extract</b> — read each leaf's per-leaf dictionary, validate every value is well-formed
 * UTF-8, and emit {@code (value, leafId, localId)} into a bounded run buffer that sorts and spills
 * when full. Validation is not defensive padding: the merge orders by BYTES, and the equivalence
 * between byte order and the engine's collation holds only for well-formed UTF-8, so a CESU-8
 * surrogate would be ranked into a position {@code compareUtf16Range} disagrees with.</li>
 * <li><b>S2 merge</b> — k-way merge the runs, mint one rank per distinct value into a front-less
 * appender, and emit {@code (leafId, localId, rank)} triples. The appender commits per
 * generation.</li>
 * <li><b>S3 group</b> — bucket the triples by leaf. Counting sort rather than a comparison sort:
 * leaf ids are dense and bounded, so this is one counting pass and one scatter.</li>
 * <li><b>S4 remap</b> — rewrite each leaf's column through the ranks and drop its DICT, BLOOM and
 * DICT_HASHES segments, in ONE commit.</li>
 * </ol>
 *
 * <p>
 * <b>Why S4 is a single commit while S2 is many.</b> The column kind lives in EVERY row-group
 * descriptor, not only in the resource-level metadata, and {@code ProjectionColumnStore} refuses to
 * build when leaf 0 disagrees with any other leaf. A per-epoch S4 would therefore leave every
 * intermediate revision with a store that cannot be constructed — not one that quietly serves the
 * old form. The dirty set that buys this is small because S2 has already committed the dictionary
 * and only one column's id lane is rewritten: at 100M that is ~312 MB for URL, against the whole
 * dictionary the naive reading of the design feared.
 * </p>
 *
 * <p>
 * The pass is a bulk reorganisation, not an online one: it needs the resource's write transaction
 * for its duration.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ProjectionRankPass {

  /**
   * Segment 1's master switch, OFF by default — the same property that gates the block encoder, so
   * the pass and the storage forms it depends on cannot be enabled independently of each other.
   */
  private static final boolean ENABLED = Boolean.getBoolean("sirix.projection.globalDict.rank");

  /** Default run-buffer ceiling. The only tunable, and correctness-neutral. */
  private static final long DEFAULT_BUFFER_BYTES = 256L << 20;

  /** {@code (leafId, localId, rank)}, fixed width so S3 can address them arithmetically. */
  private static final int TRIPLE_BYTES = 3 * Integer.BYTES;

  /** What one pass did, so a caller can report bytes and time without instrumenting the stages. */
  public record Result(int column, int distinctValues, int leavesRewritten, long dictionaryHeaderKey, int generations,
      long extractedEntries, long spilledBytes, long extractNanos, long mergeNanos, long groupNanos, long remapNanos) {
  }

  private ProjectionRankPass() {
    throw new AssertionError("no instances");
  }

  /**
   * Runs the pass for one column.
   *
   * @param wtx the resource's write transaction, which the pass commits several times
   * @param indexNumber the projection index number
   * @param column the column to convert
   * @param spillDirectory a directory for the run and triple files, created if absent
   * @param bufferBytes run-buffer ceiling, or {@code <= 0} for the default
   * @return what the pass did
   */
  public static Result run(final JsonNodeTrx wtx, final int indexNumber, final int column, final Path spillDirectory,
      final long bufferBytes) {
    if (wtx == null) {
      throw new IllegalArgumentException("a write transaction is required — the pass is a bulk reorganisation");
    }
    if (column < 0) {
      throw new IllegalArgumentException("column must not be negative: " + column);
    }
    if (!ENABLED) {
      throw new IllegalStateException("the rank pass is disabled; set -Dsirix.projection.globalDict.rank=true to run "
          + "it. It converts a column's encoding and the arms that serve COLUMN_KIND_STRING_GLOBAL are not all "
          + "built, so it is opt-in until they are");
    }
    final long buffer = bufferBytes > 0
        ? bufferBytes
        : DEFAULT_BUFFER_BYTES;
    try {
      Files.createDirectories(spillDirectory);
      return runChecked(wtx, indexNumber, column, spillDirectory, buffer);
    } catch (final IOException e) {
      throw new UncheckedIOException("rank pass failed for column " + column, e);
    }
  }

  private static Result runChecked(final JsonNodeTrx wtx, final int indexNumber, final int column,
      final Path spillDirectory, final long bufferBytes) throws IOException {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), indexNumber);
    final byte[] slot0 = storage.getBlob(0L);
    if (slot0 == null) {
      throw new IllegalStateException("projection index " + indexNumber + " has no metadata; nothing to convert");
    }
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(slot0);
    final byte[] kinds = metadata.columnKinds();
    if (column >= kinds.length) {
      throw new IllegalArgumentException("column " + column + " is outside the index's " + kinds.length + " columns");
    }
    if (kinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      throw new IllegalStateException("column " + column + " is kind " + kinds[column]
          + ", not STRING_DICT; only a per-leaf dictionary can be " + "ranked");
    }
    final int rowGroupCount = metadata.rowGroupCount();

    final Path runDirectory = spillDirectory.resolve("runs-c" + column);
    deleteRecursively(runDirectory);
    Files.createDirectories(runDirectory);

    final long t0 = System.nanoTime();
    final ExtractResult extracted = extract(storage, column, rowGroupCount, runDirectory, bufferBytes);
    final long t1 = System.nanoTime();

    final Path triples = spillDirectory.resolve("triples-c" + column + ".bin");
    final MergeResult merged = merge(wtx, column, extracted.runs, triples);
    final long t2 = System.nanoTime();

    final Path grouped = spillDirectory.resolve("triples-c" + column + "-byleaf.bin");
    final int[] leafOffsets = groupByLeaf(triples, grouped, rowGroupCount, merged.triples);
    final long t3 = System.nanoTime();

    final int rewritten =
        remap(wtx, indexNumber, column, rowGroupCount, grouped, leafOffsets, merged.headerKey, metadata);
    final long t4 = System.nanoTime();

    deleteRecursively(runDirectory);
    Files.deleteIfExists(triples);
    Files.deleteIfExists(grouped);

    return new Result(column, merged.distinctValues, rewritten, merged.headerKey, merged.generations, extracted.entries,
        extracted.spilledBytes, t1 - t0, t2 - t1, t3 - t2, t4 - t3);
  }

  // -----------------------------------------------------------------------------------------------
  // S1 — extract
  // -----------------------------------------------------------------------------------------------

  private record ExtractResult(List<Path> runs, long entries, long spilledBytes) {
  }

  private static ExtractResult extract(final ProjectionIndexHOTStorage storage, final int column,
      final int rowGroupCount, final Path runDirectory, final long bufferBytes) throws IOException {
    final RunBuffer runBuffer = new RunBuffer(bufferBytes);
    final List<Path> runs = new ArrayList<>();
    long entries = 0;
    long spilled = 0;

    for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
      final byte[] descriptor = storage.getVerifiedRowGroupDescriptor(rowGroupId);
      if (descriptor == null) {
        continue;
      }
      if (RowGroupDescriptor.kind(descriptor, column) != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("leaf " + rowGroupId + " declares column " + column + " as kind "
            + RowGroupDescriptor.kind(descriptor, column) + " while the index declares STRING_DICT");
      }
      final byte[] dictSegment = storage.getVerifiedColumnSegment(rowGroupId, descriptor,
          ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(column),
          ProjectionIndexColumnSegmentCodec.SEG_KIND_DICT);
      if (dictSegment == null) {
        // A rowless leaf writes no DICT segment at all; it contributes no values and needs no ranks.
        continue;
      }
      final byte[] bodySegment = storage.getVerifiedColumnSegment(rowGroupId, descriptor,
          ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(column),
          ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY);
      final ProjectionColumnStore.ColumnSlice slice =
          ProjectionIndexColumnSegmentCodec.decodeStringSlice(descriptor, bodySegment, dictSegment, column);
      final byte[] dictBytes = slice.dictBytes();
      final int[] dictOffsets = slice.dictOffsets();
      final int dictSize = slice.dictSize();
      for (int local = 0; local < dictSize; local++) {
        final int offset = dictOffsets[local];
        final int length = dictOffsets[local + 1] - offset;
        if (!isWellFormedUtf8(dictBytes, offset, length)) {
          throw new IllegalStateException("leaf " + rowGroupId + " column " + column + " entry " + local
              + " is not well-formed UTF-8; the rank pass orders by bytes and that equivalence holds only for "
              + "well-formed input");
        }
        if (!runBuffer.add(dictBytes, offset, length, rowGroupId, local)) {
          spilled += spillRun(runBuffer, runDirectory, runs);
          if (!runBuffer.add(dictBytes, offset, length, rowGroupId, local)) {
            throw new IllegalStateException("value of " + length + " bytes does not fit an empty run buffer of "
                + bufferBytes + " bytes; raise sirix.projection.globalDict.rankPass.bufferBytes");
          }
        }
        entries++;
      }
    }
    if (runBuffer.size() > 0) {
      spilled += spillRun(runBuffer, runDirectory, runs);
    }
    return new ExtractResult(runs, entries, spilled);
  }

  private static long spillRun(final RunBuffer runBuffer, final Path runDirectory, final List<Path> runs)
      throws IOException {
    runBuffer.sort();
    final Path run = runDirectory.resolve("run-" + runs.size() + ".bin");
    final long written = runBuffer.writeTo(run);
    runs.add(run);
    runBuffer.clear();
    return written;
  }

  // -----------------------------------------------------------------------------------------------
  // S2 — merge and rank
  // -----------------------------------------------------------------------------------------------

  private record MergeResult(int distinctValues, long headerKey, int generations, long triples) {
  }

  private static MergeResult merge(final JsonNodeTrx wtx, final int column, final List<Path> runs, final Path triples)
      throws IOException {
    final DatabaseType databaseType = GlobalValueDictionary.databaseTypeOf(wtx.getStorageEngineWriter());
    // The appender re-reads the transaction's writer after every commit: a StorageEngineWriter is
    // bound to a revision, so holding the pre-commit one would stage the next generation against a
    // revision that has already been published.
    final RankPassDictionaryAppender appender =
        new RankPassDictionaryAppender(column, databaseType, wtx.getStorageEngineWriter(), generation -> {
          wtx.commit();
          return wtx.getStorageEngineWriter();
        });

    final PriorityQueue<RunCursor> queue = new PriorityQueue<>();
    final List<RunCursor> open = new ArrayList<>(runs.size());
    long tripleCount = 0;
    int distinct = 0;
    try (DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(triples), 1 << 20))) {
      for (final Path run : runs) {
        final RunCursor cursor = new RunCursor(run);
        open.add(cursor);
        if (cursor.advance()) {
          queue.add(cursor);
        }
      }
      byte[] current = null;
      int currentLength = 0;
      int rank = 0;
      while (!queue.isEmpty()) {
        final RunCursor head = queue.poll();
        if (current == null
            || ValueDictionaryEntryNode.compareUtf16Range(current, 0, currentLength, head.value, 0, head.length) != 0) {
          rank = appender.accept(head.value, 0, head.length);
          distinct++;
          if (current == null || current.length < head.length) {
            current = new byte[Math.max(head.length, 256)];
          }
          System.arraycopy(head.value, 0, current, 0, head.length);
          currentLength = head.length;
        }
        out.writeInt(head.leafId);
        out.writeInt(head.localId);
        out.writeInt(rank);
        tripleCount++;
        if (head.advance()) {
          queue.add(head);
        }
      }
      appender.finish();
      // The separator array is built once the dictionary is complete and before anything reads it:
      // it is what makes a probe decode ONE block instead of one per binary-search step.
      final var storageEngineWriter = wtx.getStorageEngineWriter();
      GlobalValueDictionary.buildBlockIndex(appender.headerKey(),
          storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage()), databaseType,
          storageEngineWriter, storageEngineWriter.getLog());
    } finally {
      for (final RunCursor cursor : open) {
        cursor.close();
      }
    }
    // The dictionary must be durable before S4 publishes leaves that name its ids.
    wtx.commit();
    return new MergeResult(distinct, appender.headerKey(), appender.generations(), tripleCount);
  }

  // -----------------------------------------------------------------------------------------------
  // S3 — group the triples by leaf
  // -----------------------------------------------------------------------------------------------

  /**
   * Counting sort of the triples by leaf id.
   *
   * <p>
   * A comparison sort would be the general answer and the wrong one here: leaf ids are dense, bounded
   * by the index's own row-group count, and already known, so one counting pass and one scatter is
   * {@code O(n)} with a single {@code int[]} of leaf offsets.
   * </p>
   *
   * @return the start offset, in triples, of each leaf's run, with a trailing total
   */
  private static int[] groupByLeaf(final Path triples, final Path grouped, final int rowGroupCount,
      final long tripleCount) throws IOException {
    if (tripleCount > Integer.MAX_VALUE) {
      throw new IllegalStateException("this column has " + tripleCount + " dictionary entries, above what one pass "
          + "addresses; convert it in leaf ranges");
    }
    final int[] offsets = new int[rowGroupCount + 2];
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(triples), 1 << 20))) {
      for (long i = 0; i < tripleCount; i++) {
        final int leafId = in.readInt();
        in.readInt();
        in.readInt();
        offsets[leafId + 1]++;
      }
    }
    for (int i = 1; i < offsets.length; i++) {
      offsets[i] += offsets[i - 1];
    }
    final int[] cursor = offsets.clone();
    try (RandomAccessFile out = new RandomAccessFile(grouped.toFile(), "rw");
        DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(triples), 1 << 20))) {
      out.setLength((long) tripleCount * TRIPLE_BYTES);
      final byte[] record = new byte[TRIPLE_BYTES];
      for (long i = 0; i < tripleCount; i++) {
        final int leafId = in.readInt();
        final int localId = in.readInt();
        final int rank = in.readInt();
        writeInt(record, 0, leafId);
        writeInt(record, 4, localId);
        writeInt(record, 8, rank);
        out.seek((long) cursor[leafId] * TRIPLE_BYTES);
        out.write(record);
        cursor[leafId]++;
      }
    }
    return offsets;
  }

  // -----------------------------------------------------------------------------------------------
  // S4 — remap the leaves
  // -----------------------------------------------------------------------------------------------

  private static int remap(final JsonNodeTrx wtx, final int indexNumber, final int column, final int rowGroupCount,
      final Path grouped, final int[] leafOffsets, final long headerKey, final ProjectionIndexMetadata metadata)
      throws IOException {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), indexNumber);
    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    int rewritten = 0;
    try (RandomAccessFile in = new RandomAccessFile(grouped.toFile(), "r")) {
      final byte[] record = new byte[TRIPLE_BYTES];
      for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
        final int from = leafOffsets[rowGroupId];
        final int to = leafOffsets[rowGroupId + 1];
        final byte[] raw = storage.getRowGroupFromColumnSegmentSlots(rowGroupId);
        if (raw == null) {
          continue;
        }
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(raw);
        int highestLocal = -1;
        for (int i = from; i < to; i++) {
          in.seek((long) i * TRIPLE_BYTES);
          in.readFully(record);
          highestLocal = Math.max(highestLocal, readInt(record, 4));
        }
        final int[] localToGlobal = new int[highestLocal + 1];
        for (int i = from; i < to; i++) {
          in.seek((long) i * TRIPLE_BYTES);
          in.readFully(record);
          localToGlobal[readInt(record, 4)] = readInt(record, 8);
        }
        page.remapStringDictColumnToGlobal(column, localToGlobal);
        storage.putRowGroupAsColumnSegmentSlots(rowGroupId, ProjectionIndexColumnSegmentCodec.encode(page, workspace));
        rewritten++;
      }
    }

    // The column stops being a string kind, and rewriteTouchedChunks skips non-string kinds — so
    // without this its Bloom chunks stay on disk forever, reachable by nothing. Measured at 1M:
    // 1.82 MB across four columns.
    final int droppedBloomBlobs = ProjectionBloomChunks.dropColumn(storage, column, rowGroupCount);

    final byte[] kinds = metadata.columnKinds();
    kinds[column] = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
    long[] anchors = metadata.valueDictionaryHeaderKeys();
    if (anchors == null) {
      anchors = new long[kinds.length];
    }
    // Without the anchor a global column "can only be scanned, never probed" — a silent loss of
    // predicate pushdown that would show up only as latency, which is exactly the failure this
    // pass is measured against.
    anchors[column] = headerKey;
    final ProjectionIndexMetadata next =
        new ProjectionIndexMetadata(metadata.rootPath(), metadata.fieldPaths(), metadata.fieldNames(), kinds,
            metadata.rowGroupCount(), wtx.getRevisionNumber(), metadata.setValueRowCounts(), anchors);
    // Slot 0 LAST and in the SAME commit as every descriptor: the kind lives in both, and a store
    // whose leaves and metadata disagree refuses to build at all.
    storage.putBlob(0L, next.serialize());
    wtx.commit();
    if (droppedBloomBlobs > 0 && System.getProperty("sirix.projection.globalDict.rankPass.diag") != null) {
      System.out.println("[rankPass] column " + column + " released " + droppedBloomBlobs + " Bloom blobs");
    }
    return rewritten;
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  /** One spilled run, read back in order; comparable so a priority queue can merge k of them. */
  private static final class RunCursor implements Comparable<RunCursor>, AutoCloseable {
    private final DataInputStream in;
    private byte[] value = new byte[256];
    private int length;
    private int leafId;
    private int localId;

    private RunCursor(final Path path) throws IOException {
      this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 1 << 20));
    }

    private boolean advance() throws IOException {
      final int next;
      try {
        next = in.readInt();
      } catch (final java.io.EOFException exhausted) {
        return false;
      }
      if (value.length < next) {
        value = new byte[Math.max(next, value.length * 2)];
      }
      in.readFully(value, 0, next);
      length = next;
      leafId = in.readInt();
      localId = in.readInt();
      return true;
    }

    @Override
    public int compareTo(final RunCursor other) {
      final int byValue = ValueDictionaryEntryNode.compareUtf16Range(value, 0, length, other.value, 0, other.length);
      if (byValue != 0) {
        return byValue;
      }
      final int byLeaf = Integer.compare(leafId, other.leafId);
      return byLeaf != 0
          ? byLeaf
          : Integer.compare(localId, other.localId);
    }

    @Override
    public void close() {
      try {
        in.close();
      } catch (final IOException ignored) {
        // A run file is scratch; a failure to close it must not mask the pass's own outcome.
      }
    }
  }

  /**
   * A bounded arena of {@code (value, leafId, localId)} whose sort is a merge sort over an
   * {@code int[]} permutation — no per-entry object and no boxing.
   */
  private static final class RunBuffer {
    private final long ceilingBytes;
    private byte[] bytes = new byte[1 << 20];
    private int[] offsets = new int[1 << 12];
    private int[] leafIds = new int[1 << 12];
    private int[] localIds = new int[1 << 12];
    private int[] order = new int[1 << 12];
    private int count;

    private RunBuffer(final long ceilingBytes) {
      this.ceilingBytes = ceilingBytes;
    }

    private int size() {
      return count;
    }

    private boolean add(final byte[] source, final int offset, final int length, final int leafId, final int localId) {
      final long projected = (long) offsets[count] + length + (long) (count + 1) * (Integer.BYTES * 4);
      if (count > 0 && projected > ceilingBytes) {
        return false;
      }
      if (count + 2 >= offsets.length) {
        offsets = java.util.Arrays.copyOf(offsets, offsets.length << 1);
        leafIds = java.util.Arrays.copyOf(leafIds, leafIds.length << 1);
        localIds = java.util.Arrays.copyOf(localIds, localIds.length << 1);
        order = java.util.Arrays.copyOf(order, order.length << 1);
      }
      while (offsets[count] + length > bytes.length) {
        bytes = java.util.Arrays.copyOf(bytes, bytes.length << 1);
      }
      System.arraycopy(source, offset, bytes, offsets[count], length);
      offsets[count + 1] = offsets[count] + length;
      leafIds[count] = leafId;
      localIds[count] = localId;
      count++;
      return true;
    }

    private void sort() {
      for (int i = 0; i < count; i++) {
        order[i] = i;
      }
      int[] source = java.util.Arrays.copyOf(order, count);
      int[] target = new int[count];
      for (int width = 1; width < count; width <<= 1) {
        for (int start = 0; start < count; start += width << 1) {
          final int middle = Math.min(start + width, count);
          final int end = Math.min(start + (width << 1), count);
          int left = start;
          int right = middle;
          for (int at = start; at < end; at++) {
            target[at] = left < middle && (right >= end || compare(source[left], source[right]) <= 0)
                ? source[left++]
                : source[right++];
          }
        }
        final int[] swap = source;
        source = target;
        target = swap;
      }
      System.arraycopy(source, 0, order, 0, count);
    }

    private int compare(final int left, final int right) {
      final int byValue = ValueDictionaryEntryNode.compareUtf16Range(bytes, offsets[left],
          offsets[left + 1] - offsets[left], bytes, offsets[right], offsets[right + 1] - offsets[right]);
      if (byValue != 0) {
        return byValue;
      }
      final int byLeaf = Integer.compare(leafIds[left], leafIds[right]);
      return byLeaf != 0
          ? byLeaf
          : Integer.compare(localIds[left], localIds[right]);
    }

    private long writeTo(final Path path) throws IOException {
      long written = 0;
      try (OutputStream raw = Files.newOutputStream(path);
          DataOutputStream out = new DataOutputStream(new BufferedOutputStream(raw, 1 << 20))) {
        for (int i = 0; i < count; i++) {
          final int index = order[i];
          final int offset = offsets[index];
          final int length = offsets[index + 1] - offset;
          out.writeInt(length);
          out.write(bytes, offset, length);
          out.writeInt(leafIds[index]);
          out.writeInt(localIds[index]);
          written += length + 3L * Integer.BYTES;
        }
      }
      return written;
    }

    private void clear() {
      count = 0;
      offsets[0] = 0;
    }
  }

  /**
   * Well-formed UTF-8, refusing overlong forms, encoded surrogates and {@code 0xC0/0xC1/0xF5..0xFF}.
   *
   * <p>
   * The engine's own {@code decodeCodePoint} already assumes this on the read side, so the pass is
   * checking an invariant the existing code relies on rather than inventing one — but it checks it
   * where a violation is still recoverable, which is before anything has been ranked.
   * </p>
   */
  static boolean isWellFormedUtf8(final byte[] value, final int offset, final int length) {
    int at = offset;
    final int limit = offset + length;
    while (at < limit) {
      final int lead = value[at] & 0xFF;
      if (lead < 0x80) {
        at++;
      } else if (lead >= 0xC2 && lead <= 0xDF) {
        if (at + 1 >= limit || !isContinuation(value[at + 1])) {
          return false;
        }
        at += 2;
      } else if (lead >= 0xE0 && lead <= 0xEF) {
        if (at + 2 >= limit || !isContinuation(value[at + 1]) || !isContinuation(value[at + 2])) {
          return false;
        }
        final int second = value[at + 1] & 0xFF;
        if (lead == 0xE0 && second < 0xA0 || lead == 0xED && second > 0x9F) {
          return false;
        }
        at += 3;
      } else if (lead >= 0xF0 && lead <= 0xF4) {
        if (at + 3 >= limit || !isContinuation(value[at + 1]) || !isContinuation(value[at + 2])
            || !isContinuation(value[at + 3])) {
          return false;
        }
        final int second = value[at + 1] & 0xFF;
        if (lead == 0xF0 && second < 0x90 || lead == 0xF4 && second > 0x8F) {
          return false;
        }
        at += 4;
      } else {
        return false;
      }
    }
    return true;
  }

  private static boolean isContinuation(final byte value) {
    return (value & 0xC0) == 0x80;
  }

  private static void writeInt(final byte[] target, final int offset, final int value) {
    target[offset] = (byte) (value >>> 24);
    target[offset + 1] = (byte) (value >>> 16);
    target[offset + 2] = (byte) (value >>> 8);
    target[offset + 3] = (byte) value;
  }

  private static int readInt(final byte[] source, final int offset) {
    return (source[offset] & 0xFF) << 24 | (source[offset + 1] & 0xFF) << 16 | (source[offset + 2] & 0xFF) << 8
        | source[offset + 3] & 0xFF;
  }

  private static void deleteRecursively(final Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (var walk = Files.walk(root)) {
      for (final Path path : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }
}
