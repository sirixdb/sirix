/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * One chunk's projection rows, extracted WHERE THE DATA IS: in the parallel importer's build
 * worker, from the same primitives the worker writes into page bytes, instead of read back
 * node-by-node through the transaction afterwards. The worker feeds this batch as it builds; the
 * coordinator replays the rows into the armed {@link ProjectionBulkLoad}'s streaming builder in
 * document order at adoption.
 *
 * <h2>Semantics contract</h2> Cell classification reproduces {@link ProjectionIndexRowExtractor}
 * exactly — same field matching (by PATH CLASS, the {@code pathNodeKey} every named node carries),
 * same presence/poison discipline, same numeric provenance flags, via the same shared
 * classification helpers. The one intentional divergence: the extractor's work-list DFS visits
 * nested sibling subtrees in REVERSE order, while a worker streams document order. For any field
 * matched at most once per record — every corpus the gates cover — the two are indistinguishable. A
 * duplicate match poisons the cell identically in both orders; only the residual VALUE bytes stored
 * for such a poisoned cell (and the element order of a set column fed by several arrays of one
 * record) can differ, and consumers never read either through a poisoned cell.
 *
 * <h2>Memory discipline</h2> All row-indexed storage is allocated ONCE per chunk, pre-sized from
 * the feeder's member count: flat flag bytes, per-numeric-column long lanes, and per-row offsets
 * into fixed 64 KiB UTF-8 arena chunks. Every array payload is bounded to 256 KiB; the per-record
 * hot path only adds another fixed-size arena chunk when a string crosses a chunk boundary. Only
 * set columns (absent from typical corpora) allocate per row, mirroring the read-back extractor's
 * own trim allocation.
 */
public final class ProjectionChunkRowBatch {

  private static final int MAX_SAFE_ARRAY_PAYLOAD_BYTES = 256 << 10;
  private static final int MAX_SAFE_REFERENCE_ARRAY_LENGTH = MAX_SAFE_ARRAY_PAYLOAD_BYTES / Long.BYTES;
  private static final int STRING_ARENA_CHUNK_BYTES = 64 << 10;
  private static final int INITIAL_STRING_ARENA_CHUNKS = 4;

  private static final int FLAG_PRESENT = 1;
  private static final int FLAG_UNREPRESENTABLE = 1 << 1;
  private static final int FLAG_NON_INTEGRAL = 1 << 2;
  private static final int FLAG_NON_DOUBLE_SOURCE = 1 << 3;
  private static final int FLAG_BOOLEAN_VALUE = 1 << 4;

  private static final long NO_OPEN_ARRAY = Long.MIN_VALUE;
  private static final String[] EMPTY_ELEMENTS = new String[0];

  /**
   * Field mapping snapshot: the extractor's (pathNodeKey → column) pairs at chunk-dispatch time.
   * {@link ProjectionIndexRowExtractor#resolveFieldPcrs} REPLACES its arrays on refresh, so these
   * references are immutable snapshots by construction — no clone needed.
   */
  private final long[] fieldPcrKeys;
  private final int[] fieldPcrColumns;
  private final byte[] columnKinds;

  /** Primary (pathNodeKey → mapping index); {@link #extraMappings} carries rare duplicate PCRs. */
  private final Long2IntOpenHashMap firstMappingByPcr;
  private final Long2ObjectOpenHashMap<int[]> extraMappings;

  private final int expectedRows;
  private final long recordSetKey;

  /** Root node key of each record row, for the coordinator's independent cross-check. */
  private final long[] recordRootKeys;

  /** Flat column-major flags: {@code flags[column * expectedRows + row]}. */
  private final byte[] flags;

  /** Numeric/boolean-encoded lanes, allocated only for numeric column kinds. */
  private final long[][] longLanes;

  /** String lanes: fixed-size arena chunks plus per-row offsets/lengths; length -1 = no value. */
  private final byte[][][] stringArenaChunks;
  private final int[][] stringOffsets;
  private final int[][] stringLengths;
  private final int[] stringArenaUsed;
  private final int[] stringArenaAllocatedChunks;

  /** Set lanes (rare): trimmed elements per row, plus the per-record open-collection state. */
  private final String[][][] setElements;
  private final String[][] setScratch;
  private final int[] setScratchLength;
  private final long[] openSetArrayKey;
  private final boolean hasSetColumns;

  private int rowCount = -1;
  private boolean finishedBuild;

  ProjectionChunkRowBatch(final long[] fieldPcrKeys, final int[] fieldPcrColumns, final byte[] columnKinds,
      final int expectedRows, final long recordSetKey) {
    if (expectedRows < 0) {
      throw new IllegalArgumentException("expectedRows must be non-negative: " + expectedRows);
    }
    final int maxRows = maxHftChunkRows(columnKinds.length);
    if (expectedRows > maxRows) {
      throw new IllegalArgumentException("expectedRows " + expectedRows + " exceeds the HFT-safe chunk limit " + maxRows
          + " for " + columnKinds.length + " projection columns");
    }
    this.fieldPcrKeys = fieldPcrKeys;
    this.fieldPcrColumns = fieldPcrColumns;
    this.columnKinds = columnKinds;
    this.expectedRows = expectedRows;
    this.recordSetKey = recordSetKey;
    this.recordRootKeys = new long[expectedRows];
    final int columns = columnKinds.length;
    this.flags = new byte[Math.multiplyExact(columns, expectedRows)];
    this.longLanes = new long[columns][];
    this.stringArenaChunks = new byte[columns][][];
    this.stringOffsets = new int[columns][];
    this.stringLengths = new int[columns][];
    this.stringArenaUsed = new int[columns];
    this.stringArenaAllocatedChunks = new int[columns];
    this.setElements = new String[columns][][];
    this.setScratch = new String[columns][];
    this.setScratchLength = new int[columns];
    this.openSetArrayKey = new long[columns];
    boolean anySet = false;
    for (int column = 0; column < columns; column++) {
      switch (columnKinds[column]) {
        // The temporal kinds hold an epoch per row: the long lane, allocated exactly as for a
        // numeric column, and no string arena at all.
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          longLanes[column] = new long[expectedRows];
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> {
          stringArenaChunks[column] = new byte[INITIAL_STRING_ARENA_CHUNKS][];
          stringOffsets[column] = new int[expectedRows];
          final int[] lengths = new int[expectedRows];
          Arrays.fill(lengths, -1);
          stringLengths[column] = lengths;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          setElements[column] = new String[expectedRows][];
          anySet = true;
        }
        default -> {
          // BOOLEAN needs only its flag bit.
        }
      }
      openSetArrayKey[column] = NO_OPEN_ARRAY;
    }
    this.hasSetColumns = anySet;

    this.firstMappingByPcr = new Long2IntOpenHashMap(Math.max(4, fieldPcrKeys.length * 2));
    this.firstMappingByPcr.defaultReturnValue(-1);
    Long2ObjectOpenHashMap<int[]> extras = null;
    for (int mapping = 0; mapping < fieldPcrKeys.length; mapping++) {
      final long pcr = fieldPcrKeys[mapping];
      if (firstMappingByPcr.putIfAbsent(pcr, mapping) >= 0) {
        if (extras == null) {
          extras = new Long2ObjectOpenHashMap<>(4);
        }
        final int[] existing = extras.get(pcr);
        if (existing == null) {
          extras.put(pcr, new int[] {mapping});
        } else {
          final int[] grown = Arrays.copyOf(existing, existing.length + 1);
          grown[existing.length] = mapping;
          extras.put(pcr, grown);
        }
      }
    }
    this.extraMappings = extras == null
        ? new Long2ObjectOpenHashMap<>(0)
        : extras;
  }

  // ==== worker feed (document order, single builder thread per chunk) ==========================

  /** Whether this batch has any set column — lets the worker skip child-value calls entirely. */
  public boolean trackChildValues() {
    return hasSetColumns;
  }

  /** The record-set container whose direct children are this batch's records. */
  public long recordSetKey() {
    return recordSetKey;
  }

  /** A new record root was minted; every following feed call belongs to it. */
  public void beginRecord(final long recordRootKey) {
    closeOpenRecord();
    final int row = rowCount + 1;
    if (row >= expectedRows) {
      throw new IllegalStateException("chunk projection batch overflowed its " + expectedRows
          + " reserved rows at record root " + recordRootKey + " — the feeder's member count and the build disagree");
    }
    rowCount = row;
    recordRootKeys[row] = recordRootKey;
  }

  public void onNamedNumber(final long pathNodeKey, final Number value) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int column = fieldPcrColumns[mapping];
      final int cell = cellOf(column);
      final byte cellFlags = flags[cell];
      if ((cellFlags & FLAG_PRESENT) != 0) {
        // A second scalar matching the same declared field: no sequence semantics — poison.
        flags[cell] = (byte) (cellFlags | FLAG_UNREPRESENTABLE);
        continue;
      }
      final byte columnKind = columnKinds[column];
      if (!ProjectionIndexRowGroupPage.isNumericKind(columnKind) || value == null) {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        byte updated = (byte) (cellFlags | FLAG_PRESENT);
        if (ProjectionIndexRowExtractor.isNonIntegral(value)
            || ProjectionIndexRowExtractor.isLossyLongConversion(value)) {
          updated |= FLAG_NON_INTEGRAL;
        }
        flags[cell] = updated;
        longLanes[column][rowCount] = value.longValue();
      } else {
        final double doubleValue = value.doubleValue();
        if (!Double.isFinite(doubleValue)) {
          flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
        } else {
          byte updated = (byte) (cellFlags | FLAG_PRESENT);
          if (ProjectionIndexRowExtractor.isLossyDoubleConversion(value, doubleValue)) {
            updated |= FLAG_NON_INTEGRAL;
          }
          if (!(value instanceof Double)) {
            updated |= FLAG_NON_DOUBLE_SOURCE;
          }
          flags[cell] = updated;
          longLanes[column][rowCount] = ProjectionDoubleEncoding.encode(doubleValue);
        }
      }
    }
  }

  /** Feed an integral value without allocating an {@link Integer} wrapper. */
  public void onNamedInt(final long pathNodeKey, final int value) {
    onNamedIntegral(pathNodeKey, value, false);
  }

  /** Feed an integral value without allocating a {@link Long} wrapper. */
  public void onNamedLong(final long pathNodeKey, final long value) {
    onNamedIntegral(pathNodeKey, value, true);
  }

  private void onNamedIntegral(final long pathNodeKey, final long value, final boolean longSource) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int column = fieldPcrColumns[mapping];
      final int cell = cellOf(column);
      final byte cellFlags = flags[cell];
      if ((cellFlags & FLAG_PRESENT) != 0) {
        flags[cell] = (byte) (cellFlags | FLAG_UNREPRESENTABLE);
        continue;
      }
      final byte columnKind = columnKinds[column];
      if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT);
        longLanes[column][rowCount] = value;
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        final double doubleValue = value;
        byte updated = (byte) (cellFlags | FLAG_PRESENT | FLAG_NON_DOUBLE_SOURCE);
        if (longSource && (value == Long.MAX_VALUE || (long) doubleValue != value)) {
          updated |= FLAG_NON_INTEGRAL;
        }
        flags[cell] = updated;
        longLanes[column][rowCount] = ProjectionDoubleEncoding.encode(doubleValue);
      } else {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      }
    }
  }

  public void onNamedString(final long pathNodeKey, final byte[] utf8, final int length) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int column = fieldPcrColumns[mapping];
      final int cell = cellOf(column);
      final byte cellFlags = flags[cell];
      if ((cellFlags & FLAG_PRESENT) != 0) {
        flags[cell] = (byte) (cellFlags | FLAG_UNREPRESENTABLE);
        continue;
      }
      final byte columnKind = columnKinds[column];
      if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT);
        storeString(column, utf8, length);
      } else if (ProjectionIndexRowGroupPage.isTemporalKind(columnKind)) {
        // A declared temporal column converts the feed's bytes straight into its epoch lane — no
        // arena copy, no dictionary, and a value that is not exactly canonical fails the LOAD,
        // where the record that carries it is still identifiable.
        final long epoch = ProjectionTemporalCodec.parse(columnKind, utf8, 0, length);
        if (epoch == ProjectionTemporalCodec.NOT_CANONICAL) {
          throw ProjectionTemporalCodec.notCanonical(columnKind, column, utf8, 0, length);
        }
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT);
        longLanes[column][rowCount] = epoch;
      } else {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      }
    }
  }

  public void onNamedBoolean(final long pathNodeKey, final boolean value) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int column = fieldPcrColumns[mapping];
      final int cell = cellOf(column);
      final byte cellFlags = flags[cell];
      if ((cellFlags & FLAG_PRESENT) != 0) {
        flags[cell] = (byte) (cellFlags | FLAG_UNREPRESENTABLE);
        continue;
      }
      if (columnKinds[column] == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | (value
            ? FLAG_BOOLEAN_VALUE
            : 0));
      } else {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      }
    }
  }

  /** A fused null field: present, and no column kind can represent it. */
  public void onNamedNull(final long pathNodeKey) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int cell = cellOf(fieldPcrColumns[mapping]);
      final byte cellFlags = flags[cell];
      if ((cellFlags & FLAG_PRESENT) != 0) {
        flags[cell] = (byte) (cellFlags | FLAG_UNREPRESENTABLE);
      } else {
        flags[cell] = (byte) (cellFlags | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      }
    }
  }

  /** An object-valued field: present but unrepresentable for every scalar column kind. */
  public void onNamedObject(final long pathNodeKey) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int cell = cellOf(fieldPcrColumns[mapping]);
      // The extractor's container branch has no first-match guard: present and poisoned, always.
      flags[cell] = (byte) (flags[cell] | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
    }
  }

  /** An array-valued field: a SET column starts collecting its elements; every other kind poisons. */
  public void onNamedArray(final long pathNodeKey, final long arrayNodeKey) {
    for (int mapping = firstMapping(pathNodeKey); mapping >= 0; mapping = nextMapping(pathNodeKey, mapping)) {
      final int column = fieldPcrColumns[mapping];
      final int cell = cellOf(column);
      if (columnKinds[column] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        // No first-match guard, mirroring collectStringSet: several arrays of one record matching
        // the same set column ACCUMULATE elements.
        flags[cell] = (byte) (flags[cell] | FLAG_PRESENT);
        openSetArrayKey[column] = arrayNodeKey;
      } else {
        flags[cell] = (byte) (flags[cell] | FLAG_PRESENT | FLAG_UNREPRESENTABLE);
      }
    }
  }

  /** A plain string value node; collected when its parent is a set column's open array. */
  public void onChildValueString(final long parentKey, final byte[] utf8, final int length) {
    if (!hasSetColumns) {
      return;
    }
    for (int column = 0; column < columnKinds.length; column++) {
      if (openSetArrayKey[column] != parentKey) {
        continue;
      }
      final int cell = cellOf(column);
      if ((flags[cell] & FLAG_UNREPRESENTABLE) != 0) {
        continue;
      }
      final int elementCount = setScratchLength[column];
      if (elementCount == ProjectionIndexRowExtractor.MAX_STRING_SET_ELEMENTS_PER_ROW) {
        flags[cell] = (byte) (flags[cell] | FLAG_UNREPRESENTABLE);
        continue;
      }
      String[] scratch = setScratch[column];
      if (scratch == null) {
        scratch = new String[8];
        setScratch[column] = scratch;
      } else if (elementCount == scratch.length) {
        scratch = Arrays.copyOf(scratch,
            Math.min(ProjectionIndexRowExtractor.MAX_STRING_SET_ELEMENTS_PER_ROW, elementCount * 2));
        setScratch[column] = scratch;
      }
      scratch[elementCount] = new String(utf8, 0, length, StandardCharsets.UTF_8);
      setScratchLength[column] = elementCount + 1;
    }
  }

  /** Any non-string child; poisons a set column whose open array is its parent. */
  public void onChildNonString(final long parentKey) {
    if (!hasSetColumns) {
      return;
    }
    for (int column = 0; column < columnKinds.length; column++) {
      if (openSetArrayKey[column] == parentKey) {
        flags[cellOf(column)] = (byte) (flags[cellOf(column)] | FLAG_UNREPRESENTABLE);
      }
    }
  }

  /** Close the batch: verify the build produced exactly the rows the feeder counted. */
  public void finishBuild() {
    closeOpenRecord();
    finishedBuild = true;
    final int rows = rowCount + 1;
    if (rows != expectedRows) {
      throw new IllegalStateException("chunk projection batch holds " + rows + " records but the feeder counted "
          + expectedRows + " members — a build/feeder divergence, and the projection would be short rows");
    }
  }

  private void closeOpenRecord() {
    if (rowCount < 0) {
      return;
    }
    if (hasSetColumns) {
      for (int column = 0; column < columnKinds.length; column++) {
        if (columnKinds[column] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          continue;
        }
        final int elementCount = setScratchLength[column];
        if (elementCount > 0 && (flags[cellOf(column)] & FLAG_UNREPRESENTABLE) == 0) {
          final String[] trimmed = new String[elementCount];
          System.arraycopy(setScratch[column], 0, trimmed, 0, elementCount);
          setElements[column][rowCount] = trimmed;
        }
        setScratchLength[column] = 0;
        openSetArrayKey[column] = NO_OPEN_ARRAY;
      }
    }
  }

  private void storeString(final int column, final byte[] utf8, final int length) {
    final int used = stringArenaUsed[column];
    if (length < 0 || used > Integer.MAX_VALUE - length) {
      throw new IllegalStateException(
          "string arena for projection column " + column + " exceeds the 2 GiB addressable limit");
    }
    final int required = used + length;
    ensureStringArenaCapacity(column, required);
    int sourceOffset = 0;
    int destinationOffset = used;
    int remaining = length;
    final byte[][] chunks = stringArenaChunks[column];
    while (remaining > 0) {
      final int chunkIndex = destinationOffset / STRING_ARENA_CHUNK_BYTES;
      final int chunkOffset = destinationOffset & (STRING_ARENA_CHUNK_BYTES - 1);
      final int copied = Math.min(remaining, STRING_ARENA_CHUNK_BYTES - chunkOffset);
      System.arraycopy(utf8, sourceOffset, chunks[chunkIndex], chunkOffset, copied);
      sourceOffset += copied;
      destinationOffset += copied;
      remaining -= copied;
    }
    stringOffsets[column][rowCount] = used;
    stringLengths[column][rowCount] = length;
    stringArenaUsed[column] = required;
  }

  private void ensureStringArenaCapacity(final int column, final int requiredBytes) {
    if (requiredBytes == 0) {
      return;
    }
    final int requiredChunks = 1 + (requiredBytes - 1) / STRING_ARENA_CHUNK_BYTES;
    byte[][] chunks = stringArenaChunks[column];
    if (requiredChunks > chunks.length) {
      int grown = chunks.length;
      while (grown < requiredChunks) {
        grown = Math.multiplyExact(grown, 2);
      }
      chunks = Arrays.copyOf(chunks, grown);
      stringArenaChunks[column] = chunks;
    }
    final int allocatedChunks = stringArenaAllocatedChunks[column];
    for (int chunk = allocatedChunks; chunk < requiredChunks; chunk++) {
      chunks[chunk] = new byte[STRING_ARENA_CHUNK_BYTES];
    }
    stringArenaAllocatedChunks[column] = requiredChunks;
  }

  private int cellOf(final int column) {
    final int row = rowCount;
    if (row < 0) {
      throw new IllegalStateException("a chunk projection batch was fed a field before its first record root");
    }
    return column * expectedRows + row;
  }

  private int firstMapping(final long pathNodeKey) {
    return firstMappingByPcr.get(pathNodeKey);
  }

  private int nextMapping(final long pathNodeKey, final int previousMapping) {
    final int[] extras = extraMappings.get(pathNodeKey);
    if (extras == null) {
      return -1;
    }
    for (final int mapping : extras) {
      if (mapping > previousMapping) {
        return mapping;
      }
    }
    return -1;
  }

  // ==== coordinator read side (after the build future resolves) ================================

  public int rowCount() {
    if (!finishedBuild) {
      throw new IllegalStateException("chunk projection batch read before its build finished");
    }
    return rowCount + 1;
  }

  public long recordRootAt(final int row) {
    return recordRootKeys[row];
  }

  byte[] columnKindsSnapshot() {
    return columnKinds;
  }

  boolean flagPresent(final int column, final int row) {
    return (flags[column * expectedRows + row] & FLAG_PRESENT) != 0;
  }

  boolean flagUnrepresentable(final int column, final int row) {
    return (flags[column * expectedRows + row] & FLAG_UNREPRESENTABLE) != 0;
  }

  boolean flagNonIntegral(final int column, final int row) {
    return (flags[column * expectedRows + row] & FLAG_NON_INTEGRAL) != 0;
  }

  boolean flagNonDoubleSource(final int column, final int row) {
    return (flags[column * expectedRows + row] & FLAG_NON_DOUBLE_SOURCE) != 0;
  }

  boolean booleanValue(final int column, final int row) {
    return (flags[column * expectedRows + row] & FLAG_BOOLEAN_VALUE) != 0;
  }

  long longValue(final int column, final int row) {
    return longLanes[column][row];
  }

  int stringLength(final int column, final int row) {
    return stringLengths[column][row];
  }

  void copyStringTo(final int column, final int row, final byte[] destination) {
    final int length = stringLengths[column][row];
    if (length < 0 || destination.length < length) {
      throw new IllegalArgumentException(
          "destination length " + destination.length + " cannot hold string length " + length);
    }
    int sourceOffset = stringOffsets[column][row];
    int destinationOffset = 0;
    int remaining = length;
    final byte[][] chunks = stringArenaChunks[column];
    while (remaining > 0) {
      final int chunkIndex = sourceOffset / STRING_ARENA_CHUNK_BYTES;
      final int chunkOffset = sourceOffset & (STRING_ARENA_CHUNK_BYTES - 1);
      final int copied = Math.min(remaining, STRING_ARENA_CHUNK_BYTES - chunkOffset);
      System.arraycopy(chunks[chunkIndex], chunkOffset, destination, destinationOffset, copied);
      sourceOffset += copied;
      destinationOffset += copied;
      remaining -= copied;
    }
  }

  static int maxHftChunkRows(final int columnCount) {
    if (columnCount < 0) {
      throw new IllegalArgumentException("columnCount must be non-negative: " + columnCount);
    }
    if (columnCount > MAX_SAFE_REFERENCE_ARRAY_LENGTH) {
      throw new IllegalArgumentException("columnCount " + columnCount + " exceeds the HFT-safe reference-array limit "
          + MAX_SAFE_REFERENCE_ARRAY_LENGTH);
    }
    return MAX_SAFE_ARRAY_PAYLOAD_BYTES / Math.max(Long.BYTES, columnCount);
  }

  static int stringArenaChunkBytes() {
    return STRING_ARENA_CHUNK_BYTES;
  }

  String[] setElementsAt(final int column, final int row) {
    final String[] elements = setElements[column][row];
    return elements == null
        ? EMPTY_ELEMENTS
        : elements;
  }
}
