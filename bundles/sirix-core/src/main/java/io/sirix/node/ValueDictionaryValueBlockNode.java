/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;

import java.util.Arrays;

/**
 * One bounded, immutable, consecutively packed sub-block of a reverse value bucket.
 *
 * <h2>Why sub-blocks and not one blob per bucket</h2>
 *
 * A reverse bucket spans 256 ids. Storing one record per id is what makes a high-cardinality scan
 * decode a record — and allocate a value array — for every row. Storing the whole bucket as ONE
 * capped blob does not fix it either: at a 64 KiB cap roughly sixty ordinary 1 KiB values fill it,
 * and every later value in that bucket falls back to its own record, which reintroduces the per-row
 * decode on exactly the columns that matter most.
 *
 * <p>
 * So a bucket holds AS MANY sub-blocks as its values need. A sub-block closes when appending the
 * next value would exceed {@link #MAX_BLOCK_BYTES}; the next value opens a new one. An ordinary
 * value therefore never spills — only a value whose own length exceeds the target does, and that is
 * genuine overflow rather than an artefact of where a boundary happened to fall.
 *
 * <h2>Immutability and incremental append</h2>
 *
 * A closed sub-block is never rewritten. An append rewrites only the TAIL sub-block (the one still
 * open) plus its bucket's small directory, so the copy-on-write cost of appending is bounded by
 * {@link #MAX_BLOCK_BYTES} rather than by the bucket's total size — which is what keeps the
 * dictionary's append-only, no-rebuild contract under every versioning cadence.
 *
 * <h2>Access</h2>
 *
 * Values are laid out ascending by id with a prefix-offset table, so resolving an id is an index
 * into {@link #offsets} and a slice of {@link #bytes} — no per-id record and no per-id array. The
 * bytes are exposed for READ-ONLY slicing; callers must not mutate them.
 */
public final class ValueDictionaryValueBlockNode implements DataRecord {

  /**
   * Byte target a sub-block is allowed to reach. Bounded so that appending rewrites a small record
   * and so that one decoded block is a bounded retention unit for a read view.
   */
  public static final int MAX_BLOCK_BYTES = 1 << 16;

  /** Values a single sub-block may hold; a bucket spans 256 ids, so no block can exceed that. */
  public static final int MAX_BLOCK_VALUES = ValueDictionaryValueBucketNode.VALUES_PER_BUCKET;

  private final long nodeKey;
  private final int firstId;
  private final int[] offsets;
  private final byte[] bytes;

  /**
   * Defensively COPIES both arrays. Use {@link #takeOwnership} on the ingestion path, where the
   * arrays are built for the record and copying them would move the allocation this record exists to
   * remove from the read side to the write side.
   *
   * @param nodeKey this record's key
   * @param firstId dictionary id of the block's first value
   * @param offsets prefix offsets into {@code bytes}; length is {@code count + 1}, ascending,
   *        starting at {@code 0} and ending at {@code bytes.length}
   * @param bytes the packed UTF-8 values, ascending by id
   */
  public ValueDictionaryValueBlockNode(final long nodeKey, final int firstId, final int[] offsets, final byte[] bytes) {
    this(nodeKey, firstId, requireCopy(offsets), requireCopy(bytes), false);
  }

  /**
   * ADOPTS both arrays — the caller must not retain or mutate them.
   *
   * <p>
   * For the writer and the codec, which build these arrays for this record and drop them. The public
   * constructor copies, so ownership transfer is always an explicit, named choice rather than a
   * property of which constructor you happened to call.
   */
  public static ValueDictionaryValueBlockNode takeOwnership(final long nodeKey, final int firstId, final int[] offsets,
      final byte[] bytes) {
    return new ValueDictionaryValueBlockNode(nodeKey, firstId, offsets, bytes, false);
  }

  private static int[] requireCopy(final int[] values) {
    if (values == null) {
      throw new IllegalArgumentException("offsets must not be null");
    }
    return values.clone();
  }

  private static byte[] requireCopy(final byte[] values) {
    if (values == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    return values.clone();
  }

  private ValueDictionaryValueBlockNode(final long nodeKey, final int firstId, final int[] offsets, final byte[] bytes,
      final boolean unusedOwnershipMarker) {
    if (nodeKey <= 0) {
      throw new IllegalArgumentException("value dictionary value block key must be positive");
    }
    if (firstId <= 0) {
      throw new IllegalArgumentException("value dictionary value block first id must be positive");
    }
    if (offsets == null || bytes == null || offsets.length < 2 || offsets.length - 1 > MAX_BLOCK_VALUES) {
      throw new IllegalArgumentException("invalid value dictionary value block shape");
    }
    if (offsets[0] != 0 || offsets[offsets.length - 1] != bytes.length) {
      throw new IllegalArgumentException("value dictionary value block offsets must span its bytes exactly");
    }
    for (int i = 1; i < offsets.length; i++) {
      if (offsets[i] < offsets[i - 1]) {
        throw new IllegalArgumentException("value dictionary value block offsets must be ascending");
      }
    }
    if (bytes.length > MAX_BLOCK_BYTES) {
      throw new IllegalArgumentException(
          "value dictionary value block of " + bytes.length + " bytes exceeds " + MAX_BLOCK_BYTES);
    }
    // LAST id, not end-exclusive: a one-value block at Integer.MAX_VALUE is legal, and comparing the
    // exclusive end against MAX_VALUE rejected it. NodeKind's decode guard already used this form,
    // so the two disagreed about a boundary block that the codec would happily produce.
    if ((long) firstId + (offsets.length - 1) - 1L > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("value dictionary value block overruns the id space");
    }
    this.nodeKey = nodeKey;
    this.firstId = firstId;
    this.offsets = offsets;
    this.bytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_VALUE_BLOCK;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  /** Dictionary id of this block's first value. */
  public int getFirstId() {
    return firstId;
  }

  /** Values packed here. */
  public int size() {
    return offsets.length - 1;
  }

  /** Whether {@code id} is packed in this block. */
  public boolean covers(final int id) {
    // Long arithmetic: a block whose last id is Integer.MAX_VALUE would wrap `firstId + size()`
    // negative in int and report covering nothing.
    return id >= firstId && (long) id < (long) firstId + size();
  }

  /** Start of {@code id}'s value within {@link #rawBytes()}. */
  public int valueOffset(final int id) {
    return offsets[checkedIndex(id)];
  }

  /** Length of {@code id}'s value within {@link #rawBytes()}. */
  public int valueLength(final int id) {
    final int index = checkedIndex(id);
    return offsets[index + 1] - offsets[index];
  }

  /**
   * RAW read-only view of the packed bytes. Never copied on access — copying here would restore the
   * per-id allocation this record exists to remove. Callers slice with {@link #valueOffset} and
   * {@link #valueLength} and MUST NOT mutate the array.
   */
  public byte[] rawBytes() {
    return bytes;
  }

  /**
   * Prefix offset {@code index}, without materialising the table. The codec walks this rather than
   * taking a copy: a block is written once per tail-block append during ingestion, and cloning an
   * {@code int[]} there is a pure allocation with nothing bought.
   *
   * @param index {@code 0 .. size()}
   * @return the offset into {@link #rawBytes()}
   */
  public int offsetAt(final int index) {
    return offsets[index];
  }

  /**
   * The full offset table, COPIED.
   *
   * <p>
   * For the append path, which copies an open tail block's offsets into its replacement. Per-value
   * reads use {@link #offsetAt} and never materialise this.
   */
  public int[] copyOffsets() {
    return offsets.clone();
  }

  private int checkedIndex(final int id) {
    // `id - firstId` in int wraps for an extreme negative id and can land INSIDE the valid range, so
    // the difference is taken in long and only narrowed once it is known to be in bounds.
    final long index = (long) id - firstId;
    if (index < 0L || index >= size()) {
      throw new IllegalArgumentException(
          "id " + id + " is outside value block [" + firstId + ", " + ((long) firstId + size()) + ")");
    }
    return (int) index;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public boolean equals(final Object object) {
    return object instanceof ValueDictionaryValueBlockNode other && nodeKey == other.nodeKey && firstId == other.firstId
        && Arrays.equals(offsets, other.offsets) && Arrays.equals(bytes, other.bytes);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(nodeKey);
    result = 31 * result + firstId;
    result = 31 * result + Arrays.hashCode(offsets);
    return 31 * result + Arrays.hashCode(bytes);
  }
}
