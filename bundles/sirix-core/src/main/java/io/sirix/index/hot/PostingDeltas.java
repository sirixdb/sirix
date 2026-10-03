package io.sirix.index.hot;

import io.sirix.page.HOTLeafPage;
import java.util.Objects;

/**
 * Append-only posting deltas for the chunked Roaring posting lists of CAS and VALIDTIME HOT indexes
 * (index-format extension, 2026-09-28).
 *
 * <p>
 * A logical key's postings are stored as one slot per 65,536-node-key chunk under the composite key
 * {@code C = prefix ‖ chunkIdx_be4}. Every insert or removal on a popular key used to rewrite that
 * chunk's whole payload (up to an 8 KB Roaring container) and, under record-level versioning,
 * re-emit it with the leaf's carry-forward. A <em>delta slot</em> instead records one added or
 * removed node key of a hot chunk as its own tiny entry under the key {@code C ‖ suffix_be4} with
 * {@code suffix = DELTA_FLAG | seq << 1 | remove} and the ordinary single-bit posting payload. A
 * delta key extends its chunk's composite key, so it sorts immediately after the chunk and before
 * the next chunk of the same prefix, never shortens a leaf's common prefix, and every reader that
 * walks a prefix's composite range meets each base chunk first and then its deltas in sequence
 * order. Once a chunk has {@link #FOLD_BOUND} live deltas, the writer folds them into the base
 * chunk and tombstones them; a reader at an older revision still sees the older base plus its live
 * deltas, so postings are identical at every revision.
 * </p>
 *
 * <p>
 * This is the CAS/VALIDTIME chunk key format. Base chunks and delta slots share the ordinary
 * NodeReferences payload encoding. Logical keys are prefix-free: CAS values are escaped and
 * terminated; VALIDTIME keys have fixed width. A reader identifies slots by the logical prefix
 * length plus the chunk trailer and optional suffix, never by the high bit of a chunk index. Chunks
 * with the high trailer bit set use direct chunk updates.
 * </p>
 */
public final class PostingDeltas {

  /** Top bit of the 4-byte delta suffix; a chunk trailer never has it for node keys below 2^47. */
  public static final int DELTA_FLAG = 0x80000000;

  /** Chunk indices that may carry deltas: those whose trailer has the top bit clear. */
  public static final int MAX_CHUNK_IDX = 0x7FFFFFFF;

  /** Bytes of a delta suffix appended to the chunk's composite key. */
  public static final int SUFFIX_BYTES = 4;

  /** Largest delta sequence number: 7 bits. */
  public static final int MAX_SEQ = 127;

  /** Live deltas of one chunk are folded into the chunk when this many exist. */
  public static final int FOLD_BOUND = 64;

  /**
   * A chunk payload at least this long makes its postings "hot": further changes are written as
   * deltas.
   */
  public static final int HOT_CHUNK_BYTES = 256;

  /** Side-map sub-id of a referenced posting chunk (projection segments use their column ids). */
  public static final int REFERENCE_SUB_ID = 1;

  /**
   * The side-map key under which a chunk's folded payload is referenced: a 47-bit FNV-1a hash of the
   * composite key in the leaf's side-map key convention. Uniqueness is needed only within one leaf;
   * the writer detects the rare collision there and keeps that chunk inline.
   */
  public static long referenceKey(final byte[] key, final int length) {
    Objects.checkFromIndexSize(0, length, key.length);
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < length; i++) {
      hash ^= key[i] & 0xFF;
      hash *= 0x100000001b3L;
    }
    hash ^= hash >>> 29;
    return HOTLeafPage.overflowPageRefKey(hash & ((1L << 47) - 1), REFERENCE_SUB_ID);
  }

  private PostingDeltas() {
    throw new AssertionError("no instances");
  }

  /**
   * @return whether a 4-byte value read as unsigned is a delta suffix (top bit set); on a chunk
   *         trailer the same test says whether the chunk may carry deltas at all (it must be clear)
   */
  public static boolean isDelta(final long suffixOrTrailer) {
    return (suffixOrTrailer & 0x80000000L) != 0L;
  }

  /** Build a delta suffix. */
  public static int suffix(final int seq, final boolean remove) {
    if (seq < 0 || seq > MAX_SEQ) {
      throw new IllegalArgumentException("seq out of delta range: " + seq);
    }
    return DELTA_FLAG | (seq << 1) | (remove
        ? 1
        : 0);
  }

  /** @return the sequence number of a delta suffix */
  public static int seq(final long suffix) {
    return (int) ((suffix >>> 1) & MAX_SEQ);
  }

  /** @return whether a delta suffix records a removal */
  public static boolean isRemove(final long suffix) {
    return (suffix & 1L) != 0L;
  }

}
