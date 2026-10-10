package io.sirix.index.hot;

import io.sirix.page.HOTLeafPage;
import java.util.Objects;

/**
 * Append-only posting deltas for the chunked Roaring posting lists of CAS and VALIDTIME HOT indexes
 * (index-format extension, 2026-09-28). The stored layout and fold policy are specified in
 * docs/DISK_FORMAT.md, "CAS and VALIDTIME posting chunks".
 *
 * <p>
 * A delta key extends its chunk's composite key, preserving the leaf's common prefix and sorting
 * after its base but before the next chunk. Repurposing a chunk trailer would break that ordering
 * and let a prefix scan miss postings. Readers apply each base before its chronological deltas;
 * older revisions retain their own base and live deltas after a fold.
 * </p>
 *
 * <p>
 * Readers identify slots using the serializer's logical boundary, never the high bit of a chunk
 * index: the full unsigned chunk-index range must remain readable. Only eligible chunks append
 * deltas; others retain direct chunk updates.
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

  /**
   * Effective changes per fold, including the operation applied directly during the fold. A short
   * window limits the physical delta slots and leaf fragments read for hot posting chunks.
   */
  public static final int FOLD_BOUND = 16;

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
   * Test the delta flag independently of the caller's logical-boundary check.
   *
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

  /** Read the sequence number of a delta suffix. */
  public static int seq(final long suffix) {
    return (int) ((suffix >>> 1) & MAX_SEQ);
  }

  /** Test whether a delta suffix records a removal. */
  public static boolean isRemove(final long suffix) {
    return (suffix & 1L) != 0L;
  }

}
