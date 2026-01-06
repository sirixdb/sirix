/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.index.IndexType;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Bitmap chunk page for versioned NodeReferences storage in HOT indexes.
 *
 * <p>Each BitmapChunkPage stores a range of document node keys [rangeStart, rangeEnd)
 * as a Roaring64Bitmap. Supports both full mode (complete bitmap) and delta mode
 * (additions/removals from a base snapshot).</p>
 *
 * <p><b>Versioning Modes:</b></p>
 * <ul>
 *   <li><b>Full mode</b>: Complete bitmap for the range (used for snapshots)</li>
 *   <li><b>Delta mode</b>: Only additions and removals since base (for incremental)</li>
 * </ul>
 *
 * <p><b>Chunk Range:</b></p>
 * <pre>
 * CHUNK_SIZE = 65536 (64K document keys per chunk)
 * Chunk 0: [0, 65536)
 * Chunk 1: [65536, 131072)
 * Chunk N: [N * 65536, (N+1) * 65536)
 * </pre>
 *
 * @author Johannes Lichtenberger
 * @see io.sirix.index.hot.ChunkDirectory
 */
public final class BitmapChunkPage implements Page {

  /** Size of each chunk in document node keys (64K). */
  public static final long CHUNK_SIZE = 65536L;

  /** Flag bits for serialization. */
  private static final byte FLAG_IS_DELTA = 0x01;
  private static final byte FLAG_IS_DELETED = 0x02;

  // ===== Page Identity =====
  private final long pageKey;
  private final int revision;
  private final IndexType indexType;

  // ===== Chunk Identity =====
  private final long rangeStart;
  private final long rangeEnd;

  // ===== Data Storage =====
  // Full mode: complete bitmap (non-null when !isDelta && !isDeleted)
  private @Nullable Roaring64Bitmap bitmap;

  // Delta mode: additions and removals since base (non-null when isDelta)
  private @Nullable Roaring64Bitmap additions;
  private @Nullable Roaring64Bitmap removals;

  // Mode flags
  private final boolean isDelta;
  private final boolean isDeleted;

  /**
   * Create a new full (snapshot) BitmapChunkPage.
   *
   * @param pageKey    the page key
   * @param revision   the revision number
   * @param indexType  the index type
   * @param rangeStart start of document key range (inclusive)
   * @param rangeEnd   end of document key range (exclusive)
   * @param bitmap     the complete bitmap for this range
   * @return new full BitmapChunkPage
   */
  public static BitmapChunkPage createFull(long pageKey, int revision, IndexType indexType,
                                            long rangeStart, long rangeEnd,
                                            @NonNull Roaring64Bitmap bitmap) {
    Objects.requireNonNull(bitmap, "bitmap must not be null for full mode");
    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        bitmap, null, null, false, false);
  }

  /**
   * Create a new delta BitmapChunkPage.
   *
   * @param pageKey    the page key
   * @param revision   the revision number
   * @param indexType  the index type
   * @param rangeStart start of document key range (inclusive)
   * @param rangeEnd   end of document key range (exclusive)
   * @param additions  keys added since base
   * @param removals   keys removed since base
   * @return new delta BitmapChunkPage
   */
  public static BitmapChunkPage createDelta(long pageKey, int revision, IndexType indexType,
                                             long rangeStart, long rangeEnd,
                                             @NonNull Roaring64Bitmap additions,
                                             @NonNull Roaring64Bitmap removals) {
    Objects.requireNonNull(additions, "additions must not be null for delta mode");
    Objects.requireNonNull(removals, "removals must not be null for delta mode");
    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        null, additions, removals, true, false);
  }

  /**
   * Create an empty delta BitmapChunkPage for modification.
   *
   * @param pageKey    the page key
   * @param revision   the revision number
   * @param indexType  the index type
   * @param rangeStart start of document key range (inclusive)
   * @param rangeEnd   end of document key range (exclusive)
   * @return new empty delta BitmapChunkPage
   */
  public static BitmapChunkPage createEmptyDelta(long pageKey, int revision, IndexType indexType,
                                                  long rangeStart, long rangeEnd) {
    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        null, new Roaring64Bitmap(), new Roaring64Bitmap(), true, false);
  }

  /**
   * Create an empty full BitmapChunkPage.
   *
   * @param pageKey    the page key
   * @param revision   the revision number
   * @param indexType  the index type
   * @param rangeStart start of document key range (inclusive)
   * @param rangeEnd   end of document key range (exclusive)
   * @return new empty full BitmapChunkPage
   */
  public static BitmapChunkPage createEmptyFull(long pageKey, int revision, IndexType indexType,
                                                 long rangeStart, long rangeEnd) {
    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        new Roaring64Bitmap(), null, null, false, false);
  }

  /**
   * Create a tombstone BitmapChunkPage (marks entire chunk as deleted).
   *
   * @param pageKey    the page key
   * @param revision   the revision number
   * @param indexType  the index type
   * @param rangeStart start of document key range (inclusive)
   * @param rangeEnd   end of document key range (exclusive)
   * @return tombstone BitmapChunkPage
   */
  public static BitmapChunkPage createTombstone(long pageKey, int revision, IndexType indexType,
                                                 long rangeStart, long rangeEnd) {
    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        null, null, null, false, true);
  }

  /**
   * Private constructor.
   */
  private BitmapChunkPage(long pageKey, int revision, IndexType indexType,
                          long rangeStart, long rangeEnd,
                          @Nullable Roaring64Bitmap bitmap,
                          @Nullable Roaring64Bitmap additions,
                          @Nullable Roaring64Bitmap removals,
                          boolean isDelta, boolean isDeleted) {
    if (rangeStart < 0) {
      throw new IllegalArgumentException("rangeStart must be non-negative: " + rangeStart);
    }
    if (rangeEnd <= rangeStart) {
      throw new IllegalArgumentException("rangeEnd must be > rangeStart: " + rangeEnd + " <= " + rangeStart);
    }
    this.pageKey = pageKey;
    this.revision = revision;
    this.indexType = Objects.requireNonNull(indexType, "indexType must not be null");
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.bitmap = bitmap;
    this.additions = additions;
    this.removals = removals;
    this.isDelta = isDelta;
    this.isDeleted = isDeleted;
  }

  // ===== Static utility methods =====

  /**
   * Calculate the chunk index for a document node key.
   *
   * @param documentNodeKey the document node key
   * @return the chunk index
   */
  public static int chunkIndexFor(long documentNodeKey) {
    if (documentNodeKey < 0) {
      throw new IllegalArgumentException("documentNodeKey must be non-negative: " + documentNodeKey);
    }
    return (int) (documentNodeKey / CHUNK_SIZE);
  }

  /**
   * Calculate the range start for a chunk index.
   *
   * @param chunkIndex the chunk index
   * @return the range start
   */
  public static long chunkRangeStart(int chunkIndex) {
    if (chunkIndex < 0) {
      throw new IllegalArgumentException("chunkIndex must be non-negative: " + chunkIndex);
    }
    return (long) chunkIndex * CHUNK_SIZE;
  }

  /**
   * Calculate the range end for a chunk index.
   *
   * @param chunkIndex the chunk index
   * @return the range end (exclusive)
   */
  public static long chunkRangeEnd(int chunkIndex) {
    return chunkRangeStart(chunkIndex) + CHUNK_SIZE;
  }

  // ===== Accessors =====

  public long getPageKey() {
    return pageKey;
  }

  public int getRevision() {
    return revision;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public long getRangeStart() {
    return rangeStart;
  }

  public long getRangeEnd() {
    return rangeEnd;
  }

  public boolean isDelta() {
    return isDelta;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public boolean isFullSnapshot() {
    return !isDelta && !isDeleted && bitmap != null;
  }

  /**
   * Get the complete bitmap (only valid for full mode).
   *
   * @return the bitmap, or null if delta mode or deleted
   */
  public @Nullable Roaring64Bitmap getBitmap() {
    return bitmap;
  }

  /**
   * Get additions (only valid for delta mode).
   *
   * @return additions bitmap, or null if full mode
   */
  public @Nullable Roaring64Bitmap getAdditions() {
    return additions;
  }

  /**
   * Get removals (only valid for delta mode).
   *
   * @return removals bitmap, or null if full mode
   */
  public @Nullable Roaring64Bitmap getRemovals() {
    return removals;
  }

  // ===== Modification methods =====

  /**
   * Add a document node key to this chunk.
   * For full mode, adds to bitmap. For delta mode, adds to additions and removes from removals.
   *
   * @param documentNodeKey the key to add
   * @throws IllegalArgumentException if key is outside this chunk's range
   */
  public void addKey(long documentNodeKey) {
    validateKeyInRange(documentNodeKey);

    if (isDeleted) {
      throw new IllegalStateException("Cannot add to deleted chunk");
    }

    if (isDelta) {
      additions.add(documentNodeKey);
      removals.removeLong(documentNodeKey);
    } else {
      if (bitmap == null) {
        throw new IllegalStateException("Full mode chunk has null bitmap");
      }
      bitmap.add(documentNodeKey);
    }
  }

  /**
   * Remove a document node key from this chunk.
   * For full mode, removes from bitmap. For delta mode, adds to removals and removes from additions.
   *
   * @param documentNodeKey the key to remove
   * @throws IllegalArgumentException if key is outside this chunk's range
   */
  public void removeKey(long documentNodeKey) {
    validateKeyInRange(documentNodeKey);

    if (isDeleted) {
      return; // Already deleted, nothing to do
    }

    if (isDelta) {
      removals.add(documentNodeKey);
      additions.removeLong(documentNodeKey);
    } else {
      if (bitmap == null) {
        throw new IllegalStateException("Full mode chunk has null bitmap");
      }
      bitmap.removeLong(documentNodeKey);
    }
  }

  /**
   * Check if this chunk contains the given document node key.
   * For full mode, checks bitmap. For delta mode, returns false (need to combine first).
   *
   * @param documentNodeKey the key to check
   * @return true if contained (for full mode only)
   */
  public boolean containsKey(long documentNodeKey) {
    if (isDeleted) {
      return false;
    }
    if (isDelta) {
      throw new IllegalStateException("Cannot query delta chunk directly; combine with base first");
    }
    return bitmap != null && bitmap.contains(documentNodeKey);
  }

  private void validateKeyInRange(long documentNodeKey) {
    if (documentNodeKey < rangeStart || documentNodeKey >= rangeEnd) {
      throw new IllegalArgumentException(
          "Key " + documentNodeKey + " is outside range [" + rangeStart + ", " + rangeEnd + ")");
    }
  }

  // ===== Copy methods =====

  /**
   * Create a copy of this page as a full snapshot.
   * If this is a delta, the bitmap will be null (need to combine first).
   *
   * @param newRevision the new revision number
   * @return a copy as full snapshot
   */
  public BitmapChunkPage copyAsFull(int newRevision) {
    Roaring64Bitmap newBitmap = bitmap != null ? bitmap.clone() : new Roaring64Bitmap();
    return new BitmapChunkPage(pageKey, newRevision, indexType, rangeStart, rangeEnd,
        newBitmap, null, null, false, false);
  }

  /**
   * Create a copy of this page for a new revision.
   *
   * @param newRevision the new revision number
   * @return a copy of this page
   */
  public BitmapChunkPage copy(int newRevision) {
    return new BitmapChunkPage(
        pageKey, newRevision, indexType, rangeStart, rangeEnd,
        bitmap != null ? bitmap.clone() : null,
        additions != null ? additions.clone() : null,
        removals != null ? removals.clone() : null,
        isDelta, isDeleted
    );
  }

  // ===== Serialization =====

  /**
   * Serialize this page to a DataOutput.
   *
   * @param out the output stream
   * @throws IOException if an I/O error occurs
   */
  public void serialize(DataOutput out) throws IOException {
    // Flags
    byte flags = 0;
    if (isDelta) flags |= FLAG_IS_DELTA;
    if (isDeleted) flags |= FLAG_IS_DELETED;
    out.writeByte(flags);

    // Range
    out.writeLong(rangeStart);
    out.writeLong(rangeEnd);

    // Revision
    out.writeInt(revision);

    // Index type
    out.writeByte(indexType.ordinal());

    // Data based on mode
    if (!isDeleted) {
      if (isDelta) {
        // Serialize additions
        serializeBitmap(out, additions);
        // Serialize removals
        serializeBitmap(out, removals);
      } else {
        // Serialize full bitmap
        serializeBitmap(out, bitmap);
      }
    }
  }

  private void serializeBitmap(DataOutput out, @Nullable Roaring64Bitmap bmp) throws IOException {
    if (bmp == null) {
      out.writeInt(0);
      return;
    }
    bmp.runOptimize();
    int size = (int) bmp.serializedSizeInBytes();
    out.writeInt(size);
    if (size > 0) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      bmp.serialize(buffer);
      out.write(buffer.array(), 0, size);
    }
  }

  /**
   * Deserialize a BitmapChunkPage from a DataInput.
   *
   * @param in      the input stream
   * @param pageKey the page key
   * @return the deserialized page
   * @throws IOException if an I/O error occurs
   */
  public static BitmapChunkPage deserialize(DataInput in, long pageKey) throws IOException {
    // Flags
    byte flags = in.readByte();
    boolean isDelta = (flags & FLAG_IS_DELTA) != 0;
    boolean isDeleted = (flags & FLAG_IS_DELETED) != 0;

    // Range
    long rangeStart = in.readLong();
    long rangeEnd = in.readLong();

    // Revision
    int revision = in.readInt();

    // Index type
    int indexTypeOrdinal = in.readByte() & 0xFF;
    IndexType indexType = IndexType.values()[indexTypeOrdinal];

    // Data based on mode
    Roaring64Bitmap bitmap = null;
    Roaring64Bitmap additions = null;
    Roaring64Bitmap removals = null;

    if (!isDeleted) {
      if (isDelta) {
        additions = deserializeBitmap(in);
        removals = deserializeBitmap(in);
      } else {
        bitmap = deserializeBitmap(in);
      }
    }

    return new BitmapChunkPage(pageKey, revision, indexType, rangeStart, rangeEnd,
        bitmap, additions, removals, isDelta, isDeleted);
  }

  private static Roaring64Bitmap deserializeBitmap(DataInput in) throws IOException {
    int size = in.readInt();
    if (size == 0) {
      return new Roaring64Bitmap();
    }
    byte[] data = new byte[size];
    in.readFully(data);
    Roaring64Bitmap bmp = new Roaring64Bitmap();
    bmp.deserialize(ByteBuffer.wrap(data));
    return bmp;
  }

  // ===== Page interface =====

  @Override
  public List<PageReference> getReferences() {
    return Collections.emptyList();
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    return null; // BitmapChunkPage has no child references
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    return true; // No references to set
  }

  @Override
  public String toString() {
    return "BitmapChunkPage{" +
        "pageKey=" + pageKey +
        ", revision=" + revision +
        ", indexType=" + indexType +
        ", range=[" + rangeStart + ", " + rangeEnd + ")" +
        ", isDelta=" + isDelta +
        ", isDeleted=" + isDeleted +
        ", size=" + (bitmap != null ? bitmap.getLongCardinality() :
            (additions != null ? "+" + additions.getLongCardinality() + "/-" + removals.getLongCardinality() : 0)) +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BitmapChunkPage that = (BitmapChunkPage) o;
    return pageKey == that.pageKey &&
        revision == that.revision &&
        rangeStart == that.rangeStart &&
        rangeEnd == that.rangeEnd &&
        isDelta == that.isDelta &&
        isDeleted == that.isDeleted &&
        indexType == that.indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageKey, revision, indexType, rangeStart, rangeEnd, isDelta, isDeleted);
  }
}

