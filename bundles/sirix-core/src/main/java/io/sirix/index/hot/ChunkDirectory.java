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

package io.sirix.index.hot;

import io.sirix.page.BitmapChunkPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Directory of bitmap chunks for a single index key.
 *
 * <p>
 * Maps document node key ranges to {@link BitmapChunkPage} references. Stored as the value in
 * {@link io.sirix.page.HOTLeafPage} entries.
 * </p>
 *
 * <p>
 * <b>Structure:</b>
 * </p>
 * 
 * <pre>
 * ChunkDirectory for indexKey "/root/items/item"
 * ├── Chunk 0: [0, 65536) → PageRef(key=1234)
 * ├── Chunk 1: [65536, 131072) → PageRef(key=1235)
 * └── Chunk 2: [131072, 196608) → PageRef(key=1236)
 * </pre>
 *
 * <p>
 * Chunks are created lazily as document node keys are added. The directory grows dynamically when a
 * new range is needed.
 * </p>
 *
 * @author Johannes Lichtenberger
 * @see BitmapChunkPage
 */
public final class ChunkDirectory {

  /** Initial capacity for chunk arrays. */
  private static final int INITIAL_CAPACITY = 4;

  /** Growth factor for array expansion. */
  private static final int GROWTH_FACTOR = 2;

  // ===== Chunk storage =====
  private int chunkCount;
  private int[] chunkIndices; // Sorted array of chunk indices
  private PageReference[] chunkRefs; // Corresponding page references

  // ===== Modification tracking =====
  private boolean modified;

  /**
   * Create a new empty ChunkDirectory.
   */
  public ChunkDirectory() {
    this.chunkCount = 0;
    this.chunkIndices = new int[INITIAL_CAPACITY];
    this.chunkRefs = new PageReference[INITIAL_CAPACITY];
    this.modified = false;
  }

  /**
   * Create a ChunkDirectory with pre-allocated arrays.
   *
   * @param chunkCount the number of chunks
   * @param chunkIndices the chunk indices (sorted)
   * @param chunkRefs the page references
   */
  ChunkDirectory(int chunkCount, int[] chunkIndices, PageReference[] chunkRefs) {
    if (chunkCount < 0) {
      throw new IllegalArgumentException("chunkCount must be non-negative: " + chunkCount);
    }
    if (chunkIndices.length < chunkCount || chunkRefs.length < chunkCount) {
      throw new IllegalArgumentException("Arrays too small for chunkCount: " + chunkCount);
    }
    this.chunkCount = chunkCount;
    this.chunkIndices = chunkIndices;
    this.chunkRefs = chunkRefs;
    this.modified = false;
  }

  // ===== Accessors =====

  /**
   * Get the number of chunks in this directory.
   *
   * @return the chunk count
   */
  public int chunkCount() {
    return chunkCount;
  }

  /**
   * Check if this directory is empty.
   *
   * @return true if empty
   */
  public boolean isEmpty() {
    return chunkCount == 0;
  }

  /**
   * Check if this directory has been modified since creation.
   *
   * @return true if modified
   */
  public boolean isModified() {
    return modified;
  }

  /**
   * Clear the modified flag.
   */
  public void clearModified() {
    this.modified = false;
  }

  /**
   * Get the chunk index at a position.
   *
   * @param position the position (0 to chunkCount-1)
   * @return the chunk index
   */
  public int getChunkIndex(int position) {
    Objects.checkIndex(position, chunkCount);
    return chunkIndices[position];
  }

  /**
   * Get all chunk indices.
   *
   * @return array of chunk indices (copy)
   */
  public int[] getChunkIndices() {
    return Arrays.copyOf(chunkIndices, chunkCount);
  }

  /**
   * Get the page reference at a position.
   *
   * @param position the position (0 to chunkCount-1)
   * @return the page reference
   */
  public PageReference getChunkRefAtPosition(int position) {
    Objects.checkIndex(position, chunkCount);
    return chunkRefs[position];
  }

  /**
   * Get all page references.
   *
   * @return array of page references (copy)
   */
  public PageReference[] getChunkRefs() {
    return Arrays.copyOf(chunkRefs, chunkCount);
  }

  // ===== Chunk lookup =====

  /**
   * Get the page reference for a specific chunk index.
   *
   * @param chunkIndex the chunk index
   * @return the page reference, or null if chunk doesn't exist
   */
  public @Nullable PageReference getChunkRef(int chunkIndex) {
    int pos = findPosition(chunkIndex);
    if (pos >= 0) {
      return chunkRefs[pos];
    }
    return null;
  }

  /**
   * Get or create the page reference for a chunk index. Creates a new reference if the chunk doesn't
   * exist.
   *
   * @param chunkIndex the chunk index
   * @return the page reference (never null)
   */
  public @NonNull PageReference getOrCreateChunkRef(int chunkIndex) {
    if (chunkIndex < 0) {
      throw new IllegalArgumentException("chunkIndex must be non-negative: " + chunkIndex);
    }

    int pos = findPosition(chunkIndex);
    if (pos >= 0) {
      return chunkRefs[pos];
    }

    // Insert new chunk at correct position
    int insertPos = -pos - 1;
    ensureCapacity(chunkCount + 1);

    // Shift elements to make room
    if (insertPos < chunkCount) {
      System.arraycopy(chunkIndices, insertPos, chunkIndices, insertPos + 1, chunkCount - insertPos);
      System.arraycopy(chunkRefs, insertPos, chunkRefs, insertPos + 1, chunkCount - insertPos);
    }

    // Insert new entry
    chunkIndices[insertPos] = chunkIndex;
    chunkRefs[insertPos] = new PageReference();
    chunkCount++;
    modified = true;

    return chunkRefs[insertPos];
  }

  /**
   * Set the page reference for a chunk index.
   *
   * @param chunkIndex the chunk index
   * @param ref the page reference
   */
  public void setChunkRef(int chunkIndex, @NonNull PageReference ref) {
    Objects.requireNonNull(ref, "ref must not be null");

    int pos = findPosition(chunkIndex);
    if (pos >= 0) {
      chunkRefs[pos] = ref;
    } else {
      // Insert new chunk
      int insertPos = -pos - 1;
      ensureCapacity(chunkCount + 1);

      if (insertPos < chunkCount) {
        System.arraycopy(chunkIndices, insertPos, chunkIndices, insertPos + 1, chunkCount - insertPos);
        System.arraycopy(chunkRefs, insertPos, chunkRefs, insertPos + 1, chunkCount - insertPos);
      }

      chunkIndices[insertPos] = chunkIndex;
      chunkRefs[insertPos] = ref;
      chunkCount++;
    }
    modified = true;
  }

  /**
   * Find the position of a chunk index using binary search.
   *
   * @param chunkIndex the chunk index to find
   * @return position if found (>= 0), or (-(insertion point) - 1) if not found
   */
  private int findPosition(int chunkIndex) {
    return Arrays.binarySearch(chunkIndices, 0, chunkCount, chunkIndex);
  }

  /**
   * Ensure the arrays have enough capacity.
   */
  private void ensureCapacity(int minCapacity) {
    if (chunkIndices.length < minCapacity) {
      int newCapacity = Math.max(chunkIndices.length * GROWTH_FACTOR, minCapacity);
      chunkIndices = Arrays.copyOf(chunkIndices, newCapacity);
      chunkRefs = Arrays.copyOf(chunkRefs, newCapacity);
    }
  }

  // ===== Utility methods =====

  /**
   * Get the chunk index for a document node key.
   *
   * @param documentNodeKey the document node key
   * @return the chunk index
   */
  public static int chunkIndexFor(long documentNodeKey) {
    return BitmapChunkPage.chunkIndexFor(documentNodeKey);
  }

  /**
   * Create a copy of this directory.
   *
   * @return a deep copy
   */
  public ChunkDirectory copy() {
    int[] newIndices = Arrays.copyOf(chunkIndices, chunkIndices.length);
    PageReference[] newRefs = new PageReference[chunkRefs.length];
    for (int i = 0; i < chunkCount; i++) {
      newRefs[i] = new PageReference(chunkRefs[i]);
    }
    ChunkDirectory copy = new ChunkDirectory(chunkCount, newIndices, newRefs);
    copy.modified = this.modified;
    return copy;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ChunkDirectory{chunks=[");
    for (int i = 0; i < chunkCount; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(chunkIndices[i]).append("->").append(chunkRefs[i]);
    }
    sb.append("], modified=").append(modified).append("}");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ChunkDirectory that = (ChunkDirectory) o;
    if (chunkCount != that.chunkCount)
      return false;
    for (int i = 0; i < chunkCount; i++) {
      if (chunkIndices[i] != that.chunkIndices[i])
        return false;
      if (!Objects.equals(chunkRefs[i], that.chunkRefs[i]))
        return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = chunkCount;
    for (int i = 0; i < chunkCount; i++) {
      result = 31 * result + chunkIndices[i];
      result = 31 * result + (chunkRefs[i] != null
          ? chunkRefs[i].hashCode()
          : 0);
    }
    return result;
  }
}

