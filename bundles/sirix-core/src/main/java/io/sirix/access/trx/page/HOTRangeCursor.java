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

package io.sirix.access.trx.page;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * COW-compatible range cursor for HOT indexes using parent-based navigation.
 * 
 * <p>
 * Since sibling pointers are incompatible with COW (modifying one leaf would cascade COW to all
 * siblings), this cursor uses in-order trie traversal with a parent stack maintained by
 * {@link HOTTrieReader}.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>No sibling pointers (COW-compatible)</li>
 * <li>Guard management for page lifetime</li>
 * <li>Zero-copy key/value access via MemorySegment</li>
 * <li>Implements AutoCloseable for proper cleanup</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * try (HOTRangeCursor cursor = reader.range(rootRef, fromKey, toKey)) {
 *   while (cursor.hasNext()) {
 *     HOTRangeCursor.Entry entry = cursor.next();
 *     // Process entry.key() and entry.value()
 *   }
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTTrieReader
 * @see HOTLeafPage
 */
public final class HOTRangeCursor implements Iterator<HOTRangeCursor.Entry>, AutoCloseable {

  /**
   * An entry in the range cursor. Uses MemorySegment slices for zero-copy access.
   */
  public record Entry(MemorySegment key, MemorySegment value) {

    /**
     * Get the key as a byte array (copies data).
     */
    public byte[] keyBytes() {
      byte[] bytes = new byte[(int) key.byteSize()];
      MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, bytes, 0, bytes.length);
      return bytes;
    }

    /**
     * Get the value as a byte array (copies data).
     */
    public byte[] valueBytes() {
      byte[] bytes = new byte[(int) value.byteSize()];
      MemorySegment.copy(value, ValueLayout.JAVA_BYTE, 0, bytes, 0, bytes.length);
      return bytes;
    }
  }

  private final HOTTrieReader reader;
  private final PageReference rootRef;
  private final byte[] fromKey;
  private final byte[] toKey;

  // Current position
  private HOTLeafPage currentLeaf;
  private int currentIndex;
  private boolean exhausted = false;
  private boolean guardAcquired = false;

  // Pre-computed next entry (for hasNext/next pattern)
  private Entry nextEntry = null;

  /**
   * Create a new range cursor.
   *
   * @param reader the keyed trie reader
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive)
   * @param toKey the end key (inclusive)
   */
  HOTRangeCursor(@NonNull HOTTrieReader reader, @NonNull PageReference rootRef, byte[] fromKey, byte[] toKey) {
    this.reader = Objects.requireNonNull(reader);
    this.rootRef = Objects.requireNonNull(rootRef);
    this.fromKey = fromKey;
    this.toKey = toKey;

    // Initialize to first entry
    descendToFirstEntry();
  }

  /**
   * Descend to the first entry >= fromKey.
   */
  private void descendToFirstEntry() {
    if (fromKey == null) {
      // No lower bound - start at leftmost leaf
      currentLeaf = reader.navigateToLeftmostLeaf(rootRef);
      currentIndex = 0;
    } else {
      // Navigate to leaf containing fromKey
      currentLeaf = reader.navigateToLeaf(rootRef, fromKey);
      if (currentLeaf == null) {
        exhausted = true;
        return;
      }

      // Find first entry >= fromKey
      currentIndex = currentLeaf.findEntry(fromKey);
      if (currentIndex < 0) {
        // Not found - insertion point is where to start
        currentIndex = -(currentIndex + 1);
      }
    }

    if (currentLeaf != null) {
      acquireLeafGuard();
      advanceToValid();
    } else {
      exhausted = true;
    }
  }

  /**
   * Advance to the next valid entry (within range and within leaf bounds).
   */
  private void advanceToValid() {
    while (!exhausted) {
      // Check if we've exhausted the current leaf
      if (currentIndex >= currentLeaf.getEntryCount()) {
        // Advance to next leaf
        if (!advanceToNextLeaf()) {
          exhausted = true;
          nextEntry = null;
          return;
        }
        continue;
      }

      // Check if current entry is within range
      if (toKey != null) {
        MemorySegment keySlice = currentLeaf.getKeySlice(currentIndex);
        if (compareKeys(keySlice, toKey) > 0) {
          // Past the end of range
          exhausted = true;
          nextEntry = null;
          return;
        }
      }

      // Valid entry found
      nextEntry = new Entry(currentLeaf.getKeySlice(currentIndex), currentLeaf.getValueSlice(currentIndex));
      return;
    }
  }

  /**
   * Compare a MemorySegment key with a byte array key.
   */
  private static int compareKeys(MemorySegment a, byte[] b) {
    MemorySegment bSeg = MemorySegment.ofArray(b);
    long mismatch = a.mismatch(bSeg);
    if (mismatch == -1) {
      return 0;
    }
    if (mismatch == a.byteSize()) {
      return -1;
    }
    if (mismatch == bSeg.byteSize()) {
      return 1;
    }
    return Byte.compareUnsigned(a.get(ValueLayout.JAVA_BYTE, mismatch), bSeg.get(ValueLayout.JAVA_BYTE, mismatch));
  }

  /**
   * Advance to the next leaf using parent-based traversal.
   *
   * @return true if advanced to a new leaf, false if no more leaves
   */
  private boolean advanceToNextLeaf() {
    // Release current leaf guard
    releaseLeafGuard();

    // Use reader's parent-based traversal
    currentLeaf = reader.advanceToNextLeaf();

    if (currentLeaf == null) {
      return false;
    }

    // Acquire guard on new leaf
    acquireLeafGuard();
    currentIndex = 0;
    return true;
  }

  /**
   * Acquire guard on current leaf.
   */
  private void acquireLeafGuard() {
    if (currentLeaf != null && !guardAcquired) {
      currentLeaf.acquireGuard();
      guardAcquired = true;
    }
  }

  /**
   * Release guard on current leaf.
   */
  private void releaseLeafGuard() {
    if (currentLeaf != null && guardAcquired) {
      currentLeaf.releaseGuard();
      guardAcquired = false;
    }
  }

  @Override
  public boolean hasNext() {
    return nextEntry != null;
  }

  @Override
  public Entry next() {
    if (nextEntry == null) {
      throw new NoSuchElementException("No more entries in range");
    }

    Entry result = nextEntry;

    // Advance to next entry
    currentIndex++;
    advanceToValid();

    return result;
  }

  /**
   * Get the current leaf page (for testing/debugging).
   */
  @Nullable
  HOTLeafPage getCurrentLeaf() {
    return currentLeaf;
  }

  /**
   * Get the current index within the leaf (for testing/debugging).
   */
  int getCurrentIndex() {
    return currentIndex;
  }

  @Override
  public void close() {
    releaseLeafGuard();
    currentLeaf = null;
    nextEntry = null;
    exhausted = true;
    reader.clearPath();
  }
}

