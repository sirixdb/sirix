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
import org.jspecify.annotations.Nullable;

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
   * iter#08 — when {@code true}, the cursor stays positioned on the
   * current valid entry without materialising a new {@link Entry} record
   * per advance. Callers use {@link #currentKeySlice()} /
   * {@link #currentValueSlice()} / {@link #currentLeafPage()} +
   * {@link #advance()} to walk entries zero-alloc. The legacy
   * {@link Iterator} API ({@link #hasNext}/{@link #next}) continues to
   * work against the same positional state for callers that prefer it.
   */
  private boolean positionedValid = false;

  /**
   * Create a new range cursor.
   *
   * @param reader the keyed trie reader
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive)
   * @param toKey the end key (inclusive)
   */
  HOTRangeCursor(HOTTrieReader reader, PageReference rootRef, byte[] fromKey, byte[] toKey) {
    this.reader = Objects.requireNonNull(reader);
    this.rootRef = Objects.requireNonNull(rootRef);
    this.fromKey = fromKey;
    this.toKey = toKey;

    // Initialize to first entry
    descendToFirstEntry();
  }

  /**
   * Descend to the first entry {@code >= fromKey} in lex order.
   *
   * <p>Reference impl: {@code HOTSingleThreaded::lower_bound} (Binna §4.2). PEXT-routed
   * point-search alone is incorrect for non-existent fromKeys — it lands at a partial-key
   * match which can miss the lex-position. The proper algorithm walks back up the search
   * stack to the branching depth and re-positions in the affected-subtree's first child.
   * See {@link HOTTrieReader#lowerBound(io.sirix.page.PageReference, byte[])}.</p>
   */
  private void descendToFirstEntry() {
    if (fromKey == null) {
      // No lower bound — start at leftmost leaf.
      currentLeaf = reader.navigateToLeftmostLeaf(rootRef);
      currentIndex = 0;
    } else {
      final HOTTrieReader.LowerBoundResult lb = reader.lowerBound(rootRef, fromKey);
      if (lb == null || lb.leaf == null) {
        exhausted = true;
        return;
      }
      currentLeaf = lb.leaf;
      currentIndex = lb.indexInLeaf;
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
   *
   * <p>iter#08 — the positional state ({@link #positionedValid},
   * {@link #currentLeaf}, {@link #currentIndex}) is authoritative. The
   * legacy {@link #nextEntry} field is only populated on demand by
   * {@link #next()} to preserve the {@link Iterator} API; the fast-path
   * accessors read directly from the positional state.
   */
  private void advanceToValid() {
    while (!exhausted) {
      // Check if we've exhausted the current leaf
      if (currentIndex >= currentLeaf.getEntryCount()) {
        // Advance to next leaf
        if (!advanceToNextLeaf()) {
          exhausted = true;
          positionedValid = false;
          nextEntry = null;
          return;
        }
        continue;
      }

      // Check if current entry is within range — zero-alloc comparison
      // against the pre-supplied {@code toKey} bound. The previous impl
      // materialised a {@link MemorySegment} via {@code getKeySlice} +
      // {@code MemorySegment.ofArray(toKey)} on every call (3 heap
      // allocs per iteration); the new path reads the key bytes straight
      // from the HOT leaf's on/off-heap storage.
      if (toKey != null && currentLeaf.compareKeyWithBound(currentIndex, toKey) > 0) {
        exhausted = true;
        positionedValid = false;
        nextEntry = null;
        return;
      }

      // Valid entry found — expose via positional accessors.
      positionedValid = true;
      nextEntry = null;
      return;
    }
  }

  // iter#08 — compareKeys(MemorySegment, byte[]) removed in favour of the
  // zero-alloc {@link HOTLeafPage#compareKeyWithBound(int, byte[])} helper
  // called directly from {@link #advanceToValid}. The old helper allocated
  // a MemorySegment.ofArray(byte[]) wrapper on every cursor step.

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
      guardAcquired = currentLeaf.acquireGuard();
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
    return positionedValid;
  }

  @Override
  public Entry next() {
    if (!positionedValid) {
      throw new NoSuchElementException("No more entries in range");
    }

    // Legacy Iterator API: materialise the Entry record on demand.
    // Zero-alloc callers must use {@link #advance} +
    // {@link #currentKeySlice} / {@link #currentValueSlice} / {@link #currentLeafPage} instead.
    final Entry result = new Entry(currentLeaf.getKeySlice(currentIndex), currentLeaf.getValueSlice(currentIndex));

    // Advance to next entry
    currentIndex++;
    advanceToValid();

    return result;
  }

  /**
   * iter#08 zero-alloc fast-path — advance past the current entry.
   * Callers must have consumed the current entry (via
   * {@link #currentKeySlice}, {@link #currentValueSlice},
   * {@link #currentLeafPage} + {@link #currentEntryIndex}, or
   * {@link HOTLeafPage#decodeKey8BE}) BEFORE calling this. After it
   * returns, {@link #hasNext} reports whether a new valid entry is
   * now positioned.
   *
   * <p>Iteration pattern:
   * <pre>{@code
   *   while (cursor.hasNext()) {
   *     final long key = cursor.currentLeafPage().decodeKey8BE(cursor.currentEntryIndex());
   *     final MemorySegment val = cursor.currentValueSlice();
   *     consume(key, val);
   *     cursor.advance();
   *   }
   * }</pre>
   *
   * <p>Concurrency: single-threaded cursor state; guard lifetime same
   * as the legacy path.
   */
  public void advance() {
    if (!positionedValid) {
      return;
    }
    currentIndex++;
    advanceToValid();
  }

  /**
   * iter#08 zero-alloc — key slice for the current positioned entry.
   * Requires {@link #hasNext()} / {@link #advance()} to have returned
   * {@code true}. The returned slice is valid for the duration of the
   * current leaf guard; callers must not retain it across an
   * {@link #advance} call.
   *
   * <p>Note this method still allocates a heap-backed {@link MemorySegment}
   * wrapper — the underlying key bytes are on-heap inside the leaf's
   * {@code commonPrefix + suffix} reconstruction. Zero-alloc key
   * consumers should use {@link HOTLeafPage#decodeKey8BE} on
   * {@link #currentLeafPage} at {@link #currentEntryIndex} instead.
   */
  public MemorySegment currentKeySlice() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentLeaf.getKeySlice(currentIndex);
  }

  /**
   * iter#08 zero-alloc — value slice for the current positioned entry
   * (already zero-copy via {@link HOTLeafPage#getValueSlice}). Slice
   * lifetime bounded by the leaf guard.
   */
  public MemorySegment currentValueSlice() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentLeaf.getValueSlice(currentIndex);
  }

  /**
   * iter#08 zero-alloc — the HOT leaf page carrying the current entry.
   * Consumers that need an allocation-free decode of the composite
   * 8-byte key call {@link HOTLeafPage#decodeKey8BE} at
   * {@link #currentEntryIndex} on this leaf.
   */
  public HOTLeafPage currentLeafPage() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentLeaf;
  }

  /** Entry index within {@link #currentLeafPage}. */
  public int currentEntryIndex() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentIndex;
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
    positionedValid = false;
    exhausted = true;
    reader.clearPath();
  }
}

