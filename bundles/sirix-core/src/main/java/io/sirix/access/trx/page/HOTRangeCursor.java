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
 * <li>Optimistic stamp validation — leaves stay evictable, every positioning decision is confirmed
 * against the FrameSlotAllocator's per-slot seqlock version before it takes effect</li>
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
  private final byte @Nullable [] fromKey;
  private final byte @Nullable [] toKey;

  // Current position
  private HOTLeafPage currentLeaf;
  private int currentIndex;
  private boolean exhausted = false;


  /**
   * iter#08 — when {@code true}, the cursor stays positioned on the current valid entry without
   * materialising a new {@link Entry} record per advance. Callers use {@link #currentKeySlice()} /
   * {@link #currentValueSlice()} / {@link #currentLeafPage()} + {@link #advance()} to walk entries
   * zero-alloc. The standard {@link Iterator} API ({@link #hasNext}/{@link #next}) continues to work
   * against the same positional state for callers that prefer it.
   */
  private boolean positionedValid = false;

  // Verdicts computed by one batch of unpinned-leaf reads in advanceToValid(), applied only after
  // the batch passes stamp validation.
  private static final int VERDICT_ADVANCE_LEAF = 0;
  private static final int VERDICT_EXIT_SCAN = 1;
  private static final int VERDICT_SKIP_LEAF = 2;
  private static final int VERDICT_SKIP_ENTRY = 3;
  private static final int VERDICT_EMIT = 4;

  /**
   * Create a new range cursor.
   *
   * @param reader the keyed trie reader
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive), or {@code null} to start at the leftmost leaf
   * @param toKey the end key (inclusive), or {@code null} for an unbounded upper end
   */
  HOTRangeCursor(HOTTrieReader reader, PageReference rootRef, byte @Nullable [] fromKey, byte @Nullable [] toKey) {
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
   * <p>
   * Reference impl: {@code HOTSingleThreaded::lower_bound} (Binna §4.2). PEXT-routed point-search
   * alone is incorrect for non-existent fromKeys — it lands at a partial-key match which can miss the
   * lex-position. The proper algorithm walks back up the search stack to the branching depth and
   * re-positions in the affected-subtree's first child. See
   * {@link HOTTrieReader#lowerBound(io.sirix.page.PageReference, byte[])}.
   * </p>
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
      advanceToValid();
    } else {
      exhausted = true;
    }
  }

  /**
   * Advance to the next valid entry (within range and within leaf bounds).
   *
   * <p>
   * iter#08 — the positional state ({@link #positionedValid}, {@link #currentLeaf},
   * {@link #currentIndex}) is authoritative. The fast-path accessors read directly from that
   * positional state.
   */
  private void advanceToValid() {
    int tornRounds = 0;
    while (!exhausted) {
      // Classify the current position from ONE batch of unpinned-leaf reads, then validate the
      // stamp BEFORE the classification takes effect. Every cursor-state mutation below the read
      // block is therefore derived from proven-stable bytes — which is also why torn recovery
      // never rewinds: currentIndex only ever moves on validated decisions, so it stays correct
      // across a leaf reload (content per PageReference is immutable).
      final int verdict;
      int entryCount = 0;
      int rangeVerdict = 0;
      try {
        entryCount = currentLeaf.getEntryCount();
        if (currentIndex >= entryCount) {
          // Current leaf exhausted.
          verdict = VERDICT_ADVANCE_LEAF;
        } else if (currentIndex == 0 && leafCannotContainInRangeKeys(entryCount)) {
          // Whole-leaf skip. Entries WITHIN a leaf are sorted, so one comparison against each end
          // rules out every entry in it. Leaf visit order is lex-monotonic by the writer's mandatory
          // I12 invariant, so the first leaf past {@code toKey} ends the scan.
          verdict = toKey != null && currentLeaf.compareKeyWithBound(0, toKey) > 0
              ? VERDICT_EXIT_SCAN
              : VERDICT_SKIP_LEAF;
        } else if ((rangeVerdict = classifyAgainstBounds(currentIndex)) != 0) {
          // Per-entry range check — zero-alloc comparison against the pre-supplied bounds,
          // reading the key bytes straight from the HOT leaf's on/off-heap storage. Same
          // The first key past {@code toKey} ends the scan on the canonical lex-monotonic trie.
          // rangeVerdict > 0 means "past toKey", which the compare above already established — no
          // second full-key comparison per rejected entry (these run byte-at-a-time over keys up to
          // 256 bytes, so the duplicate doubled the cost of every out-of-range tail).
          verdict = rangeVerdict > 0
              ? VERDICT_EXIT_SCAN
              : VERDICT_SKIP_ENTRY;
        } else {
          verdict = VERDICT_EMIT;
        }
      } catch (RuntimeException e) {
        if (reader.validateCurrentLeaf()) {
          throw e; // stable bytes — genuine corruption, not a torn read
        }
        recoverTornLeaf(++tornRounds);
        continue;
      }
      if (!reader.validateCurrentLeaf()) {
        recoverTornLeaf(++tornRounds);
        continue;
      }
      tornRounds = 0;
      switch (verdict) {
        case VERDICT_ADVANCE_LEAF -> {
          if (!advanceToNextLeaf()) {
            exhausted = true;
            positionedValid = false;
            return;
          }
        }
        case VERDICT_EXIT_SCAN -> {
          exhausted = true;
          positionedValid = false;
          return;
        }
        case VERDICT_SKIP_LEAF -> currentIndex = entryCount;
        case VERDICT_SKIP_ENTRY -> currentIndex++;
        default -> {
          // Valid entry found — expose via positional accessors.
          positionedValid = true;
          return;
        }
      }
    }
  }

  /**
   * Recover from a failed stamp validation: reload the current leaf through its {@link PageReference}
   * and re-adopt the fresh copy at the SAME position — content per reference is immutable, so
   * {@link #currentIndex} stays correct.
   *
   * @param round the number of consecutive torn rounds including this one, for the retry bound
   */
  private void recoverTornLeaf(final int round) {
    recoverTorn(round);
  }

  /**
   * Bounded torn-read recovery for consumers driving this cursor: reload the current leaf and
   * re-adopt the fresh copy at the SAME position. Content per {@link PageReference} is immutable, so
   * {@link #currentEntryIndex()} stays correct across the reload — callers re-read, they do not
   * rewind.
   *
   * @param round how many consecutive torn rounds this is, including the current one
   */
  public void recoverTorn(final int round) {
    recoverTorn(round, "HOT range cursor");
  }

  /**
   * As {@link #recoverTorn(int)}, but naming the caller so an exhaustion diagnostic identifies which
   * scan path was thrashing rather than reporting every consumer as "HOT range cursor".
   *
   * @param round how many consecutive torn rounds this is, including the current one
   * @param operation the caller's name, for the exhaustion diagnostic
   */
  public void recoverTorn(final int round, final String operation) {
    reader.recoverTorn(round, operation);
    currentLeaf = reader.currentLeafPage();
  }

  /**
   * Where the entry at {@code index} sits relative to {@code [fromKey, toKey]}: {@code -1} below
   * {@code fromKey}, {@code 0} in range, {@code 1} past {@code toKey}.
   *
   * <p>
   * Three-way rather than boolean so the caller can tell "past the upper bound" (which ends the scan
   * on a lex-monotonic trie) from "below the lower bound" without repeating the comparison — these
   * run byte-at-a-time over the full key.
   *
   * <p>
   * Both ends are checked defensively even though canonical HOT traversal is lex-monotonic. The
   * explicit lower check also keeps this cursor's byte-range contract local instead of relying on
   * every caller to prove how its composite lower bound was formed.
   */
  private int classifyAgainstBounds(final int index) {
    if (fromKey != null && currentLeaf.compareKeyWithBound(index, fromKey) < 0) {
      return -1;
    }
    // Upper bound stays INCLUSIVE, as it was when this comparison ended the scan.
    if (toKey != null && currentLeaf.compareKeyWithBound(index, toKey) > 0) {
      return 1;
    }
    return 0;
  }

  /**
   * Can the current leaf be skipped whole? True when its lowest key is already past {@code toKey}, or
   * its highest key is still below {@code fromKey}. Sound because entries within leaves and canonical
   * leaf traversal are both lexicographically ordered.
   */
  private boolean leafCannotContainInRangeKeys(final int entryCount) {
    if (entryCount == 0) {
      return true;
    }
    if (toKey != null && currentLeaf.compareKeyWithBound(0, toKey) > 0) {
      return true;
    }
    return fromKey != null && currentLeaf.compareKeyWithBound(entryCount - 1, fromKey) < 0;
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
    // reader.advanceToNextLeaf resolves the next leaf through HOTTrieReader.loadPage, which
    // snapshots its optimistic stamp as the reader's current leaf. No pin is taken — the leaf
    // stays evictable, and every read of it goes through the validate-then-commit discipline
    // in advanceToValid()/next().
    currentLeaf = reader.advanceToNextLeaf();
    if (currentLeaf == null) {
      return false;
    }
    currentIndex = 0;
    return true;
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

    // Legacy Iterator API: materialise the Entry record on demand. The value bytes are COPIED out
    // of the unpinned leaf slot and the copy stamp-validated, so the returned Entry stays readable
    // no matter when the leaf is evicted afterwards (the key slice is already a heap-backed copy —
    // see currentKeySlice()). Zero-alloc callers use {@link #advance} + the positional accessors
    // and run their own validation via {@link #validateLeaf}.
    Entry result = null;
    for (int round = 1; result == null; round++) {
      try {
        final MemorySegment keySlice = currentLeaf.getKeySlice(currentIndex);
        final MemorySegment valueSlice = currentLeaf.getValueSlice(currentIndex);
        final long valueSize = valueSlice.byteSize();
        if (valueSize > currentLeaf.slotCapacity()) {
          // A length no slot can hold is either a torn read (validation below fails — retry) or
          // real corruption (it holds — the throw escapes); either way it must not drive the
          // allocation.
          throw new IllegalStateException("value slice of " + valueSize + " bytes exceeds the slot capacity");
        }
        final byte[] valueCopy = new byte[(int) valueSize];
        MemorySegment.copy(valueSlice, ValueLayout.JAVA_BYTE, 0, valueCopy, 0, valueCopy.length);
        result = new Entry(keySlice, MemorySegment.ofArray(valueCopy));
      } catch (RuntimeException e) {
        if (reader.validateCurrentLeaf()) {
          throw e;
        }
        recoverTornLeaf(round);
        continue;
      }
      if (!reader.validateCurrentLeaf()) {
        result = null;
        recoverTornLeaf(round);
      }
    }

    // Advance to next entry
    currentIndex++;
    advanceToValid();

    return result;
  }

  /**
   * iter#08 zero-alloc fast-path — advance past the current entry. Callers must have consumed the
   * current entry (via {@link #currentKeySlice}, {@link #currentValueSlice}, {@link #currentLeafPage}
   * + {@link #currentEntryIndex}, or {@link HOTLeafPage#decodeKey8BE}) BEFORE calling this. After it
   * returns, {@link #hasNext} reports whether a new valid entry is now positioned.
   *
   * <p>
   * Iteration pattern:
   * 
   * <pre>{@code
   * while (cursor.hasNext()) {
   *   final long key = cursor.currentLeafPage().decodeKey8BE(cursor.currentEntryIndex());
   *   final MemorySegment val = cursor.currentValueSlice();
   *   consume(key, val);
   *   cursor.advance();
   * }
   * }</pre>
   *
   * <p>
   * Concurrency: single-threaded cursor state; guard lifetime is the same for both cursor APIs.
   */
  public void advance() {
    if (!positionedValid) {
      return;
    }
    currentIndex++;
    advanceToValid();
  }

  /**
   * Whether every read of the current leaf's content since it was resolved saw stable bytes. One call
   * covers the whole batch of reads since the leaf's stamp snapshot — consumers of the positional
   * accessors call this AFTER reading (and before trusting the result), exactly like the cursor's own
   * advance machinery does internally.
   */
  public boolean validateLeaf() {
    return reader.validateCurrentLeaf();
  }

  /**
   * Reload the current leaf through its {@link PageReference} after a failed {@link #validateLeaf}.
   * The cursor keeps its position: content per reference is immutable, so every entry index computed
   * against the stale copy stays valid against the fresh one. Callers re-read from
   * {@link #currentLeafPage()} — the reload creates a NEW page object.
   */
  @Deprecated
  public void refreshLeaf() {
    recoverTorn(1);
  }

  /**
   * Re-seek the cursor to the first in-range entry whose composite key is {@code >=} the given key,
   * keeping the cursor's original range bounds. Group-retry hook for consumers that aggregate several
   * consecutive entries into one result: when a torn read is detected mid-group, the aggregate is
   * discarded and the walk restarted at the group's first composite — which the caller holds as
   * validated heap bytes.
   *
   * @param compositeKey the composite key to re-position at (inclusive)
   */
  public void restartAtComposite(final byte[] compositeKey) {
    Objects.requireNonNull(compositeKey);
    exhausted = false;
    positionedValid = false;
    final HOTTrieReader.LowerBoundResult lb = reader.lowerBound(rootRef, compositeKey);
    if (lb == null || lb.leaf == null) {
      exhausted = true;
      currentLeaf = null;
      return;
    }
    currentLeaf = lb.leaf;
    currentIndex = lb.indexInLeaf;
    advanceToValid();
  }

  /**
   * iter#08 zero-alloc — key slice for the current positioned entry. Requires {@link #hasNext()} /
   * {@link #advance()} to have returned {@code true}. Callers must not retain the returned slice
   * across an {@link #advance} call, and must confirm reads via {@link #validateLeaf()} before
   * trusting them — the leaf is unpinned.
   *
   * <p>
   * Note this method still allocates a heap-backed {@link MemorySegment} wrapper — the underlying key
   * bytes are on-heap inside the leaf's {@code commonPrefix + suffix} reconstruction. Zero-alloc key
   * consumers should use {@link HOTLeafPage#decodeKey8BE} on {@link #currentLeafPage} at
   * {@link #currentEntryIndex} instead.
   */
  public MemorySegment currentKeySlice() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentLeaf.getKeySlice(currentIndex);
  }

  /**
   * iter#08 zero-alloc — value slice for the current positioned entry (already zero-copy via
   * {@link HOTLeafPage#getValueSlice}). The slice views UNPINNED slot memory: consume it, then
   * confirm via {@link #validateLeaf()} (retrying through {@link #refreshLeaf()} on failure) before
   * trusting what was read.
   */
  public MemorySegment currentValueSlice() {
    if (!positionedValid) {
      throw new NoSuchElementException("cursor is not positioned on a valid entry");
    }
    return currentLeaf.getValueSlice(currentIndex);
  }

  /**
   * iter#08 zero-alloc — the HOT leaf page carrying the current entry. Consumers that need an
   * allocation-free decode of the composite 8-byte key call {@link HOTLeafPage#decodeKey8BE} at
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
    // Nothing to release: neither the cursor nor the reader pins leaves. Clearing just drops the
    // references so an abandoned cursor does not keep a leaf object reachable.
    currentLeaf = null;
    positionedValid = false;
    exhausted = true;
    reader.clearPath();
  }
}
