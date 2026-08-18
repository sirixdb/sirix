/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exact grouped {@code COUNT(DISTINCT x)} over a GLOBAL value dictionary's ids, as one dense bitmap
 * per group instead of one hash set per group per worker.
 *
 * <p>
 * Global dictionary ids are dense by construction ({@link GlobalValueDictionary} reserves node keys
 * per value precisely so the id space stays gap-free), so the set of ids seen by a group is exactly a
 * bitmap over {@code [1, entryCount]}. That changes the memory from "8 bytes per distinct value, per
 * worker that saw it" to "one bit per id in the dictionary, once" — at 40M dictionary entries a group
 * costs 5 MB no matter how many rows or workers feed it, and the whole query costs
 * {@code groups × 5 MB} rather than {@code Σ_worker Σ_group |distinct|}. That is what lets the exact
 * route answer a cardinality the hash-set budget has to decline.
 *
 * <h2>Concurrency</h2> The word arrays are SHARED across the scan's workers and bits are set with an
 * atomic OR, because a plain read-modify-write loses concurrent updates in the same word and would
 * silently undercount. Setting a bit is idempotent, so no worker needs to see another's progress —
 * only the final {@link #cardinality} does, which runs after the scan joins.
 *
 * <p>
 * Workers keep a thread-confined {@code Long2ObjectOpenHashMap<long[]>} of the arrays they have
 * already resolved, so the per-row path is a fastutil lookup on a primitive key and never boxes; this
 * class is consulted once per (worker, group).
 */
public final class GroupDistinctBitmaps {

  private static final VarHandle WORDS =
      MethodHandles.arrayElementVarHandle(long[].class).withInvokeExactBehavior();

  /** Group key to its bitmap. Touched once per (worker, group), never per row. */
  private final ConcurrentHashMap<Long, long[]> byGroup = new ConcurrentHashMap<>();

  /** Words per group — {@code ceil((maxId + 1) / 64)}. */
  private final int wordsPerGroup;

  /** Highest id the dictionary can issue; an id past it means the caller's premise is wrong. */
  private final long maxId;

  /** Bytes this instance may allocate across all groups before it declines. */
  private final long budgetBytes;

  private final AtomicLong allocatedBytes = new AtomicLong();

  /**
   * @param maxId highest dictionary id, from {@code ValueDictionaryHeaderNode.getEntryCount()}
   * @param budgetBytes ceiling on total bitmap bytes; beyond it {@link #acquire} declines
   * @throws IllegalArgumentException if {@code maxId} is negative or the per-group bitmap cannot be
   *         addressed as a single array
   */
  public GroupDistinctBitmaps(final long maxId, final long budgetBytes) {
    if (maxId < 0) {
      throw new IllegalArgumentException("maxId must be >= 0, got " + maxId);
    }
    if (budgetBytes <= 0) {
      throw new IllegalArgumentException("budgetBytes must be > 0, got " + budgetBytes);
    }
    final long words = (maxId >>> 6) + 1;
    if (words > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("dictionary of " + maxId + " ids needs " + words
          + " words, which exceeds one array");
    }
    this.maxId = maxId;
    this.wordsPerGroup = (int) words;
    this.budgetBytes = budgetBytes;
  }

  /** Bytes one group's bitmap costs — the unit the budget is spent in. */
  public long bytesPerGroup() {
    return (long) wordsPerGroup * Long.BYTES;
  }

  /**
   * The bitmap for {@code groupKey}, allocating it on first use.
   *
   * @return the shared word array, or {@code null} when allocating it would exceed the budget — the
   *         caller must DECLINE the whole route rather than answer from a partial bitmap
   */
  public long @org.jspecify.annotations.Nullable [] acquire(final long groupKey) {
    final Long key = groupKey;
    final long[] existing = byGroup.get(key);
    if (existing != null) {
      return existing;
    }
    // Reserve before allocating so two racing groups cannot both pass a budget only one fits in;
    // the loser refunds its reservation and declines.
    final long claimed = allocatedBytes.addAndGet(bytesPerGroup());
    if (claimed > budgetBytes) {
      allocatedBytes.addAndGet(-bytesPerGroup());
      return null;
    }
    final long[] created = new long[wordsPerGroup];
    final long[] raced = byGroup.putIfAbsent(key, created);
    if (raced != null) {
      allocatedBytes.addAndGet(-bytesPerGroup());
      return raced;
    }
    return created;
  }

  /**
   * Record {@code id} in {@code words}. Idempotent and safe to call concurrently on one array.
   *
   * @return {@code false} when the id falls outside the sized range — the caller must DECLINE rather
   *         than drop the value, since a dropped id is a silently low count. The range is derived
   *         from an upper bound on the dictionary, so this is not expected to fire; it exists so a
   *         dictionary that outgrew the bound degrades to the generic pipeline instead of answering
   *         wrongly.
   */
  public boolean set(final long[] words, final long id) {
    if (id < 0 || id > maxId) {
      return false;
    }
    final int word = (int) (id >>> 6);
    final long bit = 1L << (id & 63);
    // Read before writing: past the early phase almost every row's bit is ALREADY set, and this turns
    // the common case from a lock-prefixed read-modify-write into a plain load. Opaque rather than
    // volatile because no ordering against other memory is needed — only that the read reaches memory
    // instead of being folded away — which costs nothing on x86 and skips the acquire fence on ARM. A
    // stale read is harmless: it can only be stale in the direction of an extra CAS, never a lost bit,
    // because no one ever clears a bit.
    long current = (long) WORDS.getOpaque(words, word);
    while ((current & bit) == 0L) {
      final long updated = current | bit;
      final long witnessed = (long) WORDS.compareAndExchange(words, word, current, updated);
      if (witnessed == current) {
        return true;
      }
      current = witnessed;
    }
    return true;
  }

  /** Exact distinct count for {@code groupKey}; {@code 0} for a group that saw no value. */
  public long cardinality(final long groupKey) {
    final long[] words = byGroup.get(groupKey);
    if (words == null) {
      return 0;
    }
    long count = 0;
    for (final long w : words) {
      count += Long.bitCount(w);
    }
    return count;
  }

  /** Group keys with a bitmap — the merge iterates these rather than the accumulator table. */
  public Iterable<Long> groups() {
    return byGroup.keySet();
  }
}
