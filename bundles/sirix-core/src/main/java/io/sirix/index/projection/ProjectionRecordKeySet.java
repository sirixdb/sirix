/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntToLongFunction;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * The index-routed row source: membership of a leaf's record keys in a SORTED key set, the
 * primitive behind {@link ProjectionIndexScan.Op#KEY_IN}.
 *
 */
public final class ProjectionRecordKeySet {

  private static final LongAdder KEY_SET_ADVANCES = new LongAdder();
  private static final boolean DIAG = Boolean.getBoolean("sirix.projection.segmentDiag");
  private static final LongAdder LOOKUP_DESCRIPTORS = new LongAdder();
  private static final LongAdder LOOKUP_KEY_SEGMENTS = new LongAdder();
  private static final LongAdder DENSE_ROWS = new LongAdder();

  public record Masks(Long2ObjectOpenHashMap<long[]> byFirstKey, LongOpenHashSet physicalSlots) {
  }

  private ProjectionRecordKeySet() {}

  public static long keySetAdvances() {
    return KEY_SET_ADVANCES.sum();
  }

  public static long lookupDescriptorsRead() {
    return LOOKUP_DESCRIPTORS.sum();
  }

  public static long lookupKeySegmentsRead() {
    return LOOKUP_KEY_SEGMENTS.sum();
  }

  public static long denseRowsVisited() {
    return DENSE_ROWS.sum();
  }

  static Masks map(final long[] sortedKeys, final ProjectionPersistedRecordLookup lookup) {
    final Long2ObjectOpenHashMap<long[]> masks = new Long2ObjectOpenHashMap<>();
    final LongOpenHashSet slots = new LongOpenHashSet();
    for (final long key : sortedKeys) {
      final long location = lookup.find(key);
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        continue;
      }
      final int slot = ProjectionPersistedRecordLookup.slot(location);
      final long[] keys = lookup.keys(slot).view().recordKeys();
      long[] mask = masks.get(keys[0]);
      if (mask == null) {
        mask = new long[(keys.length + 63) >>> 6];
        masks.put(keys[0], mask);
        slots.add(slot);
      }
      final int row = ProjectionPersistedRecordLookup.row(location);
      mask[row >>> 6] |= 1L << (row & 63);
    }
    if (DIAG) {
      LOOKUP_DESCRIPTORS.add(lookup.descriptorsRead());
      LOOKUP_KEY_SEGMENTS.add(lookup.keySegmentsRead());
    }
    return new Masks(masks, slots);
  }

  static Masks map(final long[] sortedKeys, final long[][] leafKeys) {
    return map(sortedKeys, leafKeys, leaf -> leaf + 1L);
  }

  static Masks map(final long[] sortedKeys, final long[][] leafKeys, final IntToLongFunction physicalSlot) {
    final Long2ObjectOpenHashMap<long[]> masks = new Long2ObjectOpenHashMap<>(leafKeys.length);
    final LongOpenHashSet slots = new LongOpenHashSet();
    int cursor = 0;
    long previous = Long.MIN_VALUE;
    long visited = 0;
    for (int leaf = 0; leaf < leafKeys.length; leaf++) {
      final long[] keys = leafKeys[leaf];
      visited += keys.length;
      if (keys.length == 0) {
        continue;
      }
      final long[] mask = new long[(keys.length + 63) >>> 6];
      boolean matched = false;
      for (int row = 0; row < keys.length; row++) {
        final long key = keys[row];
        final boolean member;
        if (key >= previous) {
          while (cursor < sortedKeys.length && sortedKeys[cursor] < key) {
            cursor++;
          }
          member = cursor < sortedKeys.length && sortedKeys[cursor] == key;
          previous = key;
        } else {
          member = Arrays.binarySearch(sortedKeys, 0, cursor, key) >= 0;
        }
        if (member) {
          mask[row >>> 6] |= 1L << (row & 63);
          matched = true;
        }
      }
      if (matched && masks.put(keys[0], mask) != null) {
        throw new IllegalArgumentException("projection leaves repeat a record key");
      }
      if (matched) {
        slots.add(physicalSlot.applyAsLong(leaf));
      }
    }
    KEY_SET_ADVANCES.add(cursor);
    if (DIAG) {
      DENSE_ROWS.add(visited);
    }
    return new Masks(masks, slots);
  }

  /**
   * Validate a sorted key set: strictly ascending, non-negative keys.
   *
   * @throws IllegalArgumentException when the set is unsorted or holds a duplicate or negative key
   */
  static void requireSorted(final long[] sortedKeys) {
    if (sortedKeys == null) {
      throw new IllegalArgumentException("sortedKeys must not be null");
    }
    if (sortedKeys.length > 0 && sortedKeys[0] < 0L) {
      throw new IllegalArgumentException("record keys are non-negative: " + sortedKeys[0]);
    }
    for (int i = 1; i < sortedKeys.length; i++) {
      if (sortedKeys[i] <= sortedKeys[i - 1]) {
        throw new IllegalArgumentException("sortedKeys must be strictly ascending at index " + i + ": "
            + sortedKeys[i - 1] + " then " + sortedKeys[i]);
      }
    }
  }

  /**
   * Content hash of the set — what a plan memo keyed by predicate SHAPE must fold in, so two queries
   * over different key sets never share a shape fingerprint.
   */
  static long contentHash(final long[] sortedKeys) {
    long h = 0x9E3779B97F4A7C15L ^ sortedKeys.length;
    for (final long key : sortedKeys) {
      h = (h ^ key) * 0xBF58476D1CE4E5B9L;
      h ^= h >>> 31;
    }
    return h;
  }

  /** Whether any key of the set lies in {@code [min, max]}. */
  public static boolean anyIn(final long[] sortedKeys, final long min, final long max) {
    if (min > max || sortedKeys.length == 0) {
      return false;
    }
    final int at = lowerBound(sortedKeys, min);
    return at < sortedKeys.length && sortedKeys[at] <= max;
  }

  /**
   * AND the membership of each row's record key into {@code mask}: bit {@code row} stays set iff it
   * was set and {@code keys[row]} is in the set. Only the first {@code ceil(rowCount/64)} words are
   * touched; tail bits beyond {@code rowCount} are left as the caller had them (every caller fills
   * the tail clear).
   *
   * @param keys the leaf's record keys in physical row order ({@code keys.length >= rowCount})
   * @param rowCount rows of the leaf
   * @param sortedKeys the set, strictly ascending
   * @param mask the conjunctive mask, updated in place
   * @return how many rows of the leaf are members (independent of the incoming mask)
   */
  public static int andMembership(final long[] keys, final int rowCount, final long[] sortedKeys, final long[] mask) {
    final int stride = (rowCount + 63) >>> 6;
    final int setSize = sortedKeys.length;
    if (setSize == 0) {
      Arrays.fill(mask, 0, stride, 0L);
      return 0;
    }
    int cursor = rowCount == 0
        ? 0
        : lowerBound(sortedKeys, keys[0]);
    long previous = Long.MIN_VALUE;
    int members = 0;
    for (int w = 0; w < stride; w++) {
      long out = 0L;
      final int rowBase = w << 6;
      final int rowEnd = Math.min(rowBase + 64, rowCount);
      for (int row = rowBase; row < rowEnd; row++) {
        final long key = keys[row];
        final boolean member;
        if (key >= previous) {
          // The ascending run: advance the set cursor monotonically.
          while (cursor < setSize && sortedKeys[cursor] < key) {
            cursor++;
          }
          member = cursor < setSize && sortedKeys[cursor] == key;
          previous = key;
        } else {
          // An order exception: the key fell behind the run, so the cursor says nothing about it.
          // The cursor itself is NOT moved — the run resumes from where it was.
          member = Arrays.binarySearch(sortedKeys, 0, cursor, key) >= 0;
        }
        if (member) {
          out |= 1L << (row & 63);
          members++;
        }
      }
      mask[w] &= out;
    }
    return members;
  }

  /** First index whose key is {@code >= key}, or {@code length}. */
  static int lowerBound(final long[] sorted, final long key) {
    int lo = 0;
    int hi = sorted.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (sorted[mid] < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }
}
