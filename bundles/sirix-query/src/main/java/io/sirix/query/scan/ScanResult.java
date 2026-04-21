/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * Zero-allocation result types for DirectPageScanner.
 * No boxing, no Map, no String during scanning — only flat arrays.
 */
package io.sirix.query.scan;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * HFT-grade scan results using flat arrays.
 */
public final class ScanResult {

  private ScanResult() {}

  /**
   * Kind count result — flat long[256] array indexed by NodeKind ordinal.
   * No Map, no boxing. Access: {@code counts[NodeKind.STRING_VALUE.getId()]}.
   */
  public static final class KindCounts {
    private final long[] counts;
    private final long total;

    public KindCounts(long[] counts) {
      this.counts = counts;
      long t = 0;
      for (long c : counts) t += c;
      this.total = t;
    }

    /** Direct array access — zero allocation. */
    public long[] counts() { return counts; }

    /** Count for a specific kind ordinal. */
    public long get(int kindId) { return counts[kindId]; }

    /** Total across all kinds. */
    public long total() { return total; }

    /** Convert to Map only when needed (e.g., for output). */
    public Map<Integer, Long> toMap() {
      Map<Integer, Long> m = new HashMap<>();
      for (int i = 0; i < 256; i++)
        if (counts[i] > 0) m.put(i, counts[i]);
      return m;
    }
  }

  /**
   * Group-by result backed by open-addressing byte-key hash table.
   * No String, no Map during scanning. Convert to Map only for output.
   */
  public static final class GroupByResult {
    private static final int CAPACITY = 4096;
    private static final int MASK = CAPACITY - 1;

    private final byte[][] keys;
    private final long[] counts;
    private int size;

    public GroupByResult() {
      this.keys = new byte[CAPACITY][];
      this.counts = new long[CAPACITY];
    }

    /** Insert or increment — 1BRC open-addressing, zero allocation for known keys. */
    public void add(byte[] value, int off, int len) {
      int hash = longHash(value, off, len);
      int idx = hash & MASK;
      while (true) {
        byte[] existing = keys[idx];
        if (existing == null) {
          byte[] copy = new byte[len];
          System.arraycopy(value, off, copy, 0, len);
          keys[idx] = copy;
          counts[idx] = 1;
          size++;
          return;
        }
        if (existing.length == len && bytesEqual(existing, 0, value, off, len)) {
          counts[idx]++;
          return;
        }
        idx = (idx + 31) & MASK;
      }
    }

    /** Insert pre-copied key bytes. */
    public void add(byte[] key) {
      add(key, 0, key.length);
    }

    /**
     * Insert {@code count} occurrences of a key at once — avoids {@code count}
     * separate hash+probe cycles when the caller has already aggregated a run
     * (e.g. per-page group-by flushing). Zero-alloc on an existing entry;
     * one copy on a fresh entry.
     */
    public void addN(byte[] value, int off, int len, long count) {
      int hash = longHash(value, off, len);
      int idx = hash & MASK;
      while (true) {
        byte[] existing = keys[idx];
        if (existing == null) {
          byte[] copy = new byte[len];
          System.arraycopy(value, off, copy, 0, len);
          keys[idx] = copy;
          counts[idx] = count;
          size++;
          return;
        }
        if (existing.length == len && bytesEqual(existing, 0, value, off, len)) {
          counts[idx] += count;
          return;
        }
        idx = (idx + 31) & MASK;
      }
    }

    /** Merge another result into this one. */
    public void merge(GroupByResult other) {
      for (int i = 0; i < CAPACITY; i++) {
        if (other.keys[i] != null) {
          mergeKey(other.keys[i], other.counts[i]);
        }
      }
    }

    private void mergeKey(byte[] key, long count) {
      int hash = longHash(key, 0, key.length);
      int idx = hash & MASK;
      while (true) {
        byte[] existing = keys[idx];
        if (existing == null) {
          keys[idx] = key;
          counts[idx] = count;
          size++;
          return;
        }
        if (existing.length == key.length && bytesEqual(existing, 0, key, 0, key.length)) {
          counts[idx] += count;
          return;
        }
        idx = (idx + 31) & MASK;
      }
    }

    /** Number of unique groups. */
    public int size() { return size; }

    /** Raw key/count access for zero-allocation iteration. */
    public byte[][] rawKeys() { return keys; }
    public long[] rawCounts() { return counts; }
    public int capacity() { return CAPACITY; }

    /** Iterate without allocation. */
    public void forEach(GroupConsumer consumer) {
      for (int i = 0; i < CAPACITY; i++) {
        if (keys[i] != null) {
          consumer.accept(keys[i], counts[i]);
        }
      }
    }

    /** Convert to Map only for output. */
    public Map<String, Long> toMap() {
      Map<String, Long> m = new HashMap<>(size * 2);
      for (int i = 0; i < CAPACITY; i++) {
        if (keys[i] != null) {
          m.put(new String(keys[i], StandardCharsets.UTF_8), counts[i]);
        }
      }
      return m;
    }

    @FunctionalInterface
    public interface GroupConsumer {
      void accept(byte[] key, long count);
    }

    // ==================== 1BRC hash primitives ====================

    /**
     * Byte-array → long VarHandle (little-endian on all target platforms we
     * ship on). JIT compiles to a single unaligned 64-bit load; the old
     * byte-shift formulation was 8 byte-loads + 7 shifts + 7 ORs per call
     * and dominated the groupBy hot loop once combineRecordPages was
     * skipped.
     */
    private static final java.lang.invoke.VarHandle LONG_LE = java.lang.invoke.MethodHandles
        .byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.LITTLE_ENDIAN);

    static int longHash(byte[] buf, int off, int len) {
      long h;
      if (len >= 8) {
        h = (long) LONG_LE.get(buf, off);
        if (len >= 16) h ^= (long) LONG_LE.get(buf, off + 8);
      } else {
        // Short-value path: pack up to 7 bytes into a long via a single
        // aligned read when safe, else byte loop. Short values (<8 bytes)
        // are common for enum-like group-by keys ("Eng", "Sales", ...).
        h = 0;
        for (int i = 0; i < len; i++) h = (h << 8) | (buf[off + i] & 0xFFL);
      }
      h ^= (h >>> 33) ^ (h >>> 15);
      return (int) h;
    }

    static long readLong(byte[] buf, int off) {
      return (long) LONG_LE.get(buf, off);
    }

    public static boolean bytesEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
      int i = 0;
      for (; i + 8 <= len; i += 8) {
        if ((long) LONG_LE.get(a, aOff + i) != (long) LONG_LE.get(b, bOff + i)) return false;
      }
      for (; i < len; i++) {
        if (a[aOff + i] != b[bOff + i]) return false;
      }
      return true;
    }
  }
}
