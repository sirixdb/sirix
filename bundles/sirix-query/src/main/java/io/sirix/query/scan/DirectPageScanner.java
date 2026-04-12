/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * HFT-grade direct page scanner for SirixDB.
 *
 * 1BRC-inspired: parallel page-range splitting, open-addressing byte-key hash
 * map, zero String allocation during scanning, bitmap-filtered moveTo().
 *
 * For counting: reads node kind from page directory (1 byte per slot) via
 * getSlotNodeKindId() — no moveTo(), no FlyweightNode deserialization.
 *
 * For aggregation: 1BRC open-addressing hash with raw byte[] keys, stride-31
 * probing, longHash(), 8-byte-at-a-time comparison. String only created for
 * the final ~N output groups.
 */
package io.sirix.query.scan;

import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public final class DirectPageScanner {

  private static final int MAP_CAPACITY = 4096;
  private static final int MAP_MASK = MAP_CAPACITY - 1;

  private DirectPageScanner() {}

  // ==================== Count total (SIMD bitmap popcount) ====================

  public static long countTotal(ResourceSession<?, ?> session, int revision, int threads) {
    long maxNodeKey;
    try {
      var rtx = session.beginNodeReadOnlyTrx(revision);
      maxNodeKey = rtx.getMaxNodeKey();
      rtx.close();
    } catch (Exception e) { throw new RuntimeException(e); }
    long maxPageKey = maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
    long totalPages = maxPageKey + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;
    AtomicLong total = new AtomicLong();

    ExecutorService exec = Executors.newFixedThreadPool(eff, r -> { var t = new Thread(r, "count"); t.setDaemon(true); return t; });
    try {
      Future<?>[] futures = new Future[eff];
      for (int i = 0; i < eff; i++) {
        long s = (long) i * ppt, e = Math.min((long) (i + 1) * ppt, totalPages);
        futures[i] = exec.submit(() -> {
          long local = 0;
          try (var reader = session.createStorageEngineReader(revision)) {
            for (long pk = s; pk < e; pk++) {
              var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
              if (res != null && res.page() instanceof KeyValueLeafPage kv)
                local += kv.populatedSlotCount();
            }
          }
          total.addAndGet(local);
        });
      }
      for (var f : futures) f.get();
    } catch (Exception ex) {
      throw new RuntimeException("Parallel count failed", ex);
    } finally {
      exec.shutdown();
    }
    return total.get();
  }

  // ==================== Count by kind (1 byte per slot, no deser) ====================

  public static Map<Integer, Long> countByKind(ResourceSession<?, ?> session, int revision, int threads) {
    long maxNodeKey;
    try {
      var rtx = session.beginNodeReadOnlyTrx(revision);
      maxNodeKey = rtx.getMaxNodeKey();
      rtx.close();
    } catch (Exception e) { throw new RuntimeException(e); }
    long maxPageKey = maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
    long totalPages = maxPageKey + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;
    long[][] perThread = new long[eff][256];

    ExecutorService exec = Executors.newFixedThreadPool(eff, r -> { var t = new Thread(r, "kind"); t.setDaemon(true); return t; });
    try {
      Future<?>[] futures = new Future[eff];
      for (int i = 0; i < eff; i++) {
        int idx = i;
        long s = (long) i * ppt, e = Math.min((long) (i + 1) * ppt, totalPages);
        futures[i] = exec.submit(() -> {
          long[] counts = perThread[idx];
          try (var reader = session.createStorageEngineReader(revision)) {
            for (long pk = s; pk < e; pk++) {
              var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
              if (res == null) continue;
              if (!(res.page() instanceof KeyValueLeafPage kv)) continue;
              kv.forEachPopulatedSlot(slot -> {
                int k = kv.getSlotNodeKindId(slot);
                if (k > 0 && k < 256) counts[k]++;
                return true;
              });
            }
          }
        });
      }
      for (var f : futures) f.get();
    } catch (Exception ex) {
      throw new RuntimeException("Parallel scan failed", ex);
    } finally {
      exec.shutdown();
    }

    Map<Integer, Long> merged = new HashMap<>();
    for (long[] c : perThread)
      for (int i = 0; i < 256; i++)
        if (c[i] > 0) merged.merge(i, c[i], Long::sum);
    return merged;
  }

  // ==================== Group-by (1BRC byte-key hash, zero String) ====================

  public static Map<String, Long> groupByStringValue(
      JsonResourceSession session, int revision, int targetKindId, int threads) {
    long maxNodeKey;
    try {
      var rtx = session.beginNodeReadOnlyTrx(revision);
      maxNodeKey = rtx.getMaxNodeKey();
      rtx.close();
    } catch (Exception e) { throw new RuntimeException(e); }
    long maxPageKey = maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
    long totalPages = maxPageKey + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;

    // Per-thread 1BRC open-addressing byte-key hash maps
    byte[][][] ptKeys = new byte[eff][MAP_CAPACITY][];
    long[][] ptCounts = new long[eff][MAP_CAPACITY];

    ExecutorService exec = Executors.newFixedThreadPool(eff, r -> { var t = new Thread(r, "groupby"); t.setDaemon(true); return t; });
    try {
      Future<?>[] futures = new Future[eff];
      for (int i = 0; i < eff; i++) {
        int idx = i;
        long s = (long) i * ppt, e = Math.min((long) (i + 1) * ppt, totalPages);
        futures[i] = exec.submit(() -> {
          byte[][] keys = ptKeys[idx];
          long[] counts = ptCounts[idx];
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
               StorageEngineReader reader = session.createStorageEngineReader(revision)) {
            for (long pk = s; pk < e; pk++) {
              var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
              if (res == null) continue;
              if (!(res.page() instanceof KeyValueLeafPage kv)) continue;
              long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

              kv.forEachPopulatedSlot(slot -> {
                if (kv.getSlotNodeKindId(slot) != targetKindId) return true;
                if (!rtx.moveTo(base + slot)) return true;

                // HFT: raw bytes, no String
                byte[] val = rtx.getValueBytes();
                if (val == null || val.length == 0) return true;

                // 1BRC open-addressing insert
                int hash = longHash(val, 0, val.length);
                int mi = hash & MAP_MASK;
                while (true) {
                  byte[] existing = keys[mi];
                  if (existing == null) {
                    keys[mi] = val.clone();
                    counts[mi] = 1;
                    break;
                  }
                  if (existing.length == val.length && bytesEqual(existing, val, val.length)) {
                    counts[mi]++;
                    break;
                  }
                  mi = (mi + 31) & MAP_MASK;
                }
                return true;
              });
            }
          }
        });
      }
      for (var f : futures) f.get();
    } catch (Exception ex) {
      throw new RuntimeException("Parallel scan failed", ex);
    } finally {
      exec.shutdown();
    }

    // Merge: String only for the ~N output groups
    Map<String, Long> merged = new HashMap<>();
    for (int t = 0; t < eff; t++)
      for (int i = 0; i < MAP_CAPACITY; i++)
        if (ptKeys[t][i] != null)
          merged.merge(new String(ptKeys[t][i], StandardCharsets.UTF_8), ptCounts[t][i], Long::sum);
    return merged;
  }

  // ==================== 1BRC hash primitives ====================

  private static int longHash(byte[] buf, int off, int len) {
    long h;
    if (len >= 8) {
      h = readLong(buf, off);
      if (len >= 16) h ^= readLong(buf, off + 8);
    } else {
      h = 0;
      for (int i = 0; i < len; i++) h = (h << 8) | (buf[off + i] & 0xFFL);
    }
    h ^= (h >>> 33) ^ (h >>> 15);
    return (int) h;
  }

  private static long readLong(byte[] buf, int off) {
    return ((long) buf[off] << 56) | ((long) (buf[off + 1] & 0xFF) << 48)
        | ((long) (buf[off + 2] & 0xFF) << 40) | ((long) (buf[off + 3] & 0xFF) << 32)
        | ((long) (buf[off + 4] & 0xFF) << 24) | ((long) (buf[off + 5] & 0xFF) << 16)
        | ((long) (buf[off + 6] & 0xFF) << 8) | (buf[off + 7] & 0xFF);
  }

  private static boolean bytesEqual(byte[] a, byte[] b, int len) {
    int i = 0;
    for (; i + 8 <= len; i += 8)
      if (readLong(a, i) != readLong(b, i)) return false;
    for (; i < len; i++)
      if (a[i] != b[i]) return false;
    return true;
  }
}
