/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * HFT-grade direct page scanner. 1BRC-inspired.
 * Zero String allocation, zero boxing, flat array results.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public final class DirectPageScanner {

  private DirectPageScanner() {}

  // ==================== Count total ====================

  public static long countTotal(ResourceSession<?, ?> session, int revision, int threads) {
    long maxNodeKey = getMaxNodeKey(session, revision);
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;
    AtomicLong total = new AtomicLong();

    parallel(eff, i -> {
      long s = (long) i * ppt, e = Math.min(s + ppt, totalPages);
      long local = 0;
      try (var reader = session.createStorageEngineReader(revision)) {
        for (long pk = s; pk < e; pk++) {
          var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
          if (res != null && res.page() instanceof KeyValueLeafPage kv)
            // size() counts logical records exactly once across inline, sidecar and
            // reference-only overflow carriers. populatedSlotCount() is intentionally the
            // physical inline-bitmap cardinality used by version reconstruction.
            local += kv.size();
        }
      }
      total.addAndGet(local);
    });
    return total.get();
  }

  // ==================== Count by kind ====================

  public static ScanResult.KindCounts countByKind(ResourceSession<?, ?> session, int revision, int threads) {
    long maxNodeKey = getMaxNodeKey(session, revision);
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;
    long[][] perThread = new long[eff][256];

    parallel(eff, i -> {
      long[] counts = perThread[i];
      long s = (long) i * ppt, e = Math.min(s + ppt, totalPages);
      try (var reader = session.createStorageEngineReader(revision)) {
        for (long pk = s; pk < e; pk++) {
          var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
          if (res == null || !(res.page() instanceof KeyValueLeafPage kv))
            continue;
          final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
          kv.forEachPopulatedSlot(slot -> {
            int k = kv.getSlotNodeKindId(slot);
            // Kind zero is either a legacy inline record or a reference-only overflow carrier.
            // Both need one cold point read; ordinary flyweight slots and sidecar descriptors keep
            // the allocation-free directory-kind path.
            if (k == 0) {
              final var record = reader.getRecord(base + slot, IndexType.DOCUMENT, 0);
              if (record != null && record.getKind() instanceof NodeKind nodeKind) {
                k = nodeKind.getId() & 0xFF;
              }
            }
            if (k > 0 && k < 256)
              counts[k]++;
            return true;
          });
        }
      }
    });

    // Merge into flat array
    long[] merged = new long[256];
    for (long[] c : perThread)
      for (int j = 0; j < 256; j++)
        merged[j] += c[j];
    return new ScanResult.KindCounts(merged);
  }

  // ==================== Group-by string value ====================

  public static ScanResult.GroupByResult groupByStringValue(JsonResourceSession session, int revision, int targetKindId,
      int threads) {
    long maxNodeKey = getMaxNodeKey(session, revision);
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, totalPages);
    long ppt = (totalPages + eff - 1) / eff;

    ScanResult.GroupByResult[] perThread = new ScanResult.GroupByResult[eff];
    for (int i = 0; i < eff; i++)
      perThread[i] = new ScanResult.GroupByResult();

    parallel(eff, i -> {
      ScanResult.GroupByResult result = perThread[i];
      long s = (long) i * ppt, e = Math.min(s + ppt, totalPages);
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
          StorageEngineReader reader = session.createStorageEngineReader(revision)) {
        for (long pk = s; pk < e; pk++) {
          var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
          if (res == null || !(res.page() instanceof KeyValueLeafPage kv))
            continue;
          long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

          kv.forEachPopulatedSlot(slot -> {
            final int physicalKindId = kv.getSlotNodeKindId(slot);
            if (physicalKindId != targetKindId && physicalKindId != 0)
              return true;
            if (!rtx.moveTo(base + slot))
              return true;
            if ((rtx.getKind().getId() & 0xFF) != targetKindId)
              return true;
            byte[] val = rtx.getValueBytes();
            if (val != null && val.length > 0)
              result.add(val);
            return true;
          });
        }
      }
    });

    // Merge per-thread results
    ScanResult.GroupByResult merged = perThread[0];
    for (int i = 1; i < eff; i++)
      merged.merge(perThread[i]);
    return merged;
  }

  // ==================== Internals ====================

  private static long getMaxNodeKey(ResourceSession<?, ?> session, int revision) {
    try {
      var rtx = session.beginNodeReadOnlyTrx(revision);
      long key = rtx.getMaxNodeKey();
      rtx.close();
      return key;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface ChunkTask {
    void run(int threadIndex) throws Exception;
  }

  private static void parallel(int threads, ChunkTask task) {
    ExecutorService exec = Executors.newFixedThreadPool(threads, r -> {
      var t = new Thread(r, "scan");
      t.setDaemon(true);
      return t;
    });
    try {
      Future<?>[] futures = new Future[threads];
      for (int i = 0; i < threads; i++) {
        int idx = i;
        futures[i] = exec.submit(() -> {
          task.run(idx);
          return null;
        });
      }
      for (var f : futures)
        f.get();
    } catch (Exception e) {
      throw new RuntimeException("Parallel scan failed", e);
    } finally {
      exec.shutdown();
    }
  }
}
