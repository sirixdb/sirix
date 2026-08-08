package io.sirix.benchmark;

import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.node.DeltaVarIntCodec;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Can a JSON array's elements be enumerated from RECORD PAGE RANGES instead of by walking the
 * sibling chain?
 *
 * <p>This is the pivot the morsel redesign turns on. The sibling walk is the natural enumeration
 * but it is inherently serial and, measured on this corpus, costs 307.8 ms of the 449 ms serial
 * scan — so a design that needs one serial pass to find its chunk boundaries is capped near 1.4x
 * however many threads it then uses. Page ranges need no pass at all: the page key space is known
 * from {@code maxNodeKey} up front, so N workers can be given disjoint ranges immediately.
 *
 * <p>The cost that has to be checked is the other side of that trade. A record page holds every
 * node, not just array elements — this corpus stores about 15 nodes per element (fields, and the
 * entries of the nested {@code cast}/{@code genres} arrays) — so page scanning inspects roughly
 * 15x more slots than the sibling walk visits elements. That is only affordable because the check
 * is a directory byte read: the parent key is decoded only for slots that are already OBJECTs.
 *
 * <p>Elements are identified as OBJECT nodes whose parent is the array. Both facts come from the
 * page itself, so no node is deserialized to decide.
 *
 * <p>Usage: {@code PageRangeScanProbeMain <storeLocation> <dbName> <resource> [rounds]}
 */
public final class PageRangeScanProbeMain {

  private static final int KIND_OBJECT = NodeKind.OBJECT.getId();

  /** Slots admitted by a parent-key test alone, by node kind — the production filter's blast radius. */
  private static final long[] KIND_TALLY = new long[256];
  private static final String YEAR = "year";

  private PageRangeScanProbeMain() {
  }

  public static void main(final String... args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: PageRangeScanProbeMain <storeLocation> <dbName> <resource> [rounds]");
      System.exit(2);
    }
    final Path location = Paths.get(args[0]);
    final String dbName = args[1];
    final String resource = args[2];
    final int rounds = args.length > 3 ? Integer.parseInt(args[3]) : 3;

    try (final var database = Databases.openJsonDatabase(location.resolve(dbName));
         final JsonResourceSession session = database.beginResourceSession(resource)) {
      final int revision = session.getMostRecentRevisionNumber();

      final long arrayKey;
      final long maxNodeKey;
      final int yearNameKey;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        arrayKey = rtx.getNodeKey();
        maxNodeKey = rtx.getMaxNodeKey();
        yearNameKey = resolveYearNameKey(rtx);
      }
      final long totalPages = (maxNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT) + 1;
      System.out.printf("# array nodeKey=%d, maxNodeKey=%,d, pages=%,d, slots=%,d%n",
                        arrayKey, maxNodeKey, totalPages, totalPages << PageLayout.SLOT_COUNT_EXPONENT);

      final int cores = Runtime.getRuntime().availableProcessors();
      System.out.printf("%-8s | %10s | %10s | %8s | %12s | %s%n",
                        "threads", "best(ms)", "ns/elem", "speedup", "elements", "sum(year)");

      double serialMs = 0.0;
      for (final int threads : new int[] { 1, 2, 4, 8, 12, 16, cores }) {
        double best = Double.MAX_VALUE;
        long[] out = null;
        for (int r = 0; r < rounds; r++) {
          final long t0 = System.nanoTime();
          out = scan(session, revision, arrayKey, totalPages, threads, yearNameKey);
          best = Math.min(best, (System.nanoTime() - t0) / 1e6);
        }
        if (threads == 1) {
          serialMs = best;
        }
        System.out.printf("%-8d | %10.1f | %10.1f | %7.2fx | %,12d | %d%n",
                          threads, best, best * 1e6 / Math.max(1, out[0]), serialMs / best, out[0], out[1]);
      }
      System.out.println("# kinds admitted by the parent-key test alone:");
      for (int k = 0; k < 256; k++) {
        if (KIND_TALLY[k] > 0) {
          System.out.printf("#   kindId=%-4d %-24s %,d%n", k, kindName(k), KIND_TALLY[k]);
        }
      }
    }
  }

  private static String kindName(final int kindId) {
    for (final NodeKind kind : NodeKind.values()) {
      if ((kind.getId() & 0xFF) == kindId) {
        return kind.name();
      }
    }
    return "?";
  }

  private static int resolveYearNameKey(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToFirstChild();
    if (rtx.moveToFirstChild()) {
      do {
        final var name = rtx.getName();
        if (name != null && YEAR.equals(name.getLocalName())) {
          return rtx.getNameKey();
        }
      } while (rtx.moveToRightSibling());
    }
    throw new IllegalStateException("no 'year' field on the first element");
  }

  /** @return {@code [elementCount, sumOfYear]} */
  private static long[] scan(final JsonResourceSession session, final int revision, final long arrayKey,
                             final long totalPages, final int threads, final int yearNameKey)
      throws InterruptedException {
    final AtomicLong elements = new AtomicLong();
    final AtomicLong yearSum = new AtomicLong();
    final long pagesPerThread = (totalPages + threads - 1) / threads;
    final CountDownLatch done = new CountDownLatch(threads);
    final Thread[] workers = new Thread[threads];

    for (int w = 0; w < threads; w++) {
      final long lo = (long) w * pagesPerThread;
      final long hi = Math.min(lo + pagesPerThread, totalPages);
      workers[w] = new Thread(() -> {
        long localElements = 0;
        long localYears = 0;
        final long[] kindTally = new long[256];
        // One reader AND one transaction per worker, both outside the per-page loop: the array
        // slice work found per-chunk transaction open to be the dominant fixed cost, and this is
        // the same mistake one level down.
        try (final var reader = session.createStorageEngineReader(revision);
             final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          for (long pageKey = lo; pageKey < hi; pageKey++) {
            final var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) {
              continue;
            }
            final MemorySegment page = kv.getSlottedPage();
            if (page == null) {
              continue;
            }
            final int populated = PageLayout.getPopulatedCount(page);
            if (populated == 0) {
              continue;
            }
            final long pageBaseNodeKey = PageLayout.getRecordPageKey(page) << PageLayout.SLOT_COUNT_EXPONENT;
            final long[] bitmap = new long[PageLayout.BITMAP_WORDS];
            PageLayout.copyBitmapTo(page, bitmap);

            int seen = 0;
            for (int wordIdx = 0; wordIdx < PageLayout.BITMAP_WORDS && seen < populated; wordIdx++) {
              long word = bitmap[wordIdx];
              while (word != 0) {
                final int slot = (wordIdx << 6) | Long.numberOfTrailingZeros(word);
                word &= word - 1;
                seen++;

                // Tally by kind rather than pre-filtering on OBJECT: the production split must work
                // for arrays of any element type, so the question is which kinds a parent-key test
                // alone admits.
                final int kindId = PageLayout.getDirNodeKindId(page, slot);
                final long nodeKey = pageBaseNodeKey | slot;
                final long parentKey = kv.getSlotParentKey(slot);
                if (parentKey != arrayKey) {
                  continue;
                }
                kindTally[kindId & 0xFF]++;
                if (kindId != KIND_OBJECT) {
                  continue;
                }
                localElements++;
                // Same per-element work as the sibling-walk arm, so the two differ only in how the
                // element was found.
                if (rtx.moveTo(nodeKey) && rtx.moveToFirstChild()) {
                  do {
                    if (rtx.getNameKey() == yearNameKey) {
                      final Number value = rtx.getNumberValue();
                      if (value != null) {
                        localYears += value.longValue();
                      }
                      break;
                    }
                  } while (rtx.moveToRightSibling());
                }
              }
            }
          }
        } finally {
          done.countDown();
        }
        elements.addAndGet(localElements);
        yearSum.addAndGet(localYears);
        synchronized (KIND_TALLY) {
          for (int k = 0; k < 256; k++) {
            KIND_TALLY[k] += kindTally[k];
          }
        }
      }, "pagescan-" + w);
      workers[w].start();
    }
    done.await();
    for (final Thread t : workers) {
      t.join();
    }
    return new long[] { elements.get(), yearSum.get() };
  }
}
