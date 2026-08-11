package io.sirix.benchmark;

import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the parallel ceiling of SirixDB's record-level scan, so a pipeline redesign can be
 * costed before it is written.
 *
 * <p>
 * The question this answers is narrow and load-bearing. Profiling
 * {@code count(for $m in $doc[] where $m.year > 1990 return $m)} shows the leaf scan is 58-59 % of
 * the pipeline, so any morsel design that leaves the leaf serial is capped near 1.7x by Amdahl.
 * Making the leaf parallel is therefore the whole game — but only if SirixDB's storage layer
 * actually scales when N threads walk disjoint parts of the same array. {@code JsonDBArraySlice}'s
 * parallel materialization reached only 2.1-3.0x on 19 cores, which would not be enough, and the
 * reasons recorded for that ceiling (per-call transaction open, wrapper allocation) are avoidable
 * here. This harness separates the two so the difference is attributable.
 *
 * <p>
 * Phase 1 walks the array's children once, serially, and captures their node keys. That walk is NOT
 * what is being measured — it exists only to give every arm the identical work list, so the arms
 * differ in thread count and nothing else.
 *
 * <p>
 * Phase 2 replays that work list: for each element, move to it and read its {@code year} field,
 * which is the same navigation the generic pipeline performs per tuple. One transaction per worker
 * is opened outside the timed region, because a per-chunk transaction open was the dominant fixed
 * cost the array-slice work ran into and repeating that mistake would measure it instead of the
 * scan.
 *
 * <p>
 * Usage: {@code ParallelScanCeilingMain <storeLocation> <dbName> <resource> [rounds]}
 */
public final class ParallelScanCeilingMain {

  private static final String YEAR = "year";

  private ParallelScanCeilingMain() {}

  public static void main(final String... args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: ParallelScanCeilingMain <storeLocation> <dbName> <resource> [rounds]");
      System.exit(2);
    }
    final Path location = Paths.get(args[0]);
    final String dbName = args[1];
    final String resource = args[2];
    final int rounds = args.length > 3
        ? Integer.parseInt(args[3])
        : 3;

    try (final var database = Databases.openJsonDatabase(location.resolve(dbName));
        final JsonResourceSession session = database.beginResourceSession(resource)) {
      final int revision = session.getMostRecentRevisionNumber();

      final long[] keys;
      final int yearKey;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        keys = collectElementKeys(rtx);
        describeFirstElement(rtx, keys);
        yearKey = yearNameKey(rtx, keys);
      }
      System.out.printf("# %,d elements, revision %d%n", keys.length, revision);

      // The bare sibling walk, timed on its own. This is the cost of the ONE serial pass an
      // index-range split would need to locate its chunk boundaries, so it is the Amdahl floor for
      // that design: if it approaches the parallel arm's runtime, index splitting is not viable and
      // the split has to come from somewhere that needs no walk.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        double bestWalk = Double.MAX_VALUE;
        long counted = 0;
        for (int r = 0; r < rounds; r++) {
          final long t0 = System.nanoTime();
          counted = walkSiblingsOnly(rtx);
          bestWalk = Math.min(bestWalk, (System.nanoTime() - t0) / 1e6);
        }
        System.out.printf("# bare sibling walk: %,.1f ms (%.1f ns/elem) over %,d elements%n", bestWalk,
            bestWalk * 1e6 / keys.length, counted);
      }

      final int cores = Runtime.getRuntime().availableProcessors();
      System.out.printf("%-8s | %10s | %10s | %8s | %s%n", "threads", "best(ms)", "ns/elem", "speedup", "checksum");

      double serialMs = 0.0;
      for (final int threads : new int[] {1, 2, 4, 8, 12, 16, cores}) {
        double best = Double.MAX_VALUE;
        long checksum = 0;
        for (int r = 0; r < rounds; r++) {
          final long t0 = System.nanoTime();
          checksum = sumYears(session, revision, keys, threads, yearKey);
          best = Math.min(best, (System.nanoTime() - t0) / 1e6);
        }
        if (threads == 1) {
          serialMs = best;
        }
        System.out.printf("%-8d | %10.1f | %10.1f | %7.2fx | %d%n", threads, best, best * 1e6 / keys.length,
            serialMs / best, checksum);
      }
    }
  }

  /**
   * Dumps the first element's field names and kinds.
   *
   * <p>
   * A scan probe that navigates but never matches still produces plausible timings — it just measures
   * a different walk than the one it claims to. Printing what the first element actually looks like
   * is how a zero checksum gets attributed rather than explained away.
   */
  private static void describeFirstElement(final JsonNodeReadOnlyTrx rtx, final long[] keys) {
    if (keys.length == 0) {
      return;
    }
    rtx.moveTo(keys[0]);
    System.out.printf("# first element: kind=%s%n", rtx.getKind());
    if (!rtx.moveToFirstChild()) {
      return;
    }
    do {
      System.out.printf("#   child kind=%-20s nameKey=%-6d name=%s%n", rtx.getKind(), rtx.getNameKey(), rtx.getName());
    } while (rtx.moveToRightSibling());
  }

  /**
   * Node keys of the document array's element children, in document order.
   *
   * <p>
   * Grown geometrically rather than sized from {@code getChildCount()}: the count is available here,
   * but a probe that silently depends on it would break on any array whose count is not maintained,
   * and this list is built once outside the measurement either way.
   */
  private static long[] collectElementKeys(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    if (!rtx.isArray()) {
      throw new IllegalStateException("expected a top-level array, found " + rtx.getKind());
    }
    long[] keys = new long[1 << 20];
    int n = 0;
    if (rtx.moveToFirstChild()) {
      do {
        if (n == keys.length) {
          final long[] bigger = new long[keys.length << 1];
          System.arraycopy(keys, 0, bigger, 0, n);
          keys = bigger;
        }
        keys[n++] = rtx.getNodeKey();
      } while (rtx.moveToRightSibling());
    }
    final long[] exact = new long[n];
    System.arraycopy(keys, 0, exact, 0, n);
    return exact;
  }

  /** Walks the array's children and touches nothing else; returns the count seen. */
  private static long walkSiblingsOnly(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    long n = 0;
    if (rtx.moveToFirstChild()) {
      do {
        n++;
      } while (rtx.moveToRightSibling());
    }
    return n;
  }

  /**
   * Sums {@code year} over {@code keys}, split into {@code threads} contiguous chunks.
   *
   * <p>
   * Contiguous rather than round-robin on purpose: elements adjacent in the array share record pages,
   * so a contiguous chunk gives each worker its own pages and makes the arms differ in parallelism
   * rather than in cache behaviour.
   */
  private static long sumYears(final JsonResourceSession session, final int revision, final long[] keys,
      final int threads, final int yearNameKey) throws InterruptedException {
    if (threads == 1) {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        return sumRange(rtx, keys, 0, keys.length, yearNameKey);
      }
    }
    final AtomicLong total = new AtomicLong();
    final CountDownLatch done = new CountDownLatch(threads);
    final int chunk = (keys.length + threads - 1) / threads;
    final Thread[] workers = new Thread[threads];
    for (int w = 0; w < threads; w++) {
      final int lo = w * chunk;
      final int hi = Math.min(lo + chunk, keys.length);
      workers[w] = new Thread(() -> {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          total.addAndGet(sumRange(rtx, keys, lo, hi, yearNameKey));
        } finally {
          done.countDown();
        }
      }, "scan-" + w);
      workers[w].start();
    }
    done.await();
    return total.get();
  }

  /**
   * Move to each element and read its {@code year} — the navigation the pipeline does per tuple.
   *
   * <p>
   * Fields are stored as FUSED nodes ({@code OBJECT_NAMED_NUMBER} and friends): the name and the
   * value live on one node, so the value is read from the matched node itself rather than from a
   * child. Matching is on the interned name key, not a decoded {@code QNm}, because {@code getName()}
   * allocates per field visited and would put string decoding into a measurement that is supposed to
   * be about navigation.
   */
  private static long sumRange(final JsonNodeReadOnlyTrx rtx, final long[] keys, final int lo, final int hi,
      final int yearNameKey) {
    long sum = 0;
    for (int i = lo; i < hi; i++) {
      if (!rtx.moveTo(keys[i]) || !rtx.moveToFirstChild()) {
        continue;
      }
      do {
        if (rtx.getNameKey() == yearNameKey) {
          final Number value = rtx.getNumberValue();
          if (value != null) {
            sum += value.longValue();
          }
          break;
        }
      } while (rtx.moveToRightSibling());
    }
    return sum;
  }

  /** The interned name key for {@code year}, read off the first element. */
  private static int yearNameKey(final JsonNodeReadOnlyTrx rtx, final long[] keys) {
    rtx.moveTo(keys[0]);
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
}
