package io.sirix.query.bench.clickbench;

import io.brackit.query.Query;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.cache.Allocators;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;

import org.jspecify.annotations.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Differential gate for the partition-decomposed ClickBench queries: proves, for each of the 43
 * queries, that the composite (partitioned) formulation computes exactly the single-resource
 * answer.
 *
 * <p>
 * Three arms per query, all compared as ORDER-INSENSITIVE multisets of serialized items (full
 * lists, no order/limit — comparing every group and every aggregate validates the merge algebra
 * completely while sidestepping top-k boundary-tie nondeterminism):
 * <ol>
 * <li>ORIGINAL: the production query text minus its {@code subsequence} wrapper, on the
 * single-resource corpus — the ground truth.</li>
 * <li>SPEC-SINGLE (spec-generated queries only): the spec's own single-resource formulation. A
 * mismatch against (1) means the spec mis-models the query — without this arm a modeling error
 * shared by both generated texts would pass silently.</li>
 * <li>COMPOSITE: the merged partitioned formulation on the partitioned corpus.</li>
 * </ol>
 *
 * <p>
 * Usage: {@code ClickBenchCompositeDifferentialMain <location> <singleDb> <singleResource>
 * <compositeDb> <partitions>} — both databases must hold the SAME rows (same generator seed) and
 * are opened through one store rooted at {@code location}. Also times the composite PRODUCTION
 * texts (2 tries) and reports the served-counter delta, because a decomposition that answers
 * correctly from the generic pipeline is a correctness success and a performance failure.
 */
public final class ClickBenchCompositeDifferentialMain {

  private ClickBenchCompositeDifferentialMain() {}

  public static void main(final String[] args) {
    requireValidArgs(args);
    final boolean timingsOnly = args.length == 6 && "--timings-only".equals(args[5]);
    // --union: arm C is the ORIGINAL query text against the LOGICAL UNION resource of the
    // partitioned database (catalog-resolved), not the decomposed composite texts — the gate for
    // the engine-side union that makes a partitioned load protocol-legitimate.
    final boolean unionMode = args.length == 6 && "--union".equals(args[5]);
    final Path location = Path.of(args[0]);
    final String singleDb = args[1];
    final String singleResource = args[2];
    final String compositeDb = args[3];
    final int partitions = Integer.parseInt(args[4]);
    // The store would otherwise size its off-heap arenas from the shipped default (24 GiB), which
    // together with the comparison heap got the first run of this main OOM-killed at Q24.
    Allocators.getInstance().init(Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(6L << 30))));

    final List<ClickBenchCompositeQueries.CompositeQuery> composites =
        ClickBenchCompositeQueries.all(compositeDb, "hits", partitions, singleDb, singleResource);
    final List<ClickBenchQueries.Query> originals = ClickBenchQueries.all();

    int failures = 0;
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      System.out.printf("%-4s | %7s | %9s | %s%n", "q", "rows", "verdict", "note");
      for (int q = 0; !timingsOnly && q < 43; q++) {
        if (compareOneQuery(chain, ctx, composites.get(q), originals.get(q), q, singleDb, singleResource, compositeDb,
            unionMode)) {
          failures++;
        }
      }

      if (unionMode) {
        System.out.println(failures == 0
            ? "UNION DIFFERENTIAL PASS: 43/43"
            : "UNION DIFFERENTIAL FAIL: " + failures + " of 43");
        System.exit(failures == 0
            ? 0
            : 1);
      }
      System.out.println();
      System.out.println("# composite production timings (2 tries each):");
      final long served0 = servedTotal();
      System.out.printf("%-4s | %10s | %10s | %6s%n", "q", "try1(s)", "try2(s)", "served");
      for (int q = 0; q < 43; q++) {
        final String text = composites.get(q).compositeProduction();
        final long servedBefore = servedTotal();
        double t1 = -1;
        double t2 = -1;
        String note = "";
        try {
          t1 = timeOnce(chain, ctx, text);
          t2 = timeOnce(chain, ctx, text);
        } catch (final RuntimeException e) {
          note = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
        }
        System.out.printf(Locale.ROOT, "%-4d | %10.3f | %10.3f | %6d %s%n", q, t1, t2, servedTotal() - servedBefore,
            note);
      }
      System.out.printf("# production served delta: %d (43 queries x %d legs x 2 tries = %d max)%n",
          servedTotal() - served0, partitions, 43 * partitions * 2);
    }
    System.out.println(failures == 0
        ? "DIFFERENTIAL PASS: 43/43"
        : "DIFFERENTIAL FAIL: " + failures + " of 43");
    System.exit(failures == 0
        ? 0
        : 1);
  }

  /** Rejects an argument vector this main cannot run, printing the usage line first. */
  private static void requireValidArgs(final String[] args) {
    if (args.length != 5 && !(args.length == 6 && ("--timings-only".equals(args[5]) || "--union".equals(args[5])))) {
      System.err.println("usage: ClickBenchCompositeDifferentialMain <location> <singleDb> <singleResource>"
          + " <compositeDb> <partitions> [--timings-only|--union]");
      System.exit(2);
    }
  }

  /**
   * Compare one query's arms and print its row. Returns {@code true} when the query failed, so the
   * caller only has to accumulate.
   */
  private static boolean compareOneQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final ClickBenchCompositeQueries.CompositeQuery composite, final ClickBenchQueries.Query original, final int q,
      final String singleDb, final String singleResource, final String compositeDb, final boolean unionMode) {
    String note = "";
    String verdict;
    int rows = -1;
    boolean failed = false;
    try {
      final String originalFull =
          ClickBenchQueries.wrap(singleDb, singleResource, stripSubsequence(original.jsoniq(0)));
      final ArmDigest truth = items(chain, ctx, originalFull);
      rows = (int) truth.count();
      final String specDiff = specArmDiff(chain, ctx, composite, truth, unionMode);
      if (specDiff != null) {
        System.out.printf("%-4d | %7d | %9s | %s%n", q, rows, "SPEC-FAIL", specDiff);
        return true;
      }
      final ArmDigest merged = items(chain, ctx, unionMode
          ? ClickBenchQueries.wrap(compositeDb, "hits", stripSubsequence(original.jsoniq(0)))
          : composite.compositeFull());
      final String mergeDiff = diff(truth, merged);
      if (mergeDiff != null) {
        verdict = "FAIL";
        note = mergeDiff;
        failed = true;
      } else {
        verdict = "PASS";
      }
    } catch (final RuntimeException e) {
      verdict = "ERROR";
      note = e.getClass().getSimpleName() + ": " + firstLine(e.getMessage());
      failed = true;
    }
    System.out.printf("%-4d | %7d | %9s | %s%n", q, rows, verdict, note);
    return failed;
  }

  /**
   * The spec arm's divergence from truth, or {@code null} when there is no spec arm to check (union
   * mode, or a query with no single-resource spec formulation) or it agrees.
   */
  private static @Nullable String specArmDiff(final SirixCompileChain chain, final SirixQueryContext ctx,
      final ClickBenchCompositeQueries.CompositeQuery composite, final ArmDigest truth, final boolean unionMode) {
    if (unionMode || composite.singleFull() == null) {
      return null;
    }
    return diff(truth, items(chain, ctx, composite.singleFull()));
  }

  /**
   * Streamed order-insensitive multiset digest of a result. Small results additionally keep the
   * serialized rows so a mismatch can name its first difference; large results are compared by
   * {@code (count, ΣhashA, ΣhashB)} alone — two independent 64-bit per-item hashes summed, which is
   * multiset-invariant and holds nothing in memory (the first run of this gate was OOM-killed holding
   * 147k serialized rows per arm).
   */
  private record ArmDigest(long count, long sumA, long sumB, List<String> rows) {
  }

  /** Full lists (with diff detail) are kept only below this row count. */
  private static final int DETAIL_ROWS = 100_000;

  private static ArmDigest items(final SirixCompileChain chain, final SirixQueryContext ctx, final String text) {
    final Sequence result = new Query(chain, text).execute(ctx);
    long count = 0;
    long sumA = 0;
    long sumB = 0;
    List<String> rows = new ArrayList<>(1024);
    if (result != null) {
      try (final Iter iter = result.iterate()) {
        Item item;
        while ((item = iter.next()) != null) {
          final StringWriter buffer = new StringWriter(128);
          try (PrintWriter writer = new PrintWriter(buffer)) {
            new StringSerializer(writer).serialize(item);
          }
          final String row = buffer.toString().strip();
          count++;
          sumA += row.hashCode();
          sumB += fnv1a64(row);
          if (rows != null) {
            rows.add(row);
            if (rows.size() > DETAIL_ROWS) {
              rows = null;
            }
          }
        }
      }
    }
    return new ArmDigest(count, sumA, sumB, rows);
  }

  private static long fnv1a64(final String value) {
    long hash = 0xcbf29ce484222325L;
    for (int i = 0; i < value.length(); i++) {
      hash = (hash ^ value.charAt(i)) * 0x100000001b3L;
    }
    return hash;
  }

  /** Order-insensitive multiset comparison; null when equal, else a compact first-difference note. */
  private static String diff(final ArmDigest expected, final ArmDigest actual) {
    if (expected.count() != actual.count()) {
      return "row count " + actual.count() + " != expected " + expected.count();
    }
    if (expected.rows() != null && actual.rows() != null) {
      final List<String> left = new ArrayList<>(expected.rows());
      final List<String> right = new ArrayList<>(actual.rows());
      Collections.sort(left);
      Collections.sort(right);
      for (int i = 0; i < left.size(); i++) {
        if (!left.get(i).equals(right.get(i))) {
          return "first diff at sorted row " + i + ": expected " + clip(left.get(i)) + " got " + clip(right.get(i));
        }
      }
      return null;
    }
    if (expected.sumA() != actual.sumA() || expected.sumB() != actual.sumB()) {
      return "multiset hash mismatch over " + actual.count() + " rows (rerun with detail for the rows)";
    }
    return null;
  }

  private static double timeOnce(final SirixCompileChain chain, final SirixQueryContext ctx, final String text) {
    final long start = System.nanoTime();
    final Sequence result = new Query(chain, text).execute(ctx);
    if (result != null) {
      try (final Iter iter = result.iterate()) {
        while (iter.next() != null) {
          // Materialize fully; timing must include the work, not just plan construction.
        }
      }
    }
    return (System.nanoTime() - start) / 1e9;
  }

  /** Unwraps the outermost {@code subsequence(BODY, o, n)} so the full list is compared. */
  private static String stripSubsequence(final String body) {
    final String trimmed = body.strip();
    if (!trimmed.startsWith("subsequence(")) {
      return trimmed;
    }
    final int close = trimmed.lastIndexOf(')');
    final int secondComma = trimmed.lastIndexOf(',', trimmed.lastIndexOf(',', close) - 1);
    return trimmed.substring("subsequence(".length(), secondComma).strip();
  }

  private static long servedTotal() {
    return SirixVectorizedExecutor.projectionCountsServed() + SirixVectorizedExecutor.groupAggServedCount()
        + SirixVectorizedExecutor.numericGroupByServedCount() + SirixVectorizedExecutor.groupAggSlicedServedCount()
        + SirixVectorizedExecutor.sortedScanServedCount() + SirixVectorizedExecutor.predicateScanServedCount()
        + SirixVectorizedExecutor.predicateValueEmissionsServedCount();
  }

  private static String firstLine(final String message) {
    if (message == null) {
      return "";
    }
    final int newline = message.indexOf('\n');
    return newline < 0
        ? message
        : message.substring(0, newline);
  }

  private static String clip(final String value) {
    return value.length() > 120
        ? value.substring(0, 120) + "…"
        : value;
  }
}
