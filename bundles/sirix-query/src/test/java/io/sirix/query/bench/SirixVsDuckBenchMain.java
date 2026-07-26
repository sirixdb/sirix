package io.sirix.query.bench;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Apples-to-apples per-query timing against a preserved scale DB for the
 * DuckDB comparison. Differences from {@link ScaleBenchMain}'s query phase:
 * a FRESH {@link SirixVectorizedExecutor} is created for every iteration, so
 * the executor-level RESULT caches never serve a timed run (DuckDB does not
 * cache query results either) — the store, page caches, and the projection
 * index are shared across iterations, mirroring DuckDB's loaded table.
 *
 * Usage: SirixVsDuckBenchMain &lt;dbDir&gt; [iters=3] [threads=cores] [projection=true|false]
 *
 * <p>projection=false runs the region/scan fast paths only. It must run in a
 * process that never installed the projection (the registry is static per
 * resource), i.e. before — or instead of — a projection=true run.
 */
public final class SirixVsDuckBenchMain {

  private static final Map<String, String> QUERIES = new LinkedHashMap<>();
  static {
    QUERIES.put("filterCount",            "count(for $u in $doc[] where $u.age > 40 and $u.active return $u)");
    QUERIES.put("groupByDept",            "for $u in $doc[] let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
    QUERIES.put("sumAge",                 "sum(for $u in $doc[] return $u.age)");
    QUERIES.put("avgAge",                 "avg(for $u in $doc[] return $u.age)");
    QUERIES.put("minMaxAge",              "{\"min\": min(for $u in $doc[] return $u.age), \"max\": max(for $u in $doc[] return $u.age)}");
    QUERIES.put("groupBy2Keys",           "for $u in $doc[] let $d := $u.dept, $c := $u.city group by $d, $c return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
    QUERIES.put("filterGroupBy",          "for $u in $doc[] where $u.active let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
    QUERIES.put("countDistinct",          "count(for $u in $doc[] let $d := $u.dept group by $d return $d)");
    QUERIES.put("compoundAndFilterCount", "count(for $u in $doc[] where $u.age > 30 and $u.age < 50 and $u.active return $u)");
  }

  public static void main(final String[] args) throws Exception {
    final Path dbDir = Path.of(args[0]);
    final int iters = args.length > 1 ? Integer.parseInt(args[1]) : 3;
    final int threads = args.length > 2
        ? Integer.parseInt(args[2])
        : Runtime.getRuntime().availableProcessors();
    final boolean projection = args.length <= 3 || Boolean.parseBoolean(args[3]);

    final long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(24L << 30)));
    final var alloc = io.sirix.cache.Allocators.getInstance();
    alloc.init(offheap);

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection coll = (JsonDBCollection) store.lookup("scale-db");
      final JsonDBItem docItem = (JsonDBItem) coll.getDocument();
      ctx.bind(new QNm("doc"), (Sequence) docItem);
      final var session = docItem.getTrx().getResourceSession();
      final int rev = session.getMostRecentRevisionNumber();

      if (projection) {
        final long tProj = System.nanoTime();
        final int leaves = ScaleBenchProjectionSetup.installWildcard(session);
        System.out.printf("# projection: %,d leaves in %,d ms; threads=%d, iters=%d%n",
                          leaves, (System.nanoTime() - tProj) / 1_000_000, threads, iters);
      } else {
        System.out.printf("# projection: DISABLED (region/scan paths); threads=%d, iters=%d%n", threads, iters);
      }

      System.out.printf("%-26s | %10s | %10s | %12s%n", "query", "min(ms)", "avg(ms)", "result_bytes");
      for (final var e : QUERIES.entrySet()) {
        long best = Long.MAX_VALUE;
        long total = 0;
        int bytes = 0;
        // one untimed warmup with a throwaway executor (JIT + page cache)
        bytes = runOnce(session, rev, threads, chain, ctx, e.getValue());
        for (int i = 0; i < iters; i++) {
          final long t0 = System.nanoTime();
          bytes = runOnce(session, rev, threads, chain, ctx, e.getValue());
          final long ms = (System.nanoTime() - t0) / 1_000_000;
          best = Math.min(best, ms);
          total += ms;
        }
        System.out.printf("%-26s | %10d | %10d | %12d%n", e.getKey(), best, total / iters, bytes);
      }
    }
    System.exit(0);
  }

  private static int runOnce(final io.sirix.api.json.JsonResourceSession session, final int rev, final int threads,
      final SirixCompileChain chain, final SirixQueryContext ctx, final String query) throws Exception {
    final SirixVectorizedExecutor vec = new SirixVectorizedExecutor(session, rev, threads);
    SequentialPipelineStrategy.setVectorizedExecutor(vec);
    try {
      // Statically-resolvable source (matches ScaleBenchMain): brackit traces the FLWOR
      // let-binding to a DOCUMENT SourceRef the executor's acceptsSource gate can verify; an
      // external variable annotates as UNKNOWN since brackit 1.0-alpha9 and silently declines
      // every query to the generic pipeline.
      final Sequence result = new Query(chain,
          "let $doc := jn:doc('scale-db','records.jn') return (" + query + ")").execute(ctx);
      final StringWriter out = new StringWriter();
      try (PrintWriter pw = new PrintWriter(out)) {
        new StringSerializer(pw).serialize(result);
      }
      return out.getBuffer().length();
    } finally {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      vec.close();
    }
  }
}
