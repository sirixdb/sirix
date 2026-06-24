package io.sirix.query.bench;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.access.trx.node.HashType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Non-JMH scale runner for queries against a Sirix-stored JSON dataset.
 * <p>
 * JMH is unsuitable for record counts in the 100M-1B range because each
 * benchmark/parameter combination forks a fresh JVM and re-runs {@code @Setup}.
 * Re-shredding 1B records per fork would take days. This runner shreds once
 * and times each query in-process.
 *
 * <p>Usage:
 * <pre>
 *   java io.sirix.benchmark.ScaleBenchMain &lt;recordCount&gt; [vectorized=true|false] [iters=N]
 * </pre>
 */
public final class ScaleBenchMain {

  private static final String JSON_DB = "scale-db";
  private static final String JSON_RESOURCE = "records.jn";
  private static final QNm DOC_VAR = new QNm("doc");

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

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: ScaleBenchMain <recordCount> [vectorized=true|false] [iters=N]");
      System.exit(1);
    }
    // iter#08 phase-timing instrumentation (gated by -Dsirix.bench.phaseTiming=true).
    // Emits wall-clock ms deltas between phases so we can attribute the 3 s cold
    // wall to DB-open vs hydrate vs query-run. Zero cost when the flag is off.
    final boolean phaseTiming = Boolean.getBoolean("sirix.bench.phaseTiming");
    final long t0 = System.nanoTime();
    long tPhase = t0;

    long recordCount = Long.parseLong(args[0]);
    boolean vectorized = args.length < 2 || Boolean.parseBoolean(args[1]);
    int iters = args.length < 3 ? 3 : Integer.parseInt(args[2]);

    // Logback level is set via config in native-image build; no-op here to avoid
    // introducing logback-classic as a compile dependency for sirix-query.

    // Lift Sirix's 16 GB default offheap pool — at 100 M records the page-cache
    // pressure during shred otherwise tips over. Cap to host RAM minus heap.
    long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes",
                                                       String.valueOf(24L << 30)));
    var alloc = Allocators.getInstance();
    alloc.init(offheap);
    System.out.printf("# Allocator: %s   maxBufferSize = %d MB (initialized=%s)%n",
                      alloc.getClass().getSimpleName(),
                      alloc.getMaxBufferSize() / (1L << 20), alloc.isInitialized());
    if (phaseTiming) {
      final long now = System.nanoTime();
      System.out.printf("# PHASE allocInit: %,d ms (t=%,d ms)%n",
          (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
      tPhase = now;
    }

    // Re-use an existing shredded database when -Dsirix.db=/path is supplied —
    // shredding 100 M records takes ~7 minutes, and we want to iterate on
    // query-side optimizations without paying that cost each time.
    //
    // Force a fresh shred at a specific location via -Dsirix.shredDbPath=/path
    // (the path is created if it doesn't exist). This is the counterpart to
    // -Dsirix.db=/path which triggers reuse.
    String reuseDb = System.getProperty("sirix.db");
    String forceShredPath = System.getProperty("sirix.shredDbPath");
    final Path dbDir;
    final boolean shredNeeded;
    if (reuseDb != null) {
      dbDir = Path.of(reuseDb);
      shredNeeded = false;
    } else if (forceShredPath != null) {
      dbDir = Path.of(forceShredPath);
      Files.createDirectories(dbDir);
      shredNeeded = true;
    } else {
      dbDir = Files.createTempDirectory("sirix-scale-bench");
      shredNeeded = true;
    }
    System.out.printf("# Records: %,d   Vectorized: %s   Iters: %d   DB: %s   Offheap: %d MB   Reuse: %s%n",
                      recordCount, vectorized, iters, dbDir, offheap / (1L << 20), !shredNeeded);

    // Smaller auto-commit window keeps offheap segments getting recycled
    // during the shred phase. Default is 1 M nodes — too coarse for 100 M+ records.
    int autoCommit = Integer.parseInt(System.getProperty("sirix.autoCommit.nodes", "131072"));
    // Fast-shred defaults: path summary + rolling hash add ~40% CPU during
    // shred and are not needed for analytical queries. Override via
    // -DbuildPathSummary=true / -DhashType=ROLLING if needed.
    boolean pathSummary = Boolean.parseBoolean(System.getProperty("buildPathSummary", "false"));
    // PathStatistics enables the aggregate short-circuit. Requires pathSummary=true.
    // Default off; opt-in via -DbuildPathStatistics=true. Target win:
    // sumAge/avgAge/minMaxAge drop from seconds to microseconds on large datasets.
    boolean pathStatistics = Boolean.parseBoolean(System.getProperty("buildPathStatistics", "false"));
    if (pathStatistics && !pathSummary) {
      // Force pathSummary on if stats requested, otherwise the store builder throws.
      pathSummary = true;
      System.out.println("# buildPathStatistics=true implies buildPathSummary=true");
    }
    // When the projection index is requested but we're shredding a fresh DB
    // (not reusing one), the slow-path builder in ScaleBenchProjectionSetup
    // walks the PathSummary to resolve the projected paths. Without a
    // pathSummary the whole projection=true run dies with an opaque
    // `openPathSummary failed` error during install. Auto-force pathSummary
    // on just like we do for pathStatistics so the common combo Just Works.
    if (Boolean.getBoolean("projection") && !pathSummary) {
      pathSummary = true;
      System.out.println("# projection=true implies buildPathSummary=true");
    }
    HashType hash = HashType.fromString(System.getProperty("hashType", "NONE"));
    BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
        .location(dbDir)
        .numberOfNodesBeforeAutoCommit(autoCommit)
        .buildPathSummary(pathSummary)
        .buildPathStatistics(pathStatistics)
        .hashType(hash)
        .build();
    System.out.printf("# pathSummary=%s  pathStatistics=%s%n", pathSummary, pathStatistics);
    if (phaseTiming) {
      final long now = System.nanoTime();
      System.out.printf("# PHASE storeBuild: %,d ms (t=%,d ms)%n",
          (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
      tPhase = now;
    }
    SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
    SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
    if (phaseTiming) {
      final long now = System.nanoTime();
      System.out.printf("# PHASE ctxChain: %,d ms (t=%,d ms)%n",
          (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
      tPhase = now;
    }

    if (shredNeeded) {
      long shredStart = System.nanoTime();
      try (Reader src = new GeneratedRecordsReader(0, recordCount, 42L);
           JsonReader jr = new JsonReader(src)) {
        store.create(JSON_DB, JSON_RESOURCE, jr);
      }
      long shredMs = (System.nanoTime() - shredStart) / 1_000_000L;
      System.out.printf("# Shred: %,d ms (%.0f records/sec)%n",
                        shredMs, recordCount * 1000.0 / Math.max(1, shredMs));
    } else {
      System.out.println("# Shred: skipped (re-using existing DB)");
    }

    JsonResourceSession session = null;
    SirixVectorizedExecutor vec = null;
    if (vectorized) {
      JsonDBCollection coll = (JsonDBCollection) store.lookup(JSON_DB);
      if (phaseTiming) {
        final long now = System.nanoTime();
        System.out.printf("# PHASE lookup: %,d ms (t=%,d ms)%n",
            (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
        tPhase = now;
      }
      session = coll.getDatabase().beginResourceSession(JSON_RESOURCE);
      if (phaseTiming) {
        final long now = System.nanoTime();
        System.out.printf("# PHASE beginSession: %,d ms (t=%,d ms)%n",
            (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
        tPhase = now;
      }
      // Default = all cores. Overridable via -Dsirix.vec.threads=N — useful
      // when a concurrency bug in Sirix's JVMCI-compiled allocator / page
      // combiner path triggers at high fan-out.
      int vecThreads = Integer.parseInt(
          System.getProperty("sirix.vec.threads",
              String.valueOf(Runtime.getRuntime().availableProcessors())));
      vec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), vecThreads);
      SequentialPipelineStrategy.setVectorizedExecutor(vec);
      System.out.printf("# Vec threads: %d%n", vecThreads);
      if (phaseTiming) {
        final long now = System.nanoTime();
        System.out.printf("# PHASE vecExecutor: %,d ms (t=%,d ms)%n",
            (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
        tPhase = now;
      }
    }

    // -Dprojection=true installs a covering projection index on
    // (age, active, dept) so filterCount / compoundAndFilterCount route
    // through ProjectionIndexByteScan instead of the generic predicate path.
    // Projection install is expensive (walks the whole DB to build path-scoped
    // index). Skip it when we're only shredding for DB-size measurement
    // (iters <= 0) — queries aren't going to run anyway.
    if (Boolean.getBoolean("projection") && session != null && iters > 0) {
      final long tBuild = System.nanoTime();
      final int leafCount = ScaleBenchProjectionSetup.installWildcard(session);
      System.out.printf("# Projection index: %,d leaves, built in %,d ms%n",
          leafCount, (System.nanoTime() - tBuild) / 1_000_000L);
      if (phaseTiming) tPhase = System.nanoTime();
    }

    JsonDBCollection coll = (JsonDBCollection) store.lookup(JSON_DB);
    JsonDBItem docItem = (JsonDBItem) coll.getDocument();
    ctx.bind(DOC_VAR, (Sequence) docItem);
    if (phaseTiming) {
      final long now = System.nanoTime();
      System.out.printf("# PHASE docBind: %,d ms (t=%,d ms)%n",
          (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
      tPhase = now;
    }

    // iters <= 0 skips the query-running phase entirely. Useful when we
    // only care about the shred-side DB size / encoder diagnostics and
    // don't want to pay the projection-index + per-query cost.
    if (iters <= 0) {
      System.out.println("# Iters <= 0: skipping query phase.");
    } else {
      System.out.printf("%-26s | %10s | %10s | %10s | %10s%n", "query", "min(ms)", "avg(ms)", "max(ms)", "result_bytes");
      System.out.printf("%-26s + %10s + %10s + %10s + %10s%n", "--------------------------",
                        "----------", "----------", "----------", "------------");

      for (Map.Entry<String, String> e : QUERIES.entrySet()) {
        runQueryRepeated(chain, ctx, e.getKey(), e.getValue(), iters);
      }
    }
    if (phaseTiming) {
      final long now = System.nanoTime();
      System.out.printf("# PHASE queries: %,d ms (t=%,d ms)%n",
          (now - tPhase) / 1_000_000L, (now - t0) / 1_000_000L);
      tPhase = now;
    }

    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (vec != null) vec.close();
    if (session != null) session.close();
    chain.close();
    store.close();
    // Default: keep the DB around for re-use via -Dsirix.db=<path>. Set
    // -Dsirix.db.cleanup=true to delete the freshly shredded DB at the end
    // (useful for CI where disk space is scarce).
    boolean cleanup = Boolean.parseBoolean(System.getProperty("sirix.db.cleanup", "false"));
    if (shredNeeded && cleanup) {
      Databases.removeDatabase(dbDir);
    } else if (shredNeeded) {
      System.out.printf("# DB preserved at %s (set -Dsirix.db.cleanup=true to delete)%n", dbDir);
    }
  }

  private static void runQueryRepeated(SirixCompileChain chain, SirixQueryContext ctx,
                                        String name, String body, int iters) {
    String wrapped = "declare variable $doc external; " + body;

    // Warm up: enough invocations to let HotSpot tier-up the query path.
    // For very large datasets each call is expensive, so cap warmup time.
    // -Dsirix.noWarmup=true skips warmup so the first measured iter is a
    // true cold run (no executor-level result cache pre-seeded).
    final boolean noWarmup = Boolean.getBoolean("sirix.noWarmup");
    int warmupCount = noWarmup ? 0 : Math.max(3, Math.min(20, iters));
    long warmDeadline = System.nanoTime() + 5_000_000_000L; // 5s budget
    for (int i = 0; i < warmupCount && System.nanoTime() < warmDeadline; i++) {
      runOnce(chain, ctx, wrapped);
    }

    long min = Long.MAX_VALUE, max = 0, sum = 0;
    int bytes = 0;
    final boolean variedLiteral = Boolean.getBoolean("sirix.bench.variedLiteral");
    for (int i = 0; i < iters; i++) {
      final String wrappedForIter;
      if (variedLiteral) {
        final int lowJit = i;
        wrappedForIter = wrapped
            .replace("$u.age > 40", "$u.age > " + (25 + lowJit))
            .replace("$u.age > 30", "$u.age > " + (22 + lowJit))
            .replace("$u.age < 50", "$u.age < " + (55 + lowJit));
      } else {
        wrappedForIter = wrapped;
      }
      long t0 = System.nanoTime();
      bytes = runOnce(chain, ctx, wrappedForIter);
      long elapsed = System.nanoTime() - t0;
      sum += elapsed;
      if (elapsed < min) min = elapsed;
      if (elapsed > max) max = elapsed;
    }
    double minMs = min / 1e6;
    double maxMs = max / 1e6;
    double avgMs = (sum / (double) iters) / 1e6;
    System.out.printf("%-26s | %10.3f | %10.3f | %10.3f | %,10d%n",
                      name, minMs, avgMs, maxMs, bytes);
  }

  private static int runOnce(SirixCompileChain chain, SirixQueryContext ctx, String wrapped) {
    var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, wrapped).execute(ctx));
    }
    return buf.toString().length();
  }
}
