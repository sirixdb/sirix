package io.sirix.benchmark;

import ch.qos.logback.classic.Logger;
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
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

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
 *   java io.sirix.benchmark.BrackitQueryOnSirixScaleMain &lt;recordCount&gt; [vectorized=true|false] [iters=N]
 * </pre>
 */
public final class BrackitQueryOnSirixScaleMain {

  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };
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
    // Numeric-predicate variant that Brackit routes to executeFilteredGroupByCount.
    // The default impl silently drops the filter; our override implements a real
    // filter-then-group scan. Kept here as a ground-truth correctness check.
    QUERIES.put("filterGroupByAge",       "for $u in $doc[] where $u.age > 40 let $d := $u.dept group by $d return {\"dept\": $d, \"count\": count($u)}");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: BrackitQueryOnSirixScaleMain <recordCount> [vectorized=true|false] [iters=N]");
      System.exit(1);
    }
    long recordCount = Long.parseLong(args[0]);
    boolean vectorized = args.length < 2 || Boolean.parseBoolean(args[1]);
    int iters = args.length < 3 ? 3 : Integer.parseInt(args[2]);

    final Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(ch.qos.logback.classic.Level.WARN);

    // Lift Sirix's 16 GB default offheap pool — at 100 M records the page-cache
    // pressure during shred otherwise tips over. Cap to host RAM minus heap.
    long offheap = Long.parseLong(System.getProperty("sirix.offheap.bytes",
                                                       String.valueOf(24L << 30)));
    var alloc = Allocators.getInstance();
    alloc.init(offheap);
    System.out.printf("# Allocator: %s   maxBufferSize = %d MB (initialized=%s)%n",
                      alloc.getClass().getSimpleName(),
                      alloc.getMaxBufferSize() / (1L << 20), alloc.isInitialized());

    // Re-use an existing shredded database when -Dsirix.db=/path is supplied —
    // shredding 100 M records takes ~7 minutes, and we want to iterate on
    // query-side optimizations without paying that cost each time.
    String reuseDb = System.getProperty("sirix.db");
    final boolean shredNeeded = reuseDb == null;
    Path dbDir = reuseDb != null ? Path.of(reuseDb) : Files.createTempDirectory("sirix-scale-bench");
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
    HashType hash = HashType.fromString(System.getProperty("hashType", "NONE"));
    BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
        .location(dbDir)
        .numberOfNodesBeforeAutoCommit(autoCommit)
        .buildPathSummary(pathSummary)
        .buildPathStatistics(pathStatistics)
        .hashType(hash)
        .build();
    System.out.printf("# pathSummary=%s  pathStatistics=%s%n", pathSummary, pathStatistics);
    SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
    SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);

    if (shredNeeded) {
      long shredStart = System.nanoTime();
      try (Reader src = new GeneratedRecordsReader(recordCount);
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
      session = coll.getDatabase().beginResourceSession(JSON_RESOURCE);
      // Default = all cores. Overridable via -Dsirix.vec.threads=N — useful
      // when a concurrency bug in Sirix's JVMCI-compiled allocator / page
      // combiner path triggers at high fan-out.
      int vecThreads = Integer.parseInt(
          System.getProperty("sirix.vec.threads",
              String.valueOf(Runtime.getRuntime().availableProcessors())));
      vec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), vecThreads);
      SequentialPipelineStrategy.setVectorizedExecutor(vec);
      System.out.printf("# Vec threads: %d%n", vecThreads);
    }

    JsonDBCollection coll = (JsonDBCollection) store.lookup(JSON_DB);
    JsonDBItem docItem = (JsonDBItem) coll.getDocument();
    ctx.bind(DOC_VAR, (Sequence) docItem);

    System.out.printf("%-26s | %10s | %10s | %10s | %10s%n", "query", "min(ms)", "avg(ms)", "max(ms)", "result_bytes");
    System.out.printf("%-26s + %10s + %10s + %10s + %10s%n", "--------------------------",
                      "----------", "----------", "----------", "------------");

    long hitsBefore = io.sirix.cache.ShardedPageCache.getCacheHits();
    long missesBefore = io.sirix.cache.ShardedPageCache.getCacheMisses();
    for (Map.Entry<String, String> e : QUERIES.entrySet()) {
      runQueryRepeated(chain, ctx, e.getKey(), e.getValue(), iters);
    }
    long hits = io.sirix.cache.ShardedPageCache.getCacheHits() - hitsBefore;
    long misses = io.sirix.cache.ShardedPageCache.getCacheMisses() - missesBefore;
    long total = hits + misses;
    double hitPct = total == 0 ? 0.0 : 100.0 * hits / total;
    System.out.printf("# RecordPageCache during queries: hits=%,d misses=%,d (hit-rate=%.2f%%)%n",
                      hits, misses, hitPct);

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
    // -Dsirix.noWarmup=true disables warmup so the first measurement iter is
    // a true cold scan — useful for comparing the executor's single-shot
    // latency rather than the repeated-query cache-hit latency.
    final boolean noWarmup = Boolean.getBoolean("sirix.noWarmup");
    int warmupCount = noWarmup ? 0 : Math.max(3, Math.min(20, iters));
    long warmDeadline = System.nanoTime() + 5_000_000_000L; // 5s budget
    try {
      for (int i = 0; i < warmupCount && System.nanoTime() < warmDeadline; i++) {
        runOnce(chain, ctx, wrapped);
      }
    } catch (RuntimeException re) {
      System.out.printf("%-26s | (skipped: %s)%n", name, re.getMessage());
      return;
    }

    long min = Long.MAX_VALUE, max = 0, sum = 0;
    int bytes = 0;
    try {
      for (int i = 0; i < iters; i++) {
        long t0 = System.nanoTime();
        bytes = runOnce(chain, ctx, wrapped);
        long elapsed = System.nanoTime() - t0;
        sum += elapsed;
        if (elapsed < min) min = elapsed;
        if (elapsed > max) max = elapsed;
      }
    } catch (RuntimeException re) {
      System.out.printf("%-26s | (aborted iter: %s)%n", name, re.getMessage());
      return;
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

  /**
   * Streams a JSON array {@code [{record0},{record1},...,{recordN-1}]} on the fly so the caller
   * can parse arbitrarily large datasets without materializing the full string.
   */
  private static final class GeneratedRecordsReader extends Reader {
    private final long total;
    private final Random rng = new Random(42);
    private final StringBuilder line = new StringBuilder(96);
    private long produced = 0;
    private int pos = 0;
    private boolean opened = false;
    private boolean closed = false;

    GeneratedRecordsReader(long total) {
      this.total = total;
    }

    private void refill() {
      line.setLength(0);
      pos = 0;
      if (!opened) {
        line.append('[');
        opened = true;
        return;
      }
      if (produced < total) {
        if (produced > 0) line.append(',');
        line.append("{\"id\":").append(produced)
            .append(",\"age\":").append(18 + rng.nextInt(48))
            .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
            .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
            .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
            .append('}');
        produced++;
        return;
      }
      if (!closed) {
        line.append(']');
        closed = true;
      }
    }

    @Override
    public int read(char[] cbuf, int off, int len) {
      if (pos >= line.length()) {
        if (closed) return -1;
        refill();
        if (pos >= line.length()) return -1;
      }
      int n = Math.min(len, line.length() - pos);
      line.getChars(pos, pos + n, cbuf, off);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      // no-op
    }
  }
}
