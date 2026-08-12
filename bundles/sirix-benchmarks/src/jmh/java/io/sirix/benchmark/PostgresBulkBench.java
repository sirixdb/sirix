package io.sirix.benchmark;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.ShardedPageCache;
import io.sirix.page.PageKind;
import io.sirix.page.pax.RegionTable;
import io.sirix.settings.StringCompressionType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.settings.VersioningType;
import io.sirix.service.json.shredder.JsonPartitioner;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.json.shredder.ParallelJsonShredder;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.Arrays;

/**
 * The SirixDB half of the <em>bulk-data</em> PostgreSQL comparison in
 * {@code docs/COMPARISON_POSTGRES_BULK.md}: one large JSON corpus, no version history, measured for
 * ingest throughput, bytes on disk, and query latency warm and cold.
 *
 * <p>
 * This is a different experiment from {@link PostgresComparisonBench}, which measures a single
 * small document with deep version history. {@code docs/COMPARISON_POSTGRES.md} §0.14 established
 * that the versioned benchmark's 16 MiB corpus is entirely cache-resident and therefore cannot
 * support any claim about storage behaviour; {@code docs/BENCHMARK_DESIGN.md} §2.1 specifies a
 * corpus that exceeds the engine caches as the fix. This harness is that corpus.
 *
 * <p>
 * Kept in-tree deliberately, for the reason {@link PostgresComparisonBench} records: the previous
 * driver lived in {@code /tmp} and did not survive the machine it was written on.
 *
 * <p>
 * Subcommands:
 * 
 * <pre>
 *   ndjson &lt;in.json&gt; &lt;out.ndjson&gt;      stream a top-level JSON array to one object per line,
 *                                          the form PostgreSQL's COPY consumes
 *   ingest &lt;in.json&gt; &lt;dbDir&gt; [mode] [tuned] [autoCommit] [rounds] [afterCommit]
 *   query  &lt;storeLocation&gt; &lt;dbName&gt; [iters] [queryName]
 * </pre>
 *
 * <p>
 * {@code iters = 0} selects the cold regime: no warm-up and a single timed execution, so the
 * measurement includes page reads a warm run has already paid for. Drop the OS page cache before
 * launching the JVM to make it meaningful.
 */
public final class PostgresBulkBench {

  private static final String RESOURCE = "movies";
  private static final QNm DOC_VAR = new QNm("doc");
  private static final int BUF = 1 << 20;

  /**
   * The query shapes, chosen so each has an exactly equivalent formulation against both a PostgreSQL
   * {@code jsonb} column and a normalized relational table. Every one returns a scalar that is
   * cross-checked across all three arms - a fast wrong answer is not a result.
   */
  private static final Map<String, String> QUERIES = new LinkedHashMap<>();

  static {
    QUERIES.put("countAll", "count(for $m in $doc[] return $m)");
    QUERIES.put("filterCountYear", "count(for $m in $doc[] where $m.year > 1990 return $m)");
    QUERIES.put("sumYear", "sum(for $m in $doc[] return $m.year)");
    QUERIES.put("titleLookup", "count(for $m in $doc[] where $m.title eq \"Saleslady\" return $m)");
  }

  private PostgresBulkBench() {}

  public static void main(final String... args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: PostgresBulkBench <ndjson|ingest|query> ...");
      System.exit(1);
    }
    switch (args[0]) {
      case "ndjson" -> ndjson(Paths.get(args[1]), Paths.get(args[2]));
      case "ingest" -> ingest(args);
      case "query" -> query(args);
      case "projquery" -> projQuery(args);
      case "verify" -> verify(args);
      case "timeq" -> timeQuery(args);
      default -> {
        System.err.println("Unknown subcommand: " + args[0]);
        System.exit(1);
      }
    }
  }

  // ---------------------------------------------------------------- corpus preparation

  /**
   * Streams a top-level JSON array of objects into newline-delimited JSON. Never holds more than one
   * record in memory: the corpus is gigabytes and a DOM parse needs roughly ten times that. Records
   * are emitted verbatim, so PostgreSQL and SirixDB are fed the identical bytes.
   *
   * <p>
   * Escapes and braces inside string literals are tracked, so a brace in a movie title cannot
   * desynchronize the depth counter.
   */
  private static void ndjson(final Path in, final Path out) throws IOException {
    final long started = System.nanoTime();
    long records = 0;
    long bytesIn = 0;

    final byte[] buf = new byte[BUF];
    byte[] rec = new byte[1 << 16];
    int recLen = 0;
    int depth = 0;
    boolean inString = false;
    boolean escaped = false;
    boolean inRecord = false;

    try (final InputStream is = Files.newInputStream(in);
        final OutputStream os = new BufferedOutputStream(new FileOutputStream(out.toFile()), 1 << 22)) {
      int n;
      while ((n = is.read(buf)) > 0) {
        bytesIn += n;
        for (int i = 0; i < n; i++) {
          final byte b = buf[i];
          if (inRecord) {
            rec = grown(rec, recLen);
            rec[recLen++] = b;
          }
          if (inString) {
            if (escaped) {
              escaped = false;
            } else if (b == '\\') {
              escaped = true;
            } else if (b == '"') {
              inString = false;
            }
            continue;
          }
          switch (b) {
            case '"' -> inString = true;
            case '{' -> {
              if (depth == 0) {
                inRecord = true;
                recLen = 0;
                rec[recLen++] = b;
              }
              depth++;
            }
            case '}' -> {
              depth--;
              if (depth == 0 && inRecord) {
                os.write(rec, 0, recLen);
                os.write('\n');
                records++;
                inRecord = false;
                recLen = 0;
              }
            }
            default -> {
              // Structural whitespace, separators and the enclosing brackets are dropped.
            }
          }
        }
      }
    }

    final double secs = (System.nanoTime() - started) / 1e9;
    System.out.printf("# %,d records, %,d bytes, %.1f s, %.1f MB/s -> %s%n", records, bytesIn, secs,
        bytesIn / 1e6 / secs, out);
    if (depth != 0) {
      throw new IOException("unbalanced braces, final depth=" + depth);
    }
  }

  // ---------------------------------------------------------------- ingest

  private static void ingest(final String... args) throws Exception {
    final Path in = Paths.get(args[1]);
    final Path db = Paths.get(args[2]);
    final String mode = args.length > 3
        ? args[3]
        : "partitioned";
    final boolean tuned = args.length > 4 && Boolean.parseBoolean(args[4]);
    final int autoCommit = args.length > 5
        ? Integer.parseInt(args[5])
        : 100_000;
    final int rounds = args.length > 6
        ? Integer.parseInt(args[6])
        : 3;
    final AfterCommitState after = AfterCommitState.valueOf(args.length > 7
        ? args[7]
        : "KEEP_OPEN");

    final double mb = Files.size(in) / 1e6;
    final int cores = Runtime.getRuntime().availableProcessors();
    double best = Double.MAX_VALUE;

    for (int round = 0; round < rounds; round++) {
      if (Files.exists(db)) {
        Databases.removeDatabase(db);
      }
      Databases.createJsonDatabase(new DatabaseConfiguration(db));

      final long t0 = System.nanoTime();
      if ("single".equals(mode)) {
        try (final var database = Databases.openJsonDatabase(db)) {
          database.createResource(config(RESOURCE, tuned));
          try (final var session = database.beginResourceSession(RESOURCE);
              final var wtx = session.beginNodeTrx(autoCommit, after)) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(in), JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }
      } else {
        final JsonPartitioner.Plan plan = JsonPartitioner.plan(in, cores);
        try (final var database = Databases.openJsonDatabase(db)) {
          ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard", name -> config(name, tuned),
              autoCommit, cores, after);
        }
      }
      final double secs = (System.nanoTime() - t0) / 1e9;
      best = Math.min(best, secs);
      System.out.printf("  round %d: %.3f s  %.1f MB/s%n", round, secs, mb / secs);
    }

    System.out.printf("RESULT ingest mode=%s tuned=%s autoCommit=%d after=%s  best %.3f s  %.1f MB/s  dbBytes=%d%n",
        mode, tuned, autoCommit, after, best, mb / best, dirBytes(db));
  }

  private static ResourceConfiguration config(final String name, final boolean tuned) {
    ResourceConfiguration.Builder b = ResourceConfiguration.newBuilder(name);
    if (tuned) {
      // The features PostgreSQL's plain table has no equivalent for. Reporting only the tuned
      // configuration would overstate SirixDB; reporting only the defaults would hide the fact
      // that the comparison is not feature-for-feature either way.
      b = b.hashKind(HashType.NONE).buildPathSummary(false).storeDiffs(false).storeNodeHistory(false);
    }
    // Path statistics default to false (ResourceConfiguration.Builder#pathStatistics), and the
    // published comparison never turned them on — which means every scan number in
    // docs/COMPARISON_POSTGRES_BULK.md was measured with SirixDB's aggregate fast path disabled.
    // SirixVectorizedExecutor#tryPathSummaryStats answers count/sum/avg/min/max for a resolved path
    // straight from the summary, without touching a record page. The `tuned` profile disables the
    // path summary outright, so the two are mutually exclusive by construction.
    if (Boolean.getBoolean("sirix.bench.pathStats")) {
      b = b.buildPathSummary(true).buildPathStatistics(true);
    }
    // FSST string compression is off by default (StringCompressionType.NONE). It changes what the
    // column-scan path can do: FSST-encoded dictionary entries are stored as something other than
    // their value, so the dictionary sketch cannot be built from them and an equality cannot be
    // decided from the region alone. Switch it on to measure that.
    if (Boolean.getBoolean("sirix.bench.fsst")) {
      b = b.stringCompressionType(StringCompressionType.FSST);
    }
    // Page checksums are verified on read by default, and the hash covers the whole page image —
    // which is exactly the thing a column scan is trying not to read. Turning them off measures
    // what a byte-range read would be worth if the integrity check were per region instead.
    if (Boolean.getBoolean("sirix.bench.noChecksums")) {
      b = b.verifyChecksumsOnRead(false);
    }
    return b.build();
  }

  // ---------------------------------------------------------------- query

  /**
   * Selects the compile chain via {@code -Dsirix.bench.chain=sequential|parallel|morsel}.
   *
   * <p>
   * Defaults to {@code sequential}, which is what every published scan number in
   * {@code docs/COMPARISON_POSTGRES_BULK.md} was measured with. That is worth stating plainly: the
   * engine already ships a parallel chain and a morsel fan-out
   * ({@link SirixCompileChain#createParallel}, {@link SirixCompileChain#createParallelWithMorsel}),
   * and the comparison never exercised either — so the published figures are a single-threaded
   * SirixDB against a PostgreSQL that launches two parallel workers plus its leader.
   */
  private static SirixCompileChain chainFor(final BasicJsonDBStore store) {
    final String kind = System.getProperty("sirix.bench.chain", "sequential");
    return switch (kind) {
      case "parallel" -> SirixCompileChain.createParallel(null, store);
      // ordered=false. The ordered variant funnels every result item through brackit's SerialSink /
      // MutexSink, which on this corpus pins one ForkJoin worker at 100 % CPU while the rest idle —
      // 78 s of CPU in 90 s of wall-clock, no completion. An aggregate is order-insensitive, so the
      // unordered chain is the meaningful parallel measurement.
      case "parallel-unordered" -> SirixCompileChain.createParallel(null, store, false);
      // The sequential translator with source splitting — the path that actually fans out.
      case "morsel" -> SirixCompileChain.createWithMorsel(null, store);
      // The legacy arrangement, kept so the difference stays measurable: it asks for morsels and
      // gets the block-parallel chain, because the flag it sets belongs to a strategy the parallel
      // translator replaces.
      case "morsel-block" -> SirixCompileChain.createParallelWithMorsel(null, store);
      case "sequential" -> SirixCompileChain.createWithJsonStore(store);
      default -> throw new IllegalArgumentException("Unknown sirix.bench.chain: " + kind);
    };
  }

  /**
   * Cross-checks the path-statistics fast path against the full scan, query by query.
   *
   * <p>
   * A sub-millisecond filtered count over 3.48 M records can only come from maintained statistics,
   * and statistics are exactly where an engine is tempted to answer approximately. The published
   * comparison's own rule is that a fast wrong answer is not a result, so every shape the fast path
   * claims is re-run with the vectorized executor uninstalled — same query, same store, same revision
   * — and the two answers must be identical. Thresholds are chosen to straddle bucket edges (min,
   * max, off-by-one either side of the published 1990) because an approximate histogram agrees with a
   * scan in the middle of a range and diverges at the boundaries.
   *
   * <p>
   * Usage: {@code verify <storeLocation> <dbName>}
   */
  private static void verify(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];

    final Map<String, String> checks = new LinkedHashMap<>();
    // countAll is rewritten to jn:size() by ArrayCountToSizeStage; the negative controls must NOT
    // be rewritten, so they pin the stage's guards against the scan's answer.
    checks.put("countAll", "count(for $m in $doc[] return $m)");
    checks.put("countAll-unbox", "count($doc[])");
    checks.put("countAll-derefReturn", "count(for $m in $doc[] return $m.year)");
    checks.put("countAll-filtered", "count(for $m in $doc[] where $m.year > 1990 return $m)");
    checks.put("sum(year)", "sum(for $m in $doc[] return $m.year)");
    checks.put("count(year>1990)", "count(for $m in $doc[] where $m.year > 1990 return $m)");
    checks.put("count(year>1989)", "count(for $m in $doc[] where $m.year > 1989 return $m)");
    checks.put("count(year>1991)", "count(for $m in $doc[] where $m.year > 1991 return $m)");
    checks.put("count(year>1800)", "count(for $m in $doc[] where $m.year > 1800 return $m)");
    checks.put("count(year>2100)", "count(for $m in $doc[] where $m.year > 2100 return $m)");
    checks.put("count(year<1900)", "count(for $m in $doc[] where $m.year < 1900 return $m)");
    checks.put("min(year)", "min(for $m in $doc[] return $m.year)");
    checks.put("max(year)", "max(for $m in $doc[] return $m.year)");
    checks.put("count(title=Saleslady)", "count(for $m in $doc[] where $m.title eq \"Saleslady\" return $m)");
    checks.put("count(title=Nosferatu)", "count(for $m in $doc[] where $m.title eq \"Nosferatu\" return $m)");
    checks.put("count(title=__absent__)", "count(for $m in $doc[] where $m.title eq \"__absent__\" return $m)");

    final Map<String, String> fast = runAll(location, dbName, checks, true);
    final Map<String, String> slow = runAll(location, dbName, checks, false);

    int mismatches = 0;
    System.out.printf("%-26s | %-22s | %-22s | %s%n", "check", "pathStats", "full scan", "verdict");
    for (final String name : checks.keySet()) {
      final String a = fast.get(name);
      final String b = slow.get(name);
      final boolean ok = a != null && a.equals(b);
      if (!ok) {
        mismatches++;
      }
      System.out.printf("%-26s | %-22s | %-22s | %s%n", name, a, b, ok
          ? "MATCH"
          : "*** MISMATCH ***");
    }
    System.out.printf("# verify: %d checks, %d mismatches%n", checks.size(), mismatches);
    if (mismatches > 0) {
      System.exit(1);
    }
  }

  /**
   * Times one ad-hoc query, so alternative formulations of the same question can be compared without
   * editing {@link #QUERIES}. Honours the same {@code sirix.bench.*} switches as {@link #query}.
   *
   * <p>
   * Usage: {@code timeq <storeLocation> <dbName> <iters> <query body>}
   */
  private static void timeQuery(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];
    final int iters = Integer.parseInt(args[3]);
    // ";;" separates independent queries run in ONE JVM, in order. That matters for anything the
    // executor memoizes: a second identical query is a cache hit, so the only way to see the true
    // cost of a *distinct* predicate against an already-warm page cache is to run a warming query
    // first and a fresh one after, inside the same process.
    final String joined = String.join(" ", Arrays.copyOfRange(args, 4, args.length));
    final String[] bodies = joined.split(";;");

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = chainFor(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
      final Supplier<JsonDBItem> docSupplier = () -> (JsonDBItem) coll.getDocument();

      JsonResourceSession session = null;
      SirixVectorizedExecutor vec = null;
      // Build the columnar projection before anything is timed, so the shapes it can serve are
      // measured against it rather than against the storage scan. Its build cost is a full pass
      // over the corpus and is printed, not hidden: paying it once is part of the honest total.
      if (Boolean.getBoolean("sirix.bench.projection")) {
        try (final JsonResourceSession projected = coll.getDatabase().beginResourceSession(RESOURCE)) {
          final long t0 = System.nanoTime();
          final ProjectionIndexBenchSetup.BuildResult built = MoviesProjectionSetup.installWildcard(projected);
          System.out.printf("# projection: %,d leaves, %,d rows, built in %,.1f s%n", built.rowGroupCount(),
              built.totalRows(), (System.nanoTime() - t0) / 1e9);
        }
      }
      if (Boolean.getBoolean("sirix.bench.vectorized")) {
        session = coll.getDatabase().beginResourceSession(RESOURCE);
        vec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(),
            Runtime.getRuntime().availableProcessors());
        SequentialPipelineStrategy.setVectorizedExecutor(vec);
      }
      try {
        System.out.printf("%-18s | %10s | %10s | %s%n", "query", "min(ms)", "max(ms)", "result");
        for (int i = 0; i < bodies.length; i++) {
          SirixVectorizedExecutor.resetRegionOnlyCounters();
          NodeStorageEngineReader.resetVersioningDiag();
          VersioningType.resetCombineDiag();
          final long[] ioBefore = procSelfIo();
          final long hitsBefore = ShardedPageCache.getCacheHits();
          final long missesBefore = ShardedPageCache.getCacheMisses();
          runQuery(chain, ctx, docSupplier, "q" + i, bodies[i].trim(), iters);
          printIoDelta(ioBefore, procSelfIo());
          // A record-page MISS is a decode. Printed per query rather than per run: the question
          // "does the second query reuse the first one's decoded pages" cannot be answered by a
          // total, and a scan that re-decodes every page is the single largest CPU bucket there is.
          System.out.printf("                   | page cache: %d hits, %d misses%n",
              ShardedPageCache.getCacheHits() - hitsBefore, ShardedPageCache.getCacheMisses() - missesBefore);
          printRegionCounters();
          printVersioningDiag();
          printRegionReadDiag();
        }
      } finally {
        if (vec != null) {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          vec.close();
        }
        if (session != null) {
          session.close();
        }
      }
    }
  }

  /**
   * {@code rchar} = bytes the process ASKED for; {@code read_bytes} = bytes the block layer actually
   * fetched. A column read that shrinks the first but not the second is being undone by kernel
   * readahead, which no amount of arithmetic about page layout would reveal.
   */
  private static void printIoDelta(final long[] ioBefore, final long[] ioAfter) {
    if (ioBefore == null || ioAfter == null) {
      return;
    }
    System.out.printf("                   | io: requested %.1f MB, from device %.1f MB%n",
        (ioAfter[0] - ioBefore[0]) / 1048576.0, (ioAfter[1] - ioBefore[1]) / 1048576.0);
  }

  /**
   * How the answer was produced, not just how fast: a column-only page never materialized a record, a
   * fallback page did. Without this a timing change cannot be attributed.
   */
  private static void printRegionCounters() {
    final long columnar = SirixVectorizedExecutor.regionOnlyPagesServed();
    final long fellBack = SirixVectorizedExecutor.regionOnlyPageFallbacks();
    final long sketchSkips = SirixVectorizedExecutor.regionSketchSkips();
    final long sketchProbes = SirixVectorizedExecutor.regionSketchProbes();
    if (sketchSkips + sketchProbes > 0) {
      System.out.printf("                   | dict sketch: %d pages ruled out, %d needed the dictionary%n", sketchSkips,
          sketchProbes);
    }
    // WHICH of the fused kernel's decline conditions fires. A fused (multi-field) plan gets no
    // second chance, so every decline costs a page its column read AND a full record
    // reconstruction — the aggregate fallback count above cannot say which condition to fix.
    final long[] declines = SirixVectorizedExecutor.fusedDeclineCounts();
    long declineTotal = 0;
    for (final long d : declines) {
      declineTotal += d;
    }
    if (declineTotal > 0) {
      final StringBuilder why = new StringBuilder();
      final var reasons = SirixVectorizedExecutor.FusedDecline.values();
      for (int r = 0; r < declines.length; r++) {
        if (declines[r] > 0) {
          why.append(String.format("%s=%,d  ", reasons[r], declines[r]));
        }
      }
      System.out.printf("                   | fused declines (%,d): %s%n", declineTotal, why);
    }
    SirixVectorizedExecutor.resetFusedDeclineCounts();
    if (columnar + fellBack > 0) {
      System.out.printf(
          "                   | region-only pages: %d served (%d by merging fragments, "
              + "%d by scattering an optional field), %d fell back, %d unavailable%n",
          columnar, SirixVectorizedExecutor.regionMergedPages(), SirixVectorizedExecutor.regionScatterPages(), fellBack,
          SirixVectorizedExecutor.regionOnlyPagesUnavailable());
    }
  }

  /** Per-page reconstruction cost of a versioned resource, when the diagnostic is enabled. */
  private static void printVersioningDiag() {
    if (!Boolean.getBoolean("sirix.versioning.diag")) {
      return;
    }
    final long combines = NodeStorageEngineReader.versioningCombines();
    if (combines > 0) {
      System.out.printf(
          "                   | versioning: %d pages reconstructed from %d fragments "
              + "(%.1f per page), merge %.0f ms of CPU (%.2f ms per page)%n",
          combines, NodeStorageEngineReader.versioningFragmentsLoaded(),
          NodeStorageEngineReader.versioningFragmentsLoaded() / (double) combines,
          NodeStorageEngineReader.versioningCombineNanos() / 1e6,
          NodeStorageEngineReader.versioningCombineNanos() / 1e6 / combines);
      System.out.printf(
          "                   |   fragment fetch (read+decode) %.0f ms of thread time " + "(%.2f ms per page)%n",
          NodeStorageEngineReader.versioningFragmentFetchNanos() / 1e6,
          NodeStorageEngineReader.versioningFragmentFetchNanos() / 1e6 / combines);
    }
    final long slotNs = VersioningType.combineSlotCopyNanos();
    final long regionNs = VersioningType.combineRegionRebuildNanos();
    if (slotNs + regionNs > 0) {
      System.out.printf(
          "                   |   of which: slot-copy loop %.0f ms (%d slots), " + "number-region rebuild %.0f ms%n",
          slotNs / 1e6, VersioningType.combineSlotsCopied(), regionNs / 1e6);
    }
    NodeStorageEngineReader.resetVersioningDiag();
    VersioningType.resetCombineDiag();
  }

  /** What the column read actually touched on disk, when the diagnostic is enabled. */
  private static void printRegionReadDiag() {
    if (!Boolean.getBoolean("sirix.page.regionReadDiag")) {
      return;
    }
    final long decoded = PageKind.regionReadPagesDecoded();
    if (decoded > 0) {
      final long body = PageKind.regionReadBodyBytesSkipped();
      final long regions = PageKind.regionReadTableBytesRead();
      System.out.printf(
          "                   | column read: %d pages, body skipped %.1f MB, regions read %.1f MB (%.1f%% of body+regions)%n",
          decoded, body / 1048576.0, regions / 1048576.0, 100.0 * regions / (double) (body + regions));
    }
    final String[] kindNames = {"NUMBER", "STRING", "STRUCT", "DEWEYID", "NAMEKEY", "BOOLEAN", "HASH", "STRUCTPTR"};
    final StringBuilder perKind = new StringBuilder();
    for (byte k = 0; k < kindNames.length; k++) {
      final long mat = RegionTable.materializedBytes(k);
      final long skip = RegionTable.skippedBytes(k);
      if (mat + skip > 0) {
        perKind.append(String.format("%s %.0f/%.0f MB  ", kindNames[k], mat / 1048576.0, skip / 1048576.0));
      }
    }
    if (perKind.length() > 0) {
      System.out.printf("                   | region kinds materialized/skipped: %s%n", perKind);
    }
    RegionTable.resetReadDiag();
    PageKind.resetRegionReadDiag();
  }

  /**
   * {@code [rchar, read_bytes]} from {@code /proc/self/io}, or {@code null} where unavailable.
   * {@code rchar} counts bytes returned to read() calls; {@code read_bytes} counts what the block
   * layer actually fetched, so the pair separates "we asked for less" from "we read less".
   */
  private static long[] procSelfIo() {
    try {
      long rchar = -1;
      long readBytes = -1;
      for (final String line : Files.readAllLines(Paths.get("/proc/self/io"))) {
        if (line.startsWith("rchar:")) {
          rchar = Long.parseLong(line.substring(6).trim());
        } else if (line.startsWith("read_bytes:")) {
          readBytes = Long.parseLong(line.substring(11).trim());
        }
      }
      return rchar < 0 || readBytes < 0
          ? null
          : new long[] {rchar, readBytes};
    } catch (final Exception e) {
      return null;
    }
  }

  /** Runs {@code checks} in a fresh store, with the vectorized executor installed or not. */
  private static Map<String, String> runAll(final Path location, final String dbName, final Map<String, String> checks,
      final boolean vectorized) throws Exception {
    final Map<String, String> out = new LinkedHashMap<>();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
      JsonResourceSession session = null;
      SirixVectorizedExecutor vec = null;
      if (vectorized) {
        session = coll.getDatabase().beginResourceSession(RESOURCE);
        vec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(),
            Runtime.getRuntime().availableProcessors());
        SequentialPipelineStrategy.setVectorizedExecutor(vec);
      }
      try {
        for (final Map.Entry<String, String> e : checks.entrySet()) {
          ctx.bind(DOC_VAR, (Sequence) coll.getDocument());
          out.put(e.getKey(), execute(chain, ctx, "declare variable $doc external; " + e.getValue()).trim());
        }
      } finally {
        if (vectorized) {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          if (vec != null) {
            vec.close();
          }
          if (session != null) {
            session.close();
          }
        }
      }
    }
    return out;
  }

  private static void query(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];
    final int iters = args.length > 3
        ? Integer.parseInt(args[3])
        : 3;
    final String only = args.length > 4
        ? args[4]
        : null;

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = chainFor(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
      final Supplier<JsonDBItem> docSupplier = () -> (JsonDBItem) coll.getDocument();

      // Optionally install the vectorized executor WITHOUT building a projection index.
      // projQuery() couples the two, but they are independent: the projection accelerates scans,
      // whereas SirixVectorizedExecutor#tryPathSummaryStats answers count/sum/avg/min/max from the
      // path summary alone. Separating them is the point — it measures what SirixDB can answer with
      // no build step at all, against PostgreSQL's 19 s of durable index builds.
      final boolean vectorized = Boolean.getBoolean("sirix.bench.vectorized");
      JsonResourceSession vecSession = null;
      SirixVectorizedExecutor vec = null;
      if (vectorized) {
        vecSession = coll.getDatabase().beginResourceSession(RESOURCE);
        vec = new SirixVectorizedExecutor(vecSession, vecSession.getMostRecentRevisionNumber(),
            Runtime.getRuntime().availableProcessors());
        SequentialPipelineStrategy.setVectorizedExecutor(vec);
        System.out.printf("# vectorized executor installed (no projection index built)%n");
      }

      System.out.printf("%-18s | %10s | %10s | %s%n", "query", "min(ms)", "max(ms)", "result");
      final long hitsBefore = ShardedPageCache.getCacheHits();
      final long missesBefore = ShardedPageCache.getCacheMisses();
      try {
        for (final Map.Entry<String, String> e : QUERIES.entrySet()) {
          if (only == null || only.equals(e.getKey())) {
            runQuery(chain, ctx, docSupplier, e.getKey(), e.getValue(), iters);
          }
        }
      } finally {
        if (vectorized) {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          if (vec != null) {
            vec.close();
          }
          if (vecSession != null) {
            vecSession.close();
          }
        }
      }
      // A record-page miss IS a decode, so this counts the work the scan actually did. Printed
      // because a parallel read-ahead that raises the miss count is decoding pages twice rather
      // than decoding them sooner, and wall-clock alone cannot tell those apart.
      System.out.printf("# cachestats hits=%d misses=%d%n", ShardedPageCache.getCacheHits() - hitsBefore,
          ShardedPageCache.getCacheMisses() - missesBefore);
    }
  }

  /**
   * The same queries as {@link #query}, but served through the in-memory columnar projection over
   * (year, title) rather than the storage scan paths.
   *
   * <p>
   * {@code docs/COMPARISON_DUCKDB.md} is explicit that the scan paths are always-correct fallbacks
   * and not the analytical engine, so measuring only those understates what SirixDB can do on
   * analytical shapes. The projection build cost is reported alongside the query times: it is a full
   * pass over the corpus and paying it once is part of the honest total.
   *
   * <p>
   * Usage: {@code projquery <storeLocation> <dbName> [iters] [queryName]}
   */
  private static void projQuery(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];
    final int iters = args.length > 3
        ? Integer.parseInt(args[3])
        : 3;
    final String only = args.length > 4
        ? args[4]
        : null;
    final int threads = Integer.parseInt(
        System.getProperty("sirix.vec.threads", String.valueOf(Runtime.getRuntime().availableProcessors())));

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);

      try (final JsonResourceSession session = coll.getDatabase().beginResourceSession(RESOURCE)) {
        final long t0 = System.nanoTime();
        final ProjectionIndexBenchSetup.BuildResult built = MoviesProjectionSetup.installWildcard(session);
        System.out.printf("# projection: %,d leaves, %,d rows, built in %,.1f s%n", built.rowGroupCount(),
            built.totalRows(), (System.nanoTime() - t0) / 1e9);

        final SirixVectorizedExecutor vec =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), threads);
        SequentialPipelineStrategy.setVectorizedExecutor(vec);
        System.out.printf("# vectorized executor threads: %d%n", threads);
        try {
          final Supplier<JsonDBItem> docSupplier = () -> (JsonDBItem) coll.getDocument();

          System.out.printf("%-18s | %10s | %10s | %s%n", "query", "min(ms)", "max(ms)", "result");
          for (final Map.Entry<String, String> e : QUERIES.entrySet()) {
            if (only == null || only.equals(e.getKey())) {
              runQuery(chain, ctx, docSupplier, e.getKey(), e.getValue(), iters);
            }
          }
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          vec.close();
        }
      }
    }
  }

  /**
   * Whether to keep ONE {@code $doc} binding across timed iterations.
   *
   * <p>
   * Defaults to false, and that default is the whole point. {@code AbstractJsonDBArray} memoizes its
   * element list on first access: bind the document once, and the untimed warm-up pass materializes
   * all 3,482,208 elements as {@code Sequence} objects, after which every timed iteration walks that
   * Java list instead of the store. PostgreSQL re-executes a full heap scan per iteration, so
   * comparing the two measures different things — measured on this corpus, reusing the binding
   * reports {@code countAll} at 96.5 ms against 1,177.3 ms rebound (12.2x) and
   * {@code filterCountYear} at 203.8 ms against 2,884.0 ms (14.1x).
   *
   * <p>
   * Rebinding per iteration re-fetches the document, which yields a fresh array item with an empty
   * memo while leaving the page cache and JIT warm — the honest analogue of PostgreSQL re-running the
   * query. Set {@code -Dsirix.bench.reuseBinding=true} to get the old behaviour for comparison; do
   * not publish it as a scan number.
   */
  private static final boolean REUSE_BINDING = Boolean.getBoolean("sirix.bench.reuseBinding");

  private static void runQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
      final Supplier<JsonDBItem> docSupplier, final String name, final String body, final int iters) {
    final String q = "declare variable $doc external; " + body;

    if (iters == 0) {
      ctx.bind(DOC_VAR, (Sequence) docSupplier.get());
      final long t0 = System.nanoTime();
      final String result = execute(chain, ctx, q);
      System.out.printf("%-18s | %10.1f | %10s | %s%n", name, (System.nanoTime() - t0) / 1e6, "cold", result.trim());
      return;
    }

    // One untimed pass, so HotSpot has compiled the query path before anything is recorded.
    ctx.bind(DOC_VAR, (Sequence) docSupplier.get());
    String result = execute(chain, ctx, q);
    double min = Double.MAX_VALUE;
    double max = 0.0;
    for (int i = 0; i < iters; i++) {
      if (!REUSE_BINDING) {
        // Outside the timed region: fetching the document is a move to the root and a node read,
        // and what it buys is an empty element memo for the iteration that follows.
        ctx.bind(DOC_VAR, (Sequence) docSupplier.get());
      }
      final long t0 = System.nanoTime();
      result = execute(chain, ctx, q);
      final double ms = (System.nanoTime() - t0) / 1e6;
      min = Math.min(min, ms);
      max = Math.max(max, ms);
    }
    System.out.printf("%-18s | %10.1f | %10.1f | %s%n", name, min, max, result.trim());
  }

  private static String execute(final SirixCompileChain chain, final SirixQueryContext ctx, final String q) {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(sink)) {
      new Query(chain, q).prettyPrint().serialize(ctx, ps);
    }
    return sink.toString();
  }

  /** The record buffer, doubled if the next byte would not fit. Off the per-byte path in practice. */
  private static byte[] grown(final byte[] rec, final int recLen) {
    if (recLen < rec.length) {
      return rec;
    }
    final byte[] bigger = new byte[rec.length << 1];
    System.arraycopy(rec, 0, bigger, 0, recLen);
    return bigger;
  }

  private static long dirBytes(final Path dir) {
    try (final var walk = Files.walk(dir)) {
      return walk.filter(Files::isRegularFile).mapToLong(p -> {
        try {
          return Files.size(p);
        } catch (final IOException e) {
          return 0L;
        }
      }).sum();
    } catch (final IOException e) {
      return -1L;
    }
  }
}
