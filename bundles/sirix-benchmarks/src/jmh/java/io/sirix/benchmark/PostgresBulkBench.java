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
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.SirixVectorizedExecutor;
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

/**
 * The SirixDB half of the <em>bulk-data</em> PostgreSQL comparison in
 * {@code docs/COMPARISON_POSTGRES_BULK.md}: one large JSON corpus, no version history, measured for
 * ingest throughput, bytes on disk, and query latency warm and cold.
 *
 * <p>This is a different experiment from {@link PostgresComparisonBench}, which measures a single
 * small document with deep version history. {@code docs/COMPARISON_POSTGRES.md} §0.14 established
 * that the versioned benchmark's 16 MiB corpus is entirely cache-resident and therefore cannot
 * support any claim about storage behaviour; {@code docs/BENCHMARK_DESIGN.md} §2.1 specifies a
 * corpus that exceeds the engine caches as the fix. This harness is that corpus.
 *
 * <p>Kept in-tree deliberately, for the reason {@link PostgresComparisonBench} records: the
 * previous driver lived in {@code /tmp} and did not survive the machine it was written on.
 *
 * <p>Subcommands:
 * <pre>
 *   ndjson &lt;in.json&gt; &lt;out.ndjson&gt;      stream a top-level JSON array to one object per line,
 *                                          the form PostgreSQL's COPY consumes
 *   ingest &lt;in.json&gt; &lt;dbDir&gt; [mode] [tuned] [autoCommit] [rounds] [afterCommit]
 *   query  &lt;storeLocation&gt; &lt;dbName&gt; [iters] [queryName]
 * </pre>
 *
 * <p>{@code iters = 0} selects the cold regime: no warm-up and a single timed execution, so the
 * measurement includes page reads a warm run has already paid for. Drop the OS page cache before
 * launching the JVM to make it meaningful.
 */
public final class PostgresBulkBench {

  private static final String RESOURCE = "movies";
  private static final QNm DOC_VAR = new QNm("doc");
  private static final int BUF = 1 << 20;

  /**
   * The query shapes, chosen so each has an exactly equivalent formulation against both a
   * PostgreSQL {@code jsonb} column and a normalized relational table. Every one returns a scalar
   * that is cross-checked across all three arms - a fast wrong answer is not a result.
   */
  private static final Map<String, String> QUERIES = new LinkedHashMap<>();

  static {
    QUERIES.put("countAll", "count(for $m in $doc[] return $m)");
    QUERIES.put("filterCountYear", "count(for $m in $doc[] where $m.year > 1990 return $m)");
    QUERIES.put("sumYear", "sum(for $m in $doc[] return $m.year)");
    QUERIES.put("titleLookup", "count(for $m in $doc[] where $m.title eq \"Saleslady\" return $m)");
  }

  private PostgresBulkBench() {
  }

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
      default -> {
        System.err.println("Unknown subcommand: " + args[0]);
        System.exit(1);
      }
    }
  }

  // ---------------------------------------------------------------- corpus preparation

  /**
   * Streams a top-level JSON array of objects into newline-delimited JSON. Never holds more than
   * one record in memory: the corpus is gigabytes and a DOM parse needs roughly ten times that.
   * Records are emitted verbatim, so PostgreSQL and SirixDB are fed the identical bytes.
   *
   * <p>Escapes and braces inside string literals are tracked, so a brace in a movie title cannot
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
            if (recLen == rec.length) {
              final byte[] bigger = new byte[rec.length << 1];
              System.arraycopy(rec, 0, bigger, 0, recLen);
              rec = bigger;
            }
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
    System.out.printf("# %,d records, %,d bytes, %.1f s, %.1f MB/s -> %s%n",
                      records, bytesIn, secs, bytesIn / 1e6 / secs, out);
    if (depth != 0) {
      throw new IOException("unbalanced braces, final depth=" + depth);
    }
  }

  // ---------------------------------------------------------------- ingest

  private static void ingest(final String... args) throws Exception {
    final Path in = Paths.get(args[1]);
    final Path db = Paths.get(args[2]);
    final String mode = args.length > 3 ? args[3] : "partitioned";
    final boolean tuned = args.length > 4 && Boolean.parseBoolean(args[4]);
    final int autoCommit = args.length > 5 ? Integer.parseInt(args[5]) : 100_000;
    final int rounds = args.length > 6 ? Integer.parseInt(args[6]) : 3;
    final AfterCommitState after =
        AfterCommitState.valueOf(args.length > 7 ? args[7] : "KEEP_OPEN");

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
          ParallelJsonShredder.shredPartitioned(database, plan.readers(), "shard",
                                                name -> config(name, tuned), autoCommit, cores, after);
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
    return b.build();
  }

  // ---------------------------------------------------------------- query

  private static void query(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];
    final int iters = args.length > 3 ? Integer.parseInt(args[3]) : 3;
    final String only = args.length > 4 ? args[4] : null;

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);
      final JsonDBItem doc = (JsonDBItem) coll.getDocument();
      ctx.bind(DOC_VAR, (Sequence) doc);

      System.out.printf("%-18s | %10s | %10s | %s%n", "query", "min(ms)", "max(ms)", "result");
      for (final Map.Entry<String, String> e : QUERIES.entrySet()) {
        if (only == null || only.equals(e.getKey())) {
          runQuery(chain, ctx, e.getKey(), e.getValue(), iters);
        }
      }
    }
  }

  /**
   * The same queries as {@link #query}, but served through the in-memory columnar projection over
   * (year, title) rather than the storage scan paths.
   *
   * <p>{@code docs/COMPARISON_DUCKDB.md} is explicit that the scan paths are always-correct
   * fallbacks and not the analytical engine, so measuring only those understates what SirixDB can
   * do on analytical shapes. The projection build cost is reported alongside the query times: it
   * is a full pass over the corpus and paying it once is part of the honest total.
   *
   * <p>Usage: {@code projquery <storeLocation> <dbName> [iters] [queryName]}
   */
  private static void projQuery(final String... args) throws Exception {
    final Path location = Paths.get(args[1]);
    final String dbName = args[2];
    final int iters = args.length > 3 ? Integer.parseInt(args[3]) : 3;
    final String only = args.length > 4 ? args[4] : null;
    final int threads = Integer.parseInt(System.getProperty("sirix.vec.threads",
        String.valueOf(Runtime.getRuntime().availableProcessors())));

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final JsonDBCollection coll = (JsonDBCollection) store.lookup(dbName);

      try (final JsonResourceSession session = coll.getDatabase().beginResourceSession(RESOURCE)) {
        final long t0 = System.nanoTime();
        final ProjectionIndexBenchSetup.BuildResult built = MoviesProjectionSetup.installWildcard(session);
        System.out.printf("# projection: %,d leaves, %,d rows, built in %,.1f s%n",
                          built.rowGroupCount(), built.totalRows(), (System.nanoTime() - t0) / 1e9);

        final SirixVectorizedExecutor vec =
            new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), threads);
        SequentialPipelineStrategy.setVectorizedExecutor(vec);
        System.out.printf("# vectorized executor threads: %d%n", threads);
        try {
          final JsonDBItem doc = (JsonDBItem) coll.getDocument();
          ctx.bind(DOC_VAR, (Sequence) doc);

          System.out.printf("%-18s | %10s | %10s | %s%n", "query", "min(ms)", "max(ms)", "result");
          for (final Map.Entry<String, String> e : QUERIES.entrySet()) {
            if (only == null || only.equals(e.getKey())) {
              runQuery(chain, ctx, e.getKey(), e.getValue(), iters);
            }
          }
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          vec.close();
        }
      }
    }
  }

  private static void runQuery(final SirixCompileChain chain, final SirixQueryContext ctx,
                               final String name, final String body, final int iters) {
    final String q = "declare variable $doc external; " + body;

    if (iters == 0) {
      final long t0 = System.nanoTime();
      final String result = execute(chain, ctx, q);
      System.out.printf("%-18s | %10.1f | %10s | %s%n",
                        name, (System.nanoTime() - t0) / 1e6, "cold", result.trim());
      return;
    }

    // One untimed pass, so HotSpot has compiled the query path before anything is recorded.
    String result = execute(chain, ctx, q);
    double min = Double.MAX_VALUE;
    double max = 0.0;
    for (int i = 0; i < iters; i++) {
      final long t0 = System.nanoTime();
      result = execute(chain, ctx, q);
      final double ms = (System.nanoTime() - t0) / 1e6;
      min = Math.min(min, ms);
      max = Math.max(max, ms);
    }
    System.out.printf("%-18s | %10.1f | %10.1f | %s%n", name, min, max, result.trim());
  }

  private static String execute(final SirixCompileChain chain, final SirixQueryContext ctx,
                                final String q) {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    try (final PrintStream ps = new PrintStream(sink)) {
      new Query(chain, q).prettyPrint().serialize(ctx, ps);
    }
    return sink.toString();
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
