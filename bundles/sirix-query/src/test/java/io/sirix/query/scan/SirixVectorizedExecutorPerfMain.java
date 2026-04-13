package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Standalone perf comparison: Brackit query on Sirix-stored data,
 * Volcano vs SirixVectorizedExecutor. Run via:
 *   ./gradlew :sirix-query:test --tests SirixVectorizedExecutorPerfMain -Dn=100000
 */
public final class SirixVectorizedExecutorPerfMain {

  private static final String DB = "perf-db";
  private static final String RES = "records.jn";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };

  @Test
  void perf() throws Exception {
    main(new String[] { System.getProperty("n", "100000") });
  }

  public static void main(String[] args) throws Exception {
    int recordCount = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
    int warmup = 3;
    int iterations = 5;

    Path dbDir = Files.createTempDirectory("sirix-perf");
    System.out.println("[setup] generating " + recordCount + " records, dbDir=" + dbDir);
    Random rng = new Random(42);
    StringBuilder sb = new StringBuilder(recordCount * 64);
    sb.append('[');
    for (int i = 0; i < recordCount; i++) {
      if (i > 0) sb.append(',');
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(18 + rng.nextInt(48))
        .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
        .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
        .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
        .append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      System.out.println("[setup] storing into Sirix...");
      long s0 = System.nanoTime();
      String storeQuery = "jn:store('" + DB + "','" + RES + "','" + sb.toString().replace("'", "''") + "')";
      new Query(chain, storeQuery).evaluate(ctx);
      System.out.println("[setup] stored in " + (System.nanoTime() - s0) / 1_000_000 + "ms");

      var coll = store.lookup(DB);
      var resourceSession = coll.getDatabase().beginResourceSession(RES);
      int rev = resourceSession.getMostRecentRevisionNumber();

      String query = "let $doc := jn:doc('" + DB + "','" + RES + "') return sum(for $u in $doc[] return $u.age)";

      // ---- Volcano baseline ----
      System.out.println("\n=== VOLCANO (no vectorized executor) ===");
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      runQuery(chain, ctx, query, warmup, iterations);

      // ---- Vectorized, single-threaded ----
      System.out.println("\n=== VECTORIZED (1 thread) ===");
      var execSingle = new SirixVectorizedExecutor(resourceSession, rev, 1);
      SequentialPipelineStrategy.setVectorizedExecutor(execSingle);
      runQuery(chain, ctx, query, warmup, iterations);
      execSingle.close();

      // ---- Vectorized, parallel ----
      System.out.println("\n=== VECTORIZED (" + Runtime.getRuntime().availableProcessors() + " threads) ===");
      var execPar = new SirixVectorizedExecutor(resourceSession, rev);
      SequentialPipelineStrategy.setVectorizedExecutor(execPar);
      runQuery(chain, ctx, query, warmup, iterations);
      execPar.close();

      SequentialPipelineStrategy.setVectorizedExecutor(null);
      resourceSession.close();
    }

    Databases.removeDatabase(dbDir);
    System.out.println("\n[done]");
  }

  private static void runQuery(SirixCompileChain chain, SirixQueryContext ctx, String query, int warmup, int iter) {
    System.out.print("  warmup: ");
    for (int i = 0; i < warmup; i++) {
      long t = run(chain, ctx, query);
      System.out.print(t + "ms  ");
    }
    System.out.println();
    System.out.print("  measure: ");
    long total = 0;
    long min = Long.MAX_VALUE;
    long max = 0;
    for (int i = 0; i < iter; i++) {
      long t = run(chain, ctx, query);
      System.out.print(t + "ms  ");
      total += t;
      if (t < min) min = t;
      if (t > max) max = t;
    }
    System.out.println();
    System.out.println("  avg=" + (total / iter) + "ms  min=" + min + "ms  max=" + max + "ms");
  }

  private static long run(SirixCompileChain chain, SirixQueryContext ctx, String query) {
    long start = System.nanoTime();
    var buf = IOUtils.createBuffer();
    try (var ser = new StringSerializer(buf)) {
      ser.serialize(new Query(chain, query).execute(ctx));
    }
    return (System.nanoTime() - start) / 1_000_000;
  }
}
