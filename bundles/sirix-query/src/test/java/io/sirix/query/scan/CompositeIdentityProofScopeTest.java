package io.sirix.query.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The composite string-identity registry must charge its canonical-byte budget for the answer's
 * vocabulary only — the distinct strings that predicate-surviving rows turn into group keys — never
 * for every string the visited leaves' dictionaries store. Proving whole dictionaries let
 * ClickBench q39 decline on a 1M-row corpus with per-leaf dictionaries (the state every fat column
 * is in at 100M, where AUTO declines resource-wide dictionaries) although its answer names a few
 * thousand strings. Two arms: a budget that holds the answer but not the dictionaries SERVES
 * exactly; a budget that cannot even hold the answer still DECLINES — the bound is scoped, not
 * bypassed.
 */
final class CompositeIdentityProofScopeTest {

  private static final String DB = "composite-identity-scope-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final int RECORDS = 3_000;
  /** Surviving rows: buckets of sizes 1..6 (21 rows), so every group count is distinct. */
  private static final int SURVIVING = 21;
  private static final int VALUE_BYTES = 100;
  /** q39's shape: a predicate, two STRING components, count, top-K by count. */
  private static final String QUERY = "subsequence(for $u in " + SRC + " where $u.id lt " + SURVIVING
      + " let $s := $u.src, $d := $u.dst group by $s, $d let $c := count($u) order by $c descending "
      + "return {\"s\": $s, \"d\": $d, \"c\": $c}, 1, 30)";

  private Path dbDir;
  private long priorBudget;
  private long dictionaryBytesBeyondTheAnswer;

  @BeforeEach
  void setUp() throws Exception {
    priorBudget = SirixVectorizedExecutor.compositeIdentityMaxBytes;
    dbDir = Files.createTempDirectory("sirix-composite-identity-scope-");
    final Random rng = new Random(0x5C0BEL);
    final StringBuilder sb = new StringBuilder(RECORDS * 2 * VALUE_BYTES + RECORDS * 32);
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final String src;
      final String dst;
      if (i < SURVIVING) {
        final int bucket = bucketOf(i);
        src = padded("S" + bucket);
        dst = padded("D" + bucket);
      } else {
        src = highEntropy(rng, VALUE_BYTES);
        dst = highEntropy(rng, VALUE_BYTES);
        dictionaryBytesBeyondTheAnswer += src.length() + dst.length();
      }
      sb.append("{\"id\":")
        .append(i)
        .append(",\"src\":\"")
        .append(src)
        .append("\",\"dst\":\"")
        .append(dst)
        .append("\",\"cat\":")
        .append(i % 3)
        .append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/src', '/[]/dst', '/[]/cat'),
            ('long', 'string', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  void tearDown() {
    SirixVectorizedExecutor.compositeIdentityMaxBytes = priorBudget;
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  @DisplayName("a budget that holds the answer but not the dictionaries still SERVES, exactly")
  void answerSizedBudgetServes() throws Exception {
    final long budget = 64L << 10;
    assertTrue(dictionaryBytesBeyondTheAnswer > 4 * budget,
        "the dictionaries must dwarf the budget, or proving whole dictionaries would fit and prove nothing");
    final String interpreted = run(QUERY, false);
    SirixVectorizedExecutor.compositeIdentityMaxBytes = budget;
    final long served = SirixVectorizedExecutor.groupAggServedCount();
    final String vectorized = run(QUERY, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > served,
        "the composite string key must be SERVED under an answer-sized budget — whole dictionaries were charged");
    assertEquals(interpreted, vectorized, "the served answer must equal the interpreter's");
    assertEquals(6, countGroups(vectorized), "six buckets survive the predicate");
  }

  @Test
  @DisplayName("a budget that cannot hold the answer still DECLINES — scoped, not bypassed")
  void answerBeyondBudgetDeclines() throws Exception {
    final String interpreted = run(QUERY, false);
    // Twelve canonical strings of 100 bytes plus the registry's per-entry overhead exceed 1 KiB.
    SirixVectorizedExecutor.compositeIdentityMaxBytes = 1L << 10;
    final long served = SirixVectorizedExecutor.groupAggServedCount();
    final String vectorized = run(QUERY, true);
    assertEquals(served, SirixVectorizedExecutor.groupAggServedCount(),
        "a budget below the answer's own vocabulary must still decline the serve");
    assertEquals(interpreted, vectorized, "the declined query must fall back to the interpreter's answer");
  }

  /** Bucket b holds b + 1 rows (sizes 1..6 over ids 0..20). */
  private static int bucketOf(final int id) {
    int bucket = 0;
    int start = 0;
    while (id >= start + bucket + 1) {
      start += bucket + 1;
      bucket++;
    }
    return bucket;
  }

  private static String padded(final String prefix) {
    final StringBuilder sb = new StringBuilder(VALUE_BYTES).append(prefix).append('-');
    while (sb.length() < VALUE_BYTES) {
      sb.append((char) ('a' + sb.length() % 26));
    }
    return sb.toString();
  }

  /**
   * Uniform over the printable characters that survive both a JSON string and an XQuery string
   * literal unescaped ({@code "} {@code \\} {@code '} and {@code &} are excluded): FSST declines such
   * values, so every byte counts toward the dictionaries.
   */
  private static String highEntropy(final Random rng, final int len) {
    final StringBuilder sb = new StringBuilder(len);
    while (sb.length() < len) {
      final char c = (char) (33 + rng.nextInt(94));
      if (c == '"' || c == '\\' || c == '\'' || c == '&') {
        continue;
      }
      sb.append(c);
    }
    return sb.toString();
  }

  private static int countGroups(final String serialized) {
    int count = 0;
    for (int i = serialized.indexOf("\"c\":"); i >= 0; i = serialized.indexOf("\"c\":", i + 1)) {
      count++;
    }
    return count;
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
