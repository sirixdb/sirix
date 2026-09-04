package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionStringIdentityRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end gate for the string half of composite group identity.
 *
 * <p>
 * A composite key's dictionary-string components are carried as a fingerprint PAIR. That pair
 * discriminates but does not identify, and the failure it hides is silent: two distinct strings
 * sharing both lanes compare EQUAL in the group table, so the two groups fold and the probe-key
 * collision flag never moves. Two strings colliding in both real 64-bit functions cannot be
 * constructed, so this test INJECTS a degenerate fingerprint and asserts the engine behaves the
 * only two acceptable ways: prove byte equality, or DECLINE.
 *
 * <p>
 * The decline arm additionally asserts the ANSWER is still right — a decline that returned wrong
 * rows would be no better than the merge it avoids.
 */
public final class CompositeStringIdentityDeclineTest {

  private static final String DB = "composite-string-identity-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops", "Legal"};

  /** Collapses every value onto ONE fingerprint pair: the worst adversary a scan can face. */
  private static final ProjectionStringIdentityRegistry.Fingerprint ALL_COLLIDE =
      new ProjectionStringIdentityRegistry.Fingerprint() {

        @Override
        public long primary(final byte[] utf8, final int off, final int len, final long fnv1a64) {
          return 0x5EEDL;
        }

        @Override
        public long secondary(final byte[] utf8, final int off, final int len) {
          return 0xC0FFEEL;
        }
      };

  /** The composite top-K shape: a STRING component beside a numeric one. */
  private static final String QUERY = "subsequence(for $u in " + SRC + " let $d := $u.dept, $k := $u.k7 "
      + "group by $d, $k let $c := count($u) order by $c descending "
      + "return {\"d\": $d, \"k\": $k, \"c\": $c}, 1, 20)";

  /**
   * The same shape under a row predicate: partial coverage, so it can USE the column memo but never
   * EARN it.
   */
  private static final String PREDICATED_QUERY = "subsequence(for $u in " + SRC + " where $u.amount < 50 "
      + "let $d := $u.dept, $k := $u.k7 group by $d, $k let $c := count($u) order by $c descending "
      + "return {\"d\": $d, \"k\": $k, \"c\": $c}, 1, 20)";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-composite-string-identity-");
    final StringBuilder sb = new StringBuilder(8192);
    sb.append('[');
    for (int i = 0; i < 700; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":")
        .append(i)
        .append(",\"dept\":\"")
        .append(DEPTS[i % DEPTS.length])
        .append('"')
        .append(",\"k7\":")
        .append(i % 7)
        .append(",\"amount\":")
        .append(i % 97)
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
            ('/[]/id', '/[]/dept', '/[]/k7', '/[]/amount'),
            ('long', 'string', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  void tearDown() {
    ProjectionStringIdentityRegistry.resetFingerprint();
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  @DisplayName("with real fingerprints the composite string key SERVES and matches the interpreter")
  void realFingerprintsServe() throws Exception {
    final String interpreted = run(QUERY, false);
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    final String served = run(QUERY, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > before,
        "the composite string key must still be served — otherwise the decline arm proves nothing");
    assertEquals(interpreted, served);
  }

  @Test
  @DisplayName("a forced fingerprint collision DECLINES the serve instead of merging groups")
  void forcedCollisionDeclines() throws Exception {
    final String interpreted = run(QUERY, false);
    ProjectionIndexCatalog.clearCache();
    ProjectionStringIdentityRegistry.installFingerprintForTesting(ALL_COLLIDE);
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    final String underCollision = run(QUERY, true);
    // ANSWER first, serving second. The injected fingerprint drives the probe hash as well as the
    // identity lanes, so without the byte proof the five departments genuinely fold: 5 depts x 7 k7
    // = 35 real groups (top-20 window holds 20) collapses to 7. Asserting the count BEFORE the
    // served counter is what makes this a witness for the MERGE rather than only for the decline —
    // with the counter assertion first, the test aborts before it ever looks at the rows.
    assertEquals(20, countGroups(underCollision),
        "groups were merged by fingerprint instead of compared byte-wise: " + underCollision);
    assertEquals(interpreted, underCollision,
        "the declined query must still return the interpreter's answer, not a merged one");
    assertEquals(before, SirixVectorizedExecutor.groupAggServedCount(),
        "the group-aggregate route must DECLINE when string identity cannot be proven byte-wise");
  }

  @Test
  @DisplayName("a completed full-coverage serve memoizes the column and the next serve is pre-proven")
  void fullCoverageServeMemoizesTheColumn() throws Exception {
    final String interpreted = run(QUERY, false);
    final long memoedBefore = SirixVectorizedExecutor.groupIdentityMemoedCount();
    final long preProvenBefore = SirixVectorizedExecutor.groupIdentityPreProvenCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    assertEquals(interpreted, run(QUERY, true));
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.groupAggServedCount(), "the first serve must serve");
    assertEquals(memoedBefore + 1, SirixVectorizedExecutor.groupIdentityMemoedCount(),
        "the ONE string component's column is noted on the handle by a completed full-coverage serve");
    assertEquals(preProvenBefore, SirixVectorizedExecutor.groupIdentityPreProvenCount(),
        "the first serve had to prove: nothing was pre-proven");
    assertEquals(interpreted, run(QUERY, true));
    assertEquals(servedBefore + 2, SirixVectorizedExecutor.groupAggServedCount(), "the second serve must serve");
    assertEquals(preProvenBefore + 1, SirixVectorizedExecutor.groupIdentityPreProvenCount(),
        "the second serve carries the memoized column as exact identity without a proof");
    assertEquals(memoedBefore + 1, SirixVectorizedExecutor.groupIdentityMemoedCount(),
        "a pre-proven serve has nothing to note");
  }

  @Test
  @DisplayName("the column memo is keyed by the fingerprint it was earned under — an adversary cannot inherit it")
  void memoIsNotTrustedUnderAnotherFingerprint() throws Exception {
    final String interpreted = run(QUERY, false);
    final long memoedBefore = SirixVectorizedExecutor.groupIdentityMemoedCount();
    assertEquals(interpreted, run(QUERY, true));
    assertEquals(memoedBefore + 1, SirixVectorizedExecutor.groupIdentityMemoedCount(),
        "precondition: the production fingerprint earned the memo");
    // NO clearCache: the handle — and its memo — survive into the adversarial run. A memo keyed by
    // the column alone would now skip the proof and MERGE the five departments.
    ProjectionStringIdentityRegistry.installFingerprintForTesting(ALL_COLLIDE);
    final long preProvenBefore = SirixVectorizedExecutor.groupIdentityPreProvenCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final String underCollision = run(QUERY, true);
    assertEquals(20, countGroups(underCollision),
        "groups were merged under an inherited memo instead of proven: " + underCollision);
    assertEquals(interpreted, underCollision);
    assertEquals(preProvenBefore, SirixVectorizedExecutor.groupIdentityPreProvenCount(),
        "a memo earned under another fingerprint must not pre-prove anything");
    assertEquals(servedBefore, SirixVectorizedExecutor.groupAggServedCount(),
        "the adversarial fingerprint must still DECLINE — the memo did not launder it");
    assertEquals(memoedBefore + 1, SirixVectorizedExecutor.groupIdentityMemoedCount(), "a refuted scan notes nothing");
  }

  @Test
  @DisplayName("a predicated serve never earns the memo but uses one a full serve earned")
  void predicatedServeUsesButNeverEarnsTheMemo() throws Exception {
    final String interpretedPredicated = run(PREDICATED_QUERY, false);
    final String interpretedFull = run(QUERY, false);
    final long memoedBefore = SirixVectorizedExecutor.groupIdentityMemoedCount();
    final long preProvenBefore = SirixVectorizedExecutor.groupIdentityPreProvenCount();
    long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    assertEquals(interpretedPredicated, run(PREDICATED_QUERY, true));
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.groupAggServedCount(),
        "the predicated shape must serve — otherwise this proves nothing about coverage");
    assertEquals(memoedBefore, SirixVectorizedExecutor.groupIdentityMemoedCount(),
        "partial coverage proves only the strings its surviving rows name: it must not note the column");
    assertEquals(preProvenBefore, SirixVectorizedExecutor.groupIdentityPreProvenCount());
    servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    assertEquals(interpretedFull, run(QUERY, true));
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.groupAggServedCount());
    assertEquals(memoedBefore + 1, SirixVectorizedExecutor.groupIdentityMemoedCount(), "the full serve earns it");
    servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    assertEquals(interpretedPredicated, run(PREDICATED_QUERY, true));
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.groupAggServedCount());
    assertEquals(preProvenBefore + 1, SirixVectorizedExecutor.groupIdentityPreProvenCount(),
        "a subset of pairwise-distinct strings is pairwise distinct: the predicated serve is pre-proven");
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
