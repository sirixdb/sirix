package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * A predicate is compiled by resolving every field it names to an int nameKey, and the resolution
 * has two implementations that return the SAME key: a path-summary probe, and a fallback that walks
 * every page of the document looking for the first object key with that local name. Nothing about
 * an answer distinguishes them, which is why {@link SirixVectorizedExecutor#fieldKeyWalkCount()}
 * exists and why these tests assert on it.
 *
 * <p>
 * The walk's worst case is the common one. A nested chain reaches the resolver as its JOINED path
 * ({@code commit/operation}), and no single object key is ever named that, so the walk's early exit
 * never fires: it scans the whole document to return {@code -1}. On the 1M-event JSONBench corpus
 * that was ~500 ms per referenced field per query — an order of magnitude more than the aggregate
 * the query actually asked for.
 *
 * <p>
 * The path summary decides both directions, because it is complete over exactly the node kinds the
 * walk scans: every {@code OBJECT_KEY}, and every fused {@code OBJECT_NAMED_*} (which
 * {@code PathSummaryWriter} files under the {@code OBJECT_KEY} path kind), contributes a path node
 * bearing its local name. So these tests pin the risky direction too — a field that IS in the
 * document but in no projection must still resolve to its real key, since a spurious {@code -1}
 * would make the row path compare every name against a key nothing carries and quietly answer
 * nothing.
 */
public final class FieldKeyResolutionTest extends AbstractJsonTest {

  /**
   * JSONBench's nesting, plus two things the corpus needs to be a test: {@code note} is present but
   * deliberately OUTSIDE the projection, and the last two records carry no {@code commit} at all.
   */
  private static final String STORE =
      """
            jn:store('json-path1','events.jn','[
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.post",    "operation": "create"}, "note": "keep", "n": 1},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.like",    "operation": "create"}, "note": "drop", "n": 2},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.post",    "operation": "delete"}, "note": "keep", "n": 4},
              {"kind": "identity", "commit": {"collection": "app.bsky.feed.post",    "operation": "create"}, "note": "drop", "n": 8},
              {"kind": "commit",   "commit": {"collection": "app.bsky.graph.follow", "operation": "create"}, "note": "keep", "n": 16},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.repost",  "operation": "create"}, "note": "drop", "n": 32},
              {"kind": "commit",   "note": "keep", "n": 64},
              {"kind": "account",  "note": "drop", "n": 128}
            ]')
          """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','events.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/commit/collection', '/[]/commit/operation', '/[]/n'),
            ('string', 'string', 'string', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  /** The two-conjunct filter JSONBench Q2-Q5 share, one top-level field and one nested chain. */
  private static final String CREATE_COMMIT = "$r.kind eq 'commit' and $r.commit.operation eq 'create'";

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void aNestedChainPredicateResolvesWithoutWalkingTheDocument() throws IOException {
    final String filtered = """
          let $doc := jn:doc('json-path1','events.jn')
          for $r in $doc[]
          where %s
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """.formatted(CREATE_COMMIT);

    withFixture((chain, ctx, session) -> {
      final String generic = evaluateQuery(chain, ctx, filtered);
      // kind='commit' AND operation='create' keeps records 1, 2, 5 and 6; the identity record fails
      // the first conjunct, the delete fails the second, and the two commit-less records fail it
      // too — an absent field never satisfies an equality.
      Assertions.assertEquals("{\"collection\":\"app.bsky.feed.post\",\"n\":1,\"total\":1}"
          + " {\"collection\":\"app.bsky.feed.like\",\"n\":1,\"total\":2}"
          + " {\"collection\":\"app.bsky.graph.follow\",\"n\":1,\"total\":16}"
          + " {\"collection\":\"app.bsky.feed.repost\",\"n\":1,\"total\":32}", generic);

      withExecutor(session, () -> {
        final long walksBefore = SirixVectorizedExecutor.fieldKeyWalkCount();
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, filtered),
            "the two-conjunct nested filter must fold exactly like the generic pipeline");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the JSONBench filter shape must be SERVED from the projection");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.fieldKeyWalkCount() - walksBefore,
            "'commit/operation' names no object key, so the walk would scan the whole document to "
                + "return -1 — the path summary must answer instead");
      });
    });
  }

  @Test
  public void anInListOverADictColumnResolvesWithoutWalkingTheDocument() throws IOException {
    // JSONBench Q3's filter: the shared two conjuncts AND a three-literal IN list, which arrives as
    // a disjunction and takes the predicate-TREE route rather than the conjunctive one.
    final String inList = """
          let $doc := jn:doc('json-path1','events.jn')
          for $r in $doc[]
          where %s
            and ($r.commit.collection eq 'app.bsky.feed.post'
              or $r.commit.collection eq 'app.bsky.feed.repost'
              or $r.commit.collection eq 'app.bsky.feed.like')
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """.formatted(CREATE_COMMIT);
    // Every literal is absent from the corpus: no leaf dictionary interns any of them, so each leaf
    // is ruled out from its dictionary alone and no row is ever visited.
    final String noneMatch = """
          let $doc := jn:doc('json-path1','events.jn')
          for $r in $doc[]
          where %s
            and ($r.commit.collection eq 'app.bsky.feed.nope'
              or $r.commit.collection eq 'app.bsky.feed.alsonope')
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """.formatted(CREATE_COMMIT);

    withFixture((chain, ctx, session) -> {
      final String genericIn = evaluateQuery(chain, ctx, inList);
      final String genericNone = evaluateQuery(chain, ctx, noneMatch);
      Assertions.assertEquals("{\"collection\":\"app.bsky.feed.post\",\"n\":1,\"total\":1}"
          + " {\"collection\":\"app.bsky.feed.like\",\"n\":1,\"total\":2}"
          + " {\"collection\":\"app.bsky.feed.repost\",\"n\":1,\"total\":32}", genericIn);
      Assertions.assertEquals("", genericNone, "no record carries any of the two literals");

      withExecutor(session, () -> {
        final long walksBefore = SirixVectorizedExecutor.fieldKeyWalkCount();
        Assertions.assertEquals(genericIn, evaluateQuery(chain, ctx, inList),
            "the IN-list filter must fold exactly like the generic pipeline");
        Assertions.assertEquals(genericNone, evaluateQuery(chain, ctx, noneMatch),
            "a literal no leaf dictionary holds must yield no groups, not a wrong one");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.fieldKeyWalkCount() - walksBefore,
            "an IN list names the same nested chain three times and must still never walk");
      });
    });
  }

  @Test
  public void anAbsentFieldResolvesToNoKeyWithoutWalkingTheDocument() throws IOException {
    final String absent = """
          let $doc := jn:doc('json-path1','events.jn')
          for $r in $doc[]
          where $r.nosuchfield eq 'anything'
          let $k := $r.kind
          group by $k
          return {"kind": $k, "n": count($r)}
        """;

    withFixture((chain, ctx, session) -> {
      final String generic = evaluateQuery(chain, ctx, absent);
      Assertions.assertEquals("", generic, "no record carries the field, so nothing survives the filter");

      withExecutor(session, () -> {
        final long walksBefore = SirixVectorizedExecutor.fieldKeyWalkCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, absent),
            "a predicate over an absent field must select nothing, exactly like the generic pipeline");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.fieldKeyWalkCount() - walksBefore,
            "a summary that lacks the name PROVES the walk would return -1 — do not pay for the proof");
      });
    });
  }

  @Test
  public void aFieldOutsideTheProjectionStillResolvesToItsRealKey() throws IOException {
    // `note` exists on every record but is in no projection column, so the path summary is the ONLY
    // thing that can prove its key. A spurious -1 here would leave the row path comparing names
    // against a key nothing carries: it would answer EMPTY, and no other assertion would notice.
    final String unprojected = """
          let $doc := jn:doc('json-path1','events.jn')
          for $r in $doc[]
          where $r.note eq 'keep'
          let $k := $r.kind
          group by $k
          return {"kind": $k, "n": count($r), "total": sum($r.n)}
        """;

    withFixture((chain, ctx, session) -> {
      final String generic = evaluateQuery(chain, ctx, unprojected);
      Assertions.assertEquals("{\"kind\":\"commit\",\"n\":4,\"total\":85}", generic);

      withExecutor(session, () -> {
        final long walksBefore = SirixVectorizedExecutor.fieldKeyWalkCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, unprojected),
            "a present-but-unprojected field must resolve to its real key, not to -1");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.fieldKeyWalkCount() - walksBefore,
            "the summary proves presence for an unprojected field too");
      });
    });
  }

  @Test
  public void withoutAPathSummaryTheDocumentWalkStillAnswers() throws IOException {
    // The proof needs a summary. Without one there is nothing to consult, so the fallback must
    // remain reachable AND correct — this is the configuration that still pays for a walk.
    try (
        final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                       .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                       .buildPathSummary(false)
                                                       .build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, STORE).evaluate(ctx);
      final String summed = """
            let $doc := jn:doc('json-path1','events.jn')
            return sum(for $r in $doc[] where $r.kind eq 'commit' return $r.n)
          """;
      final String generic = evaluateQuery(chain, ctx, summed);
      Assertions.assertEquals("119", generic, "the six commits carry n = 1, 2, 4, 16, 32 and 64");

      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("events.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long walksBefore = SirixVectorizedExecutor.fieldKeyWalkCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, summed),
            "without a summary the walk must still resolve 'kind' and the answer must not change");
        Assertions.assertTrue(SirixVectorizedExecutor.fieldKeyWalkCount() - walksBefore >= 1L,
            "with no summary to consult there is no proof available — the walk is the fallback and "
                + "must stay reachable");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /** What a test body does once the fixture is stored, indexed and open. */
  @FunctionalInterface
  private interface FixtureBody {
    void run(SirixCompileChain chain, SirixQueryContext ctx, JsonResourceSession session) throws IOException;
  }

  /** What a test body does with the vectorized executor wired in. */
  @FunctionalInterface
  private interface ExecutorBody {
    void run() throws IOException;
  }

  /** Stores the corpus, builds the projection, and opens a session over it. */
  private void withFixture(final FixtureBody body) throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      body.run(chain, ctx, collection.getDatabase().beginResourceSession("events.jn"));
    }
  }

  /**
   * Runs {@code body} with a fresh executor wired in. The generic answer must be taken BEFORE this —
   * once wired, the fast paths answer instead, which is the whole point of the comparison.
   */
  private static void withExecutor(final JsonResourceSession session, final ExecutorBody body) throws IOException {
    final SirixVectorizedExecutor executor =
        new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
    SequentialPipelineStrategy.setVectorizedExecutor(executor);
    try {
      body.run();
    } finally {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      executor.close();
    }
  }

  private static String evaluateQuery(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr)
      throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, queryStr).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
