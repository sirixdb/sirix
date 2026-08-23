package io.sirix.query;

import io.sirix.JsonTestHelper;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The empty string is a value, and a count over it must agree with the records.
 *
 * <p>
 * It used to answer {@code 0}. Three things had to line up, and the first two were individually
 * defensible:
 *
 * <ol>
 * <li>{@code KeyValueLeafPage.readFusedObjectNamedStringStoredBytes} answered {@code null} for a
 * zero-length payload, conflating "the value is the empty string" with "there is no payload here".
 * The PAX region builder drops every slot that method declines, so a page holding {@code ""} built
 * a string column with fewer values than the field had occurrences — and no {@code ""} dictionary
 * entry.</li>
 * <li>The column consumers' completeness oracle noticed exactly that and refused to serve such a
 * page, which is why the answer was merely slow rather than wrong for most literals.</li>
 * <li>The dictionary sketch was consulted BEFORE that oracle, and a Bloom negative was taken as the
 * page's final answer. For a literal the (incomplete) dictionary did not hold, the page returned a
 * confident zero instead of falling back to the records.</li>
 * </ol>
 *
 * <p>
 * Both the storage side and the ordering are fixed; this pins the observable consequence, because
 * that is the part that must never regress. Deliberately checked with and without a projection
 * index: with one, a different route serves the count and hid the defect completely — which is how
 * it survived this long.
 */
public final class EmptyStringPredicateCountTest extends AbstractJsonTest {

  /** Two rows carry the empty string, one omits the field entirely, the rest carry real values. */
  private static final String STORE = """
        jn:store('json-path1','empties.jn','[
          {"kind":"commit","did":"did:plc:aaaa","n":1},
          {"kind":"commit","did":"did:plc:aaab","n":2},
          {"kind":"commit","did":"","n":3},
          {"kind":"identity","did":"did:plc:cccc","n":4},
          {"kind":"commit","n":5},
          {"kind":"commit","did":"did:plc:aaaa","n":6},
          {"kind":"account","did":"did:plc:dddd","n":7},
          {"kind":"commit","did":"did:plc:eeee","n":8},
          {"kind":"commit","did":"did:plc:ffff","n":9},
          {"kind":"identity","did":"","n":10}
        ]')
      """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','empties.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/n'), ('string', 'string', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  /** A present literal, an absent one, and the empty string that used to answer 0. */
  private static final String COUNTS = """
        (count(for $e in jn:doc('json-path1','empties.jn')[] where $e.did eq "did:plc:aaaa" return $e),
         count(for $e in jn:doc('json-path1','empties.jn')[] where $e.did eq "" return $e),
         count(for $e in jn:doc('json-path1','empties.jn')[] where $e.did eq "nope" return $e))
      """;

  /** The empty string must also be returnable, not just countable. */
  private static final String EMIT_EMPTIES = """
        count(for $e in jn:doc('json-path1','empties.jn')[]
              where $e.did eq "" return $e.n)
      """;

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
  public void emptyStringCountsWithoutAProjectionIndex() throws IOException {
    JsonTestHelper.deleteEverything();
    query(STORE);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Assertions.assertEquals("2 2 0", answer(COUNTS),
        "a count over the empty string disagreed with the records — the column path answered from a "
            + "string dictionary that never held it");
    Assertions.assertEquals("2", answer(EMIT_EMPTIES));
  }

  @Test
  public void emptyStringCountsWithAProjectionIndex() throws IOException {
    JsonTestHelper.deleteEverything();
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Assertions.assertEquals("2 2 0", answer(COUNTS));
    Assertions.assertEquals("2", answer(EMIT_EMPTIES));
  }

  private String answer(final String q) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new io.brackit.query.Query(chain, q).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString().trim();
    }
  }
}
