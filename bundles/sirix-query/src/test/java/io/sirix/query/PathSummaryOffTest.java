package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Path statistics default to on and cannot exist without a path summary, so turning the summary off
 * must turn the DEFAULTED statistics off with it rather than make every resource creation throw.
 * Asking for both explicitly is still an error — that caller wants something impossible.
 */
public final class PathSummaryOffTest {

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void aStoreWithoutAPathSummaryStillCreatesResources() throws Exception {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .buildPathSummary(false)
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('json-path1','a.jn','[{\"age\":10},{\"age\":20}]')").evaluate(ctx);
      try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
           final PrintWriter pw = new PrintWriter(out)) {
        new Query(chain, "sum(for $r in jn:doc('json-path1','a.jn')[] return $r.age)").serialize(ctx, pw);
        pw.flush();
        assertEquals("30", out.toString());
      }
    }
  }

  @Test
  public void askingForBothExplicitlyIsStillRejected() {
    assertThrows(IllegalStateException.class,
                 () -> BasicJsonDBStore.newBuilder()
                                       .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                       .buildPathSummary(false)
                                       .buildPathStatistics(true)
                                       .build()
                                       .create("json-path1", "a.jn", "[{\"age\":1}]"));
  }
}
