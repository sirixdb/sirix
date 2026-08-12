package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The morsel-parallel pipeline must answer exactly what the serial pipeline answers.
 *
 * <p>
 * Worth stating why this test exists in this shape. Morsel fan-out is off by default, so no other
 * test in the suite compiles a {@code MorselPipeExpr} at all — the path could break completely and
 * everything would stay green. It is also a path whose failures are quiet rather than loud: while
 * this was being built, a binding released too early made every worker scan the whole array instead
 * of its own split, and the query cheerfully returned twenty times the right count. Nothing threw.
 * So the assertions here are on ANSWERS against the serial pipeline, not on timing or on plan
 * shape.
 *
 * <p>
 * The array has to be genuinely large: splitting is declined below {@code sirix.morsel.minElements}
 * (65,536) and below 64 record pages, precisely so that small arrays do not pay for transactions
 * they cannot amortize. A smaller fixture would pass by taking the serial fallback, which is the
 * one outcome this test must not silently accept — hence {@link #splitsRatherThanFallingBack()}.
 */
public class MorselPipelineTest {

  /** Comfortably over the 65,536-element threshold, and enough nodes to span far past 64 pages. */
  private static final int ELEMENTS = 80_000;

  private static final String DB_NAME = "morsel-db";
  private static final String RESOURCE = "records";

  /** The STORE location — the directory databases live under, not the database itself. */
  private Path storeLocation;

  /** {@code storeLocation/DB_NAME}; a store resolves a database by directory name. */
  private Path databasePath;

  @Before
  public void setUp() throws Exception {
    storeLocation = Files.createTempDirectory("sirix-morsel-test");
    databasePath = storeLocation.resolve(DB_NAME);
    Databases.removeDatabase(databasePath);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    final StringBuilder json = new StringBuilder(ELEMENTS * 48);
    json.append('[');
    for (int i = 0; i < ELEMENTS; i++) {
      if (i > 0) {
        json.append(',');
      }
      // A mix of value types, so the split's element-kind filter is exercised on more than objects,
      // and a nested array so that non-member nodes share the pages being scanned.
      json.append("{\"id\":")
          .append(i)
          .append(",\"even\":")
          .append(i % 2 == 0)
          .append(",\"name\":\"n")
          .append(i % 997)
          .append('"')
          .append(",\"tags\":[\"a\",\"b\"]}");
    }
    json.append(']');

    try (final var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final var session = database.beginResourceSession(RESOURCE); final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }
  }

  @After
  public void tearDown() {
    SequentialPipelineStrategy.setMorselEnabled(false);
    Databases.removeDatabase(databasePath);
  }

  @Test
  public void morselAnswersMatchSerial() {
    final String[] queries = {
        // A filter whose predicate reads one field, the shape the fan-out was built for.
        "count(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] where $r.id > 40000 return $r)",
        // An aggregate over the RETURN expression, which runs on the workers too.
        "sum(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] return $r.id)",
        // No predicate at all: every element crosses the worker/consumer hand-off.
        "count(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] return $r)",
        // A boolean field, so the split yields elements whose matched value is not numeric.
        "count(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] where $r.even eq true() return $r)",
        // A string equality, and one that matches a small fraction.
        "count(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] where $r.name eq \"n42\" return $r)",
        // A nested array traversal — the pages a worker scans hold these members too, and they must
        // not be mistaken for members of the array being split.
        "count(for $r in jn:doc('" + DB_NAME + "','" + RESOURCE + "')[] "
            + "where (some $t in $r.tags[] satisfies $t eq \"a\") return $r)",};

    for (final String query : queries) {
      final String serial = run(query, false);
      final String morsel = run(query, true);
      assertEquals("morsel and serial disagree for: " + query, serial, morsel);
    }
  }

  /**
   * The fixture must be big enough that morsel actually splits.
   *
   * <p>
   * Without this, {@link #morselAnswersMatchSerial()} would keep passing if splitting silently
   * stopped happening — it would just be comparing the serial pipeline with itself.
   */
  @Test
  public void splitsRatherThanFallingBack() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storeLocation).build()) {
      final var collection = store.lookup(DB_NAME);
      final var document = (io.brackit.query.jdm.json.SplittableMembers) collection.getDocument();
      assertTrue("fixture too small to split; the answer comparison would be vacuous",
          document.memberSplitCount(8) > 1);
    }
  }

  private String run(final String query, final boolean morsel) {
    SequentialPipelineStrategy.setMorselEnabled(morsel);
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storeLocation).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = morsel
            ? SirixCompileChain.createWithMorsel(null, store)
            : SirixCompileChain.createWithJsonStore(store)) {
      final ByteArrayOutputStream sink = new ByteArrayOutputStream();
      try (final PrintStream out = new PrintStream(sink)) {
        new Query(chain, query).prettyPrint().serialize(ctx, out);
      }
      return sink.toString().trim();
    } finally {
      SequentialPipelineStrategy.setMorselEnabled(false);
    }
  }
}
