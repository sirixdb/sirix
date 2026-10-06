package io.sirix.query.json;

import io.brackit.query.Query;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonContainerReplacementRegressionTest {
  private static final String ORIGINAL = "{\"target\":{\"a\":[1,2,3],\"b\":{\"c\":4},\"d\":5},\"keep\":6}";
  private static final String REPLACED = "{\"target\":\"new\",\"keep\":6}";
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void queryReplacesContainerWithoutRevisitingDeletedChildren(final VersioningType versioning) {
    try (final BasicJsonDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("query", "data", ORIGINAL);
      context.setContextItem(collection.getDocument("data"));
      new Query(chain, "replace json value of $$.target with 'new'").execute(context);
      assertRevision(chain, context, collection, 2, REPLACED);
      assertRevision(chain, context, collection, 1, ORIGINAL);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void apiReplacesContainerWithoutRevisitingDeletedChildren(final VersioningType versioning) {
    try (final BasicJsonDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      final JsonDBCollection collection = store.create("api", "data", ORIGINAL);
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession("data");
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        assertTrue(trx.moveToFirstChild());
        final long rootKey = trx.getNodeKey();
        assertTrue(trx.moveToFirstChild());
        trx.replaceObjectRecordValue(new StringValue("new"));
        assertTrue(trx.moveTo(rootKey));
        assertEquals(2, trx.getChildCount());
        trx.commit();
      }
      assertRevision(chain, context, collection, 2, REPLACED);
      assertRevision(chain, context, collection, 1, ORIGINAL);
    }
  }

  private static void assertRevision(final SirixCompileChain chain, final SirixQueryContext context,
      final JsonDBCollection collection, final int revision, final String expected) {
    context.setContextItem(collection.getDocument("data", revision));
    final StringWriter output = new StringWriter();
    new Query(chain, "$$").serialize(context, new PrintWriter(output));
    assertEquals(expected, output.toString());
  }

  private BasicJsonDBStore newStore(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }
}
