package io.sirix.query.json;

import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class IndexedArrayPositionTest {
  private static final int LENGTH = 24;

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void indexedNegativePositionsUseTheContainingArray(final VersioningType versioning) {
    create(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String numbers = "jn:doc('counts','" + resource + "').numbers";
        for (int index = -1; index >= -LENGTH; index--) {
          assertPosition(chain, context, resource, numbers, index, Integer.toString(LENGTH + index));
        }
        for (int index = 0; index <= LENGTH; index++) {
          assertPosition(chain, context, resource, numbers, index, index == LENGTH
              ? ""
              : Integer.toString(index));
        }
        for (final String expression : new String[] {numbers + "[-25].v", "count(" + numbers + "[-25])"}) {
          final QueryException exception = assertThrows(QueryException.class, () -> query(chain, context, expression),
              resource + " out-of-range negative position");
          assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void negativeBoundsAreCheckedOnlyForTheSelectedContainingArray(final VersioningType versioning) {
    create(versioning, "{\"groups\":[{\"numbers\":[{\"v\":0},{\"v\":1}]},{\"numbers\":[{\"v\":2}]}]}",
        "/groups/[]/numbers/[]/v");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String groups = "jn:doc('counts','" + resource + "').groups";
        assertEquals("0", query(chain, context, groups + "[0].numbers[-2].v"), resource);
        assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
        assertEquals("2", query(chain, context, groups + "[1].numbers[-1].v"), resource);
        final QueryException exception =
            assertThrows(QueryException.class, () -> query(chain, context, groups + "[1].numbers[-2].v"), resource);
        assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
      }
    }
  }

  private static void assertPosition(final SirixCompileChain chain, final SirixQueryContext context,
      final String resource, final String numbers, final int index, final String expected) {
    assertEquals(expected, query(chain, context, numbers + "[" + index + "].v"), resource + " at " + index);
    assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()),
        resource + " must use its expected query route at " + index);
    assertEquals(expected.isEmpty()
        ? "0"
        : "1", query(chain, context, "count(" + numbers + "[" + index + "])"),
        resource + " direct array position " + index);
  }

  private static boolean containsIndex(final AST node) {
    if (node.getType() == XQExt.IndexExpr) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (containsIndex(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private void create(final VersioningType versioning) {
    final StringBuilder json = new StringBuilder(256).append("{\"numbers\":[");
    for (int i = 0; i < LENGTH; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"v\":").append(i).append('}');
    }
    json.append("]}");
    create(versioning, json.toString(), "/numbers/[]/v");
  }

  private void create(final VersioningType versioning, final String json, final String indexPath) {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.create("counts");
      for (final boolean stored : new boolean[] {true, false}) {
        for (final boolean indexed : new boolean[] {true, false}) {
          final String resource = (stored
              ? "stored"
              : "omitted")
              + (indexed
                  ? "-indexed"
                  : "-unindexed");
          collection.getDatabase()
                    .createResource(ResourceConfiguration.newBuilder(resource)
                                                         .storageType(StorageType.FILE_CHANNEL)
                                                         .versioningApproach(versioning)
                                                         .storeChildCount(stored)
                                                         .buildPathSummary(true)
                                                         .build());
          try (var session = collection.getDatabase().beginResourceSession(resource);
              var writer = session.beginNodeTrx()) {
            writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
            if (indexed) {
              final JsonIndexController controller =
                  (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
              controller.createIndexes(Set.of(
                  IndexDefs.createPathIdxDef(Set.of(parse(indexPath, PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)),
                  writer);
            }
            writer.commit();
          }
        }
      }
    }
  }

  private static String query(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    final StringWriter output = new StringWriter();
    try (var writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(new Query(chain, text).execute(context));
    }
    return output.toString();
  }
}
