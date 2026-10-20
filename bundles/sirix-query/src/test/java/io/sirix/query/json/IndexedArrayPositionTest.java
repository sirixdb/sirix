package io.sirix.query.json;

import io.brackit.query.ErrorCode;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.utils.ChildCounts;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

@Isolated
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

  @ParameterizedTest
  @MethodSource("postingSources")
  void selectedEmptyArraysRaiseBoundsErrorsWithoutLeafPostings(final VersioningType versioning,
      final IndexType indexType) {
    final boolean cas = indexType == IndexType.CAS;
    final String value = cas
        ? "{\"item\":{\"v\":2}}"
        : "{\"v\":2}";
    final String item = cas
        ? ".item"
        : "";
    create(versioning, "{\"groups\":[{\"numbers\":[]},{\"numbers\":[" + value + "]}]}", cas
        ? "/groups/[]/numbers/[]/item/v"
        : "/groups/[]/numbers/[]/v", indexType);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String groups = "jn:doc('counts','" + resource + "').groups";
        for (final String position : new String[] {"[0]", "[-2]", "[]"}) {
          for (final int index : new int[] {-1, -2}) {
            final String source = groups + position + ".numbers[" + index + "]" + item;
            final String expression = indexType == IndexType.CAS
                ? source + "[?$$.v ge 1].v"
                : source + ".v";
            final QueryException exception =
                assertThrows(QueryException.class, () -> query(chain, context, expression), resource);
            assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
            assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
          }
        }
        assertEquals("2", query(chain, context, groups + "[-1].numbers[-1]" + item + ".v"), resource);
        assertEquals("", query(chain, context, groups + "[2].numbers[-1]" + item + ".v"), resource);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nestedArrayBoundsFollowOuterSelections(final VersioningType versioning) {
    create(versioning, "{\"root\":{\"numbers\":[[],[{\"v\":2}]]}}", "/root/numbers/[]/[]/v");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String numbers = "jn:doc('counts','" + resource + "').root.numbers";
        assertEquals("2", query(chain, context, numbers + "[-1][-1].v"), resource);
        assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
        for (final int index : new int[] {-1, -2}) {
          final QueryException exception = assertThrows(QueryException.class,
              () -> query(chain, context, numbers + "[0][" + index + "].v"), resource);
          assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void terminalIndexedArrayPositionsEnforceBounds(final VersioningType versioning) {
    final StringBuilder json = new StringBuilder(128).append("{\"root\":{\"numbers\":[");
    for (int i = 0; i < LENGTH; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(i);
    }
    create(versioning, json.append("]}}").toString(), "/root/numbers/[]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String numbers = "jn:doc('counts','" + resource + "').root.numbers";
        for (int index = -LENGTH; index <= LENGTH; index++) {
          final String expected = index < 0
              ? Integer.toString(LENGTH + index)
              : index == LENGTH
                  ? ""
                  : Integer.toString(index);
          assertEquals(expected, query(chain, context, numbers + "[" + index + "]"), resource + " at " + index);
          assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
        }
        final QueryException exception =
            assertThrows(QueryException.class, () -> query(chain, context, numbers + "[-25]"), resource);
        assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
        assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void consecutiveTerminalPositionsUseTheirContainingArrays(final VersioningType versioning) {
    create(versioning, "{\"root\":{\"numbers\":[[0],[1]]}}", "/root/numbers/[]/[]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String numbers = "jn:doc('counts','" + resource + "').root.numbers";
        for (int outer = -2; outer <= 2; outer++) {
          for (int inner = -1; inner <= 1; inner++) {
            final String expected = outer == 2 || inner == 1
                ? ""
                : Integer.toString(outer < 0
                    ? 2 + outer
                    : outer);
            assertTerminalPosition(chain, context, resource, numbers + "[" + outer + "][" + inner + "]", expected);
          }
          assertTerminalPosition(chain, context, resource, numbers + "[" + outer + "][]", outer == 2
              ? ""
              : Integer.toString(outer < 0
                  ? 2 + outer
                  : outer));
        }
        assertTerminalPosition(chain, context, resource, numbers + "[][-1]", "0 1");
        assertTerminalPosition(chain, context, resource, numbers + "[][]", "0 1");
        assertTerminalPosition(chain, context, resource, numbers + "[2][-2]", "");
        for (final String suffix : new String[] {"[-3][-1]", "[-2][-2]", "[0][-2]", "[][-2]"}) {
          final QueryException exception =
              assertThrows(QueryException.class, () -> query(chain, context, numbers + suffix), resource + suffix);
          assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
          assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
        }
        if (resource.endsWith("-indexed")) {
          final Query compiled = new Query(chain, numbers + "[-1][-1]");
          try (var counts = mockStatic(ChildCounts.class, CALLS_REAL_METHODS)) {
            assertEquals("1", query(context, compiled));
            counts.verifyNoInteractions();
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void consecutiveTerminalPositionsHandleThreeArrayLevels(final VersioningType versioning) {
    create(versioning, "{\"root\":{\"numbers\":[[[0,10],[1]],[[2],[3,13,23]]]}}", "/root/numbers/[]/[]/[]");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final String numbers = "jn:doc('counts','" + resource + "').root.numbers";
        final String[] suffixes = {"[-2][-1][-1]", "[-1][-1][-2]", "[1][1][1]", "[-1][][-1]", "[-2][-1][]", "[-1][][]",
            "[][][-1]", "[2][-3][-2]", "[-2][2][-2]"};
        final String[] expected = {"1", "13", "13", "2 23", "1", "2 3 13 23", "10 1 2 23", "", ""};
        for (int i = 0; i < suffixes.length; i++) {
          assertTerminalPosition(chain, context, resource, numbers + suffixes[i], expected[i]);
        }
        final QueryException exception =
            assertThrows(QueryException.class, () -> query(chain, context, numbers + "[-2][-1][-2]"), resource);
        assertEquals(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, exception.getCode());
      }
    }
  }

  private static void assertTerminalPosition(final SirixCompileChain chain, final SirixQueryContext context,
      final String resource, final String expression, final String expected) {
    assertEquals(expected, query(chain, context, expression), resource + " at " + expression);
    assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()), expression);
  }

  @ParameterizedTest
  @MethodSource("postingSources")
  void interleavedPostingsResolveEachOmittedArrayLengthOnce(final VersioningType versioning,
      final IndexType indexType) {
    final boolean cas = indexType == IndexType.CAS;
    create(versioning, "{\"groups\":[{\"numbers\":[]},{\"numbers\":[]}]}", cas
        ? "/groups/[]/numbers/[]/item/v"
        : "/groups/[]/numbers/[]/v", indexType);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.lookup("counts");
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        try (var session = collection.getDatabase().beginResourceSession(resource);
            var writer = session.beginNodeTrx()) {
          writer.moveToDocumentRoot();
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          final long firstGroup = writer.getNodeKey();
          writer.moveToFirstChild();
          final long firstArray = writer.getNodeKey();
          writer.moveTo(firstGroup);
          writer.moveToRightSibling();
          writer.moveToFirstChild();
          final long secondArray = writer.getNodeKey();
          for (int i = 0; i < LENGTH; i++) {
            for (final long array : new long[] {firstArray, secondArray}) {
              writer.moveTo(array);
              final String value = cas
                  ? "{\"item\":{\"v\":" + i + "}}"
                  : "{\"v\":" + i + "}";
              writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(value), JsonNodeTrx.Commit.NO);
            }
          }
          writer.commit();
        }
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String resource : new String[] {"stored-indexed", "stored-unindexed", "omitted-indexed",
          "omitted-unindexed"}) {
        final Long2IntOpenHashMap resolutions = new Long2IntOpenHashMap();
        final String source = "jn:doc('counts','" + resource + "').groups[].numbers[-2]" + (cas
            ? ".item"
            : "");
        final String expression = indexType == IndexType.CAS
            ? source + "[?$$.v ge 1].v"
            : source + ".v";
        final Query compiled = new Query(chain, expression);
        try (var counts = mockStatic(ChildCounts.class, CALLS_REAL_METHODS)) {
          counts.when(() -> ChildCounts.get(any(NodeReadOnlyTrx.class))).thenAnswer(call -> {
            final NodeReadOnlyTrx cursor = call.getArgument(0);
            resolutions.addTo(cursor.getNodeKey(), 1);
            return call.callRealMethod();
          });
          assertEquals("22 22", query(context, compiled), resource);
        }
        assertEquals(resource.endsWith("-indexed"), containsIndex(chain.getOptimizedAST()));
        if (resource.equals("omitted-indexed")) {
          assertEquals(2, resolutions.size());
          for (final int count : resolutions.values()) {
            assertEquals(1, count);
          }
        } else if (resource.equals("stored-indexed")) {
          assertEquals(0, resolutions.size());
        }
      }
    }
  }

  private static Stream<Arguments> postingSources() {
    return Stream.of(VersioningType.values())
                 .flatMap(versioning -> Stream.of(Arguments.of(versioning, IndexType.PATH),
                     Arguments.of(versioning, IndexType.CAS), Arguments.of(versioning, IndexType.NAME)));
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
    create(versioning, json, indexPath, IndexType.PATH);
  }

  private void create(final VersioningType versioning, final String json, final String indexPath,
      final IndexType indexType) {
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
              final var paths = Set.of(parse(indexPath, PathParser.Type.JSON));
              final IndexDef definition = switch (indexType) {
                case CAS -> IndexDefs.createCASIdxDef(false, Type.INR, paths, 0, IndexDef.DbType.JSON);
                case NAME -> IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
                default -> IndexDefs.createPathIdxDef(paths, 0, IndexDef.DbType.JSON);
              };
              controller.createIndexes(Set.of(definition), writer);
            }
            writer.commit();
          }
        }
      }
    }
  }

  private static String query(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    return query(context, new Query(chain, text));
  }

  private static String query(final SirixQueryContext context, final Query compiled) {
    final StringWriter output = new StringWriter();
    try (var writer = new PrintWriter(output)) {
      new StringSerializer(writer).serialize(compiled.execute(context));
    }
    return output.toString();
  }
}
