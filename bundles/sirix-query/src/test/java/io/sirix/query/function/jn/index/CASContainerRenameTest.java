package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.sdb.explain.QueryPlan;
import io.sirix.query.function.jn.temporal.ValidTimeFilter;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static java.util.Objects.requireNonNull;

final class CASContainerRenameTest {
  private static final int FILLERS = 2_048;

  @TempDir
  Path directory;

  static Stream<Arguments> scenarios() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(
                     versioning -> Stream.of(false, true)
                                         .flatMap(
                                             object -> Stream.of(false, true)
                                                             .map(shared -> Arguments.of(versioning, object, shared))));
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void populatedRenamesMaintainEveryIndexAcrossCommitAndReopen(final VersioningType versioning, final boolean object,
      final boolean shared) {
    final String oldName = object
        ? "oldHolder"
        : "oldRows";
    final String newName = object
        ? "holder"
        : "rows";
    final String prefix = object
        ? "/holder/rows/[]"
        : "/rows/[]";
    final String oldPrefix = object
        ? "/oldHolder/rows/[]"
        : "/oldRows/[]";
    final String selector = object
        ? ".holder.rows"
        : ".rows";
    final String oldSelector = object
        ? ".oldHolder.rows"
        : ".oldRows";
    final Path databasePath = directory.resolve("containers");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("vf", "vt")
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final String payload = object
            ? "{\"rows\":" + rows(10, true) + "}"
            : rows(10, true);
        final String other = object
            ? "{\"rows\":" + rows(20, false) + "}"
            : rows(20, false);
        final String json = "{\"" + oldName + "\":" + payload + (shared
            ? ",\"" + oldName + "\":" + other
            : "") + "}";
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        final Set<IndexDef> definitions = new HashSet<>();
        definitions.add(cas(prefix + "/id", Type.INR, 0));
        definitions.add(cas(oldPrefix + "/id", Type.INR, 1));
        definitions.add(cas(prefix + "/item/id", Type.INR, 2));
        definitions.add(cas(prefix + "/label", Type.STR, 3));
        definitions.add(cas(prefix + "/enabled", Type.BOOL, 4));
        definitions.add(cas(prefix + "/samples/[]", Type.INR, 5));
        definitions.add(cas(prefix + "/samples/[]", Type.STR, 6));
        definitions.add(cas(prefix + "/samples/[]", Type.BOOL, 7));
        definitions.add(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON));
        definitions.add(
            IndexDefs.createPathIdxDef(Set.of(parse(prefix + "/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON));
        definitions.add(IndexDefs.createPathIdxDef(Set.of(), 1, IndexDef.DbType.JSON));
        definitions.add(IndexDefs.createValidTimeIdxDef(
            Set.of(parse(prefix + "/vf", PathParser.Type.JSON), parse(prefix + "/vt", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON));
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(definitions, writer);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long container = writer.getNodeKey();
        writer.commit();
        for (final String name : List.of(newName, oldName, newName)) {
          writer.moveTo(container);
          writer.setObjectKeyName(name);
          assertEquals(container, writer.getNodeKey());
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var optimized = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final CompileChain generic = new CompileChain();
      final var session = store.lookup("containers").getDatabase().beginResourceSession("rows");
      for (int revision = 1; revision <= 4; revision++) {
        final boolean indexed = revision % 2 == 0;
        final String document = "jn:doc('containers','rows'," + revision + ")";
        assertQueryResults(generic, optimized, context, document, selector, oldSelector, indexed, shared);
        assertCASPostings(optimized, context, document, prefix, oldPrefix, indexed, shared);
        assertSecondaryIndexes(session, revision, prefix, indexed, shared);
        final var root = requireNonNull(store.lookup("containers").getDocument("rows", revision));
        JsonDBItem scope = (JsonDBItem) requireNonNull(((Object) root).get(new QNm(indexed
            ? newName
            : oldName)));
        if (object) {
          scope = (JsonDBItem) requireNonNull(((Object) scope).get(new QNm("rows")));
        }
        final var config = requireNonNull(session.getResourceConfig().getValidTimeConfig());
        final Instant instant = Instant.parse("2020-06-01T00:00:00Z");
        assertEquals(10, total(ValidTimeFilter.linearScanSequence(scope, instant, config)));
        assertEquals(10, total(requireNonNull(ValidTimeIntervalIndex.sequence(scope, instant, config, false, false))));
      }
      assertEquals("CAS", QueryPlan
                                   .explain("for $c in jn:doc('containers','rows')" + selector
                                       + "[] where $c.id eq 1 return $c.value", store, context.getNodeStore())
                                   .indexType());
      assertEquals("CAS",
          QueryPlan.explain("jn:doc('containers','rows')" + selector + "[][?$$.id eq 1]", store, context.getNodeStore())
                   .indexType());
      assertEquals("CAS", QueryPlan
                                   .explain("jn:doc('containers','rows')" + selector + "[].item[?$$.id eq 1]", store,
                                       context.getNodeStore())
                                   .indexType());
    }
  }

  private static void assertQueryResults(final CompileChain generic, final SirixCompileChain optimized,
      final SirixQueryContext context, final String document, final String selector, final String oldSelector,
      final boolean indexed, final boolean shared) {
    final String source = document + selector + "[]";
    final List<String> queries = List.of("sum(for $c in " + source + " where $c.id eq 1 return $c.value)",
        "sum(" + source + "[?$$.id eq 1].value)", "sum(" + source + ".item[?$$.id eq 1].value)",
        "sum(" + source + ".item[?$$.id ge 1 and $$.id le 2].value)");
    for (final String query : queries) {
      assertEquals(indexed
          ? 10
          : 0, ((Numeric) new Query(generic, query).execute(context)).intValue(), query);
      assertEquals(indexed
          ? 10
          : 0, ((Numeric) new Query(optimized, query).execute(context)).intValue(), query);
    }
    final String oldQuery = "sum(for $c in " + document + oldSelector + "[] where $c.id eq 1 return $c.value)";
    final int oldValue = indexed
        ? (shared
            ? 20
            : 0)
        : 10;
    assertEquals(oldValue, ((Numeric) new Query(generic, oldQuery).execute(context)).intValue());
    assertEquals(oldValue, ((Numeric) new Query(optimized, oldQuery).execute(context)).intValue());
  }

  private static void assertCASPostings(final SirixCompileChain optimized, final SirixQueryContext context,
      final String document, final String prefix, final String oldPrefix, final boolean indexed, final boolean shared) {
    assertCount(optimized, context, document, 0, "1", prefix + "/id", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 1, "1", oldPrefix + "/id", indexed
        ? (shared
            ? 1
            : 0)
        : (shared
            ? 2
            : 1));
    assertCount(optimized, context, document, 2, "1", prefix + "/item/id", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 3, "'one'", prefix + "/label", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 4, "true()", prefix + "/enabled", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 5, "1", prefix + "/samples/[]", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 6, "'one'", prefix + "/samples/[]", indexed
        ? 1
        : 0);
    assertCount(optimized, context, document, 7, "true()", prefix + "/samples/[]", indexed
        ? 2
        : 0);
  }

  private static void assertSecondaryIndexes(final JsonResourceSession session, final int revision, final String prefix,
      final boolean indexed, final boolean shared) {
    try (var reader = session.beginNodeReadOnlyTrx(revision)) {
      final var controller = session.getRtxIndexController(revision);
      final IndexDef primary = controller.getIndexes().getIndexDef(0, IndexType.CAS);
      assertTrue(primary.hasNumericValuesOnly());
      assertTrue(primary.hasCompleteNumericCoverage());
      assertEquals(indexed
          ? FILLERS + 1
          : 0,
          count(controller.openPathIndex(reader.getStorageEngineReader(),
              controller.getIndexes().getIndexDef(0, IndexType.PATH),
              controller.createPathFilter(Set.of(prefix + "/id"), reader))));
      assertEquals(indexed
          ? 1
          : 0,
          count(controller.openPathIndex(reader.getStorageEngineReader(),
              controller.getIndexes().getIndexDef(1, IndexType.PATH),
              controller.createPathFilter(Set.of(prefix + "/label"), reader))));
      assertEquals(shared
          ? 2
          : 1,
          count(controller.openNameIndex(reader.getStorageEngineReader(),
              controller.getIndexes()
                        .getIndexDef(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID(), IndexType.NAME),
              controller.createNameFilter(Set.of("label")))));
    }
  }

  private static String rows(final int value, final boolean fillers) {
    final StringBuilder rows = new StringBuilder("[{\"id\":1,\"value\":" + value + ",\"item\":{\"id\":1,\"value\":"
        + value + "},\"label\":\"one\",\"enabled\":true,\"samples\":[1,\"one\",true,null],"
        + "\"vf\":\"2020-01-01T00:00:00Z\",\"vt\":\"2021-01-01T00:00:00Z\"}");
    if (fillers) {
      for (int i = 0; i < FILLERS; i++) {
        rows.append(",{\"id\":0,\"value\":0,\"item\":{\"id\":0,\"value\":0},"
            + "\"vf\":\"2030-01-01T00:00:00Z\",\"vt\":\"2031-01-01T00:00:00Z\"}");
      }
    }
    return rows.append(']').toString();
  }

  private static IndexDef cas(final String path, final Type type, final int id) {
    return IndexDefs.createCASIdxDef(false, type, Set.of(parse(path, PathParser.Type.JSON)), id, IndexDef.DbType.JSON);
  }

  private static long count(final Iterator<NodeReferences> postings) {
    long count = 0;
    while (postings.hasNext()) {
      count += postings.next().cardinality();
    }
    return count;
  }

  private static long total(final Sequence sequence) {
    long total = 0;
    try (var iterator = sequence.iterate()) {
      Item item;
      while ((item = iterator.next()) != null) {
        total += ((Numeric) requireNonNull(((Object) item).get(new QNm("value")))).longValue();
      }
    }
    return total;
  }

  private static void assertCount(final SirixCompileChain chain, final SirixQueryContext context, final String document,
      final int index, final String value, final String path, final int expected) {
    final String query = "count(jn:scan-cas-index(" + document + "," + index + "," + value + ",'==','" + path + "'))";
    assertEquals(expected, ((Numeric) new Query(chain, query).execute(context)).intValue(), query);
  }
}
