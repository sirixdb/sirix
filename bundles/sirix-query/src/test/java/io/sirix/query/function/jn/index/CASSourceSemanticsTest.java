package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.sdb.explain.QueryPlan;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASSourceSemanticsTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void numericCandidatesKeepEqualityAndHistoricalMaintenance(final VersioningType versioning) {
    create("[{\"id\":1.0,\"value\":10},{\"id\":1.5,\"value\":15},{\"id\":2.0,\"value\":20}]", "/[]/id", versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var optimized = SirixCompileChain.createWithJsonStore(store)) {
      final CompileChain generic = new CompileChain();
      for (final String probe : List.of("1", "1.0", "1.5", "xs:decimal('1')", "xs:double('1')")) {
        for (final boolean filter : List.of(false, true)) {
          final String text = point("jn:doc('case','rows')[]", probe, filter);
          final List<Double> expected = probe.equals("1.5")
              ? List.of(15d)
              : List.of(10d);
          assertEquals(expected, values(new Query(generic, text).execute(context)));
          assertEquals(expected, values(new Query(optimized, text).execute(context)));
        }
      }
      assertTrue(
          QueryPlan.explain(point("jn:doc('case','rows')[]", "1", false), store, context.getNodeStore()).usesIndex());
      assertEquals(List.of(1d), values(
          new Query(optimized, "count(jn:scan-cas-index(jn:doc('case','rows'),0,1,'==','/[]/id'))").execute(context)));
      assertEquals(List.of(1d), values(new Query(optimized,
          "count(jn:scan-cas-index-range(jn:doc('case','rows'),0,1,2,false(),true(),'/[]/id'))").execute(context)));
    }
    try (var database = Databases.openJsonDatabase(directory.resolve("case"));
        var session = database.beginResourceSession("rows");
        var writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"id\":1.0,\"value\":30}"),
          JsonNodeTrx.Commit.NO);
      writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.moveToFirstChild();
      writer.moveToFirstChild();
      writer.setNumberValue(2.0);
      writer.commit(null, Instant.parse("2022-01-01T00:00:00Z"));
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.moveToLastChild();
      writer.remove();
      writer.commit(null, Instant.parse("2023-01-01T00:00:00Z"));
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final CompileChain generic = new CompileChain();
      final List<List<Double>> expected = List.of(List.of(10d), List.of(10d, 30d), List.of(30d), List.of());
      for (int year = 2020; year <= 2023; year++) {
        final String source = "jn:open('case','rows',xs:dateTime('" + year + "-01-01T00:00:00Z'))[]";
        for (final boolean filter : List.of(false, true)) {
          final String text = point(source, "1", filter);
          assertEquals(expected.get(year - 2020), values(new Query(generic, text).execute(context)));
          assertEquals(expected.get(year - 2020), values(new Query(chain, text).execute(context)));
        }
        final String scan = "count(jn:scan-cas-index(jn:open('case','rows',xs:dateTime('" + year
            + "-01-01T00:00:00Z')),0,1,'==','/[]/id'))";
        assertEquals(List.of((double) expected.get(year - 2020).size()),
            values(new Query(chain, scan).execute(context)));
        final String range = "count(jn:scan-cas-index-range(jn:open('case','rows',xs:dateTime('" + year
            + "-01-01T00:00:00Z')),0,1,2,false(),true(),'/[]/id'))";
        assertEquals(List.of(year < 2022
            ? 1d
            : 2d), values(new Query(chain, range).execute(context)));
      }
    }
  }

  @Test
  void legacyIntegerRoutesKeepFractionalRangeAndExactEqualityResults() {
    final StringBuilder json = new StringBuilder("[{\"item\":{\"id\":1.0,\"value\":10}},"
        + "{\"item\":{\"id\":1.5,\"value\":15}},{\"item\":{\"id\":2.0,\"value\":20}}");
    for (int i = 0; i < 2_048; i++) {
      json.append(",{\"item\":{\"id\":0,\"value\":0}}");
    }
    json.append(']');
    create(json.toString(), "/[]/item/id", VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final CompileChain generic = new CompileChain();
      assertTrue(QueryPlan.explain("jn:doc('case','rows')[].item[?$$.id eq 1].value", store, context.getNodeStore())
                          .usesIndex());
      for (final String predicate : List.of("eq 1", "gt 1", "ge 1", "lt 2", "le 2")) {
        final String text = "jn:doc('case','rows')[].item[?$$.id " + predicate + "].value";
        assertEquals(values(new Query(generic, text).execute(context)),
            values(new Query(chain, text).execute(context)));
      }
      assertEquals(List.of(10d),
          values(new Query(chain, "jn:doc('case','rows')[].item[?$$.id eq 1].value").execute(context)));
      assertEquals(List.of(15d, 20d),
          values(new Query(chain, "jn:doc('case','rows')[].item[?$$.id gt 1].value").execute(context)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void positionalFocusAndForPositionsRemainRelativeToOriginalRows(final boolean named) {
    create(named
        ? "{\"rows\":[{\"id\":0,\"value\":0},{\"id\":1,\"value\":10}]}"
        : "[{\"id\":0,\"value\":0},{\"id\":1,\"value\":10}]",
        named
            ? "/rows/[]/id"
            : "/[]/id",
        VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final CompileChain generic = new CompileChain();
      final String source = "jn:doc('case','rows')" + (named
          ? ".rows[]"
          : "[]");
      for (final String focus : List.of("position() eq 2", "last() eq 2", "position() eq last()")) {
        final String text = source + "[?$$.id eq 1 and " + focus + "].value";
        assertEquals(List.of(10d), values(new Query(generic, text).execute(context)));
        assertEquals(List.of(10d), values(new Query(chain, text).execute(context)));
      }
      final String flwor = "for $c at $p in " + source + " where $c.id eq 1 return $p";
      assertEquals(List.of(2d), values(new Query(generic, flwor).execute(context)));
      assertEquals(List.of(2d), values(new Query(chain, flwor).execute(context)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"rows", "holder", "matching", "field"})
  void duplicateFieldsKeepTheOriginalSourceIdentity(final String shape) {
    final String json = switch (shape) {
      case "rows" -> "{\"rows\":[{\"id\":0}],\"rows\":[{\"id\":1,\"value\":10}]}";
      case "holder" -> "{\"holder\":{\"rows\":[{\"id\":0}]},\"holder\":{\"rows\":[{\"id\":1,\"value\":10}]}}";
      case "matching" -> "{\"rows\":[{\"id\":1,\"value\":10}],\"rows\":[{\"id\":1,\"value\":20}]}";
      case "field" -> "[{\"id\":0,\"id\":1,\"value\":10}]";
      default -> throw new AssertionError(shape);
    };
    final String prefix = shape.equals("holder")
        ? "/holder/rows"
        : shape.equals("field")
            ? ""
            : "/rows";
    create(json, prefix + "/[]/id", VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final CompileChain generic = new CompileChain();
      final String source = "jn:doc('case','rows')" + prefix.replace('/', '.') + "[]";
      final List<Double> expected = shape.equals("matching")
          ? List.of(10d)
          : List.of();
      for (final boolean filter : List.of(false, true)) {
        final String text = point(source, "1", filter);
        assertEquals(expected, values(new Query(generic, text).execute(context)));
        assertEquals(expected, values(new Query(chain, text).execute(context)));
      }
    }
  }

  private void create(final String json, final String path, final VersioningType versioning) {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("case")));
    try (var database = Databases.openJsonDatabase(directory.resolve("case"))) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .customCommitTimestamps(true)
                                                   .useDeweyIDs(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse(path, PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
  }

  private static String point(final String source, final String probe, final boolean filter) {
    return filter
        ? source + "[?$$.id eq " + probe + "].value"
        : "for $c in " + source + " where $c.id eq " + probe + " return $c.value";
  }

  private static List<Double> values(final Sequence sequence) {
    final List<Double> values = new ArrayList<>();
    if (sequence != null) {
      try (var iter = sequence.iterate()) {
        Item item;
        while ((item = iter.next()) != null) {
          values.add(((Numeric) item).doubleValue());
        }
      }
    }
    return values;
  }
}
