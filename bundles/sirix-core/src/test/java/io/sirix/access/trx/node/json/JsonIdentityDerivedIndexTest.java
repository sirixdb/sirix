package io.sirix.access.trx.node.json;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonIdentityDelta;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertPaths;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Isolated
final class JsonIdentityDerivedIndexTest {
  private static final String INITIAL = """
      [{"score":10,"dept":"A","validFrom":"2020-01-01T00:00:00Z","validTo":"2020-12-31T23:59:59Z"},
       {"score":20,"dept":"B","validFrom":"2021-01-01T00:00:00Z","validTo":"2021-12-31T23:59:59Z"}]
      """;

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    JsonNodeTrxImpl.replayTestHook = null;
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  private static IndexDef projection() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/score", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
  }

  private static Set<IndexDef> definitions() {
    return Set.of(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON),
        IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON),
        IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.JSON), projection(),
        IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/validFrom", PathParser.Type.JSON), parse("/[]/validTo", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void coldDerivedIndexesMatchIndependentDocumentQueries(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    try (final var database = create(sourcePath, versioning, hash, dewey);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(INITIAL), JsonNodeTrx.Commit.NO);
      source.getWtxIndexController(writer.getRevisionNumber()).createIndexes(definitions(), writer);
      writer.commit();
      writer.moveTo(1);
      writer.moveToFirstChild();
      final long first = writer.getNodeKey();
      writer.moveToRightSibling();
      final long second = writer.getNodeKey();
      writer.moveTo(field(writer, first, "score"));
      writer.setNumberValue(11);
      writer.moveTo(field(writer, first, "dept"));
      writer.setStringValue("C");
      writer.moveTo(field(writer, first, "validFrom"));
      writer.setStringValue("2022-01-01T00:00:00Z");
      writer.moveTo(field(writer, first, "validTo"));
      writer.setStringValue("2022-12-31T23:59:59Z");
      writer.moveTo(1);
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      writer.moveTo(1);
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader(
          "{\"score\":30,\"dept\":\"C\",\"validFrom\":\"2023-01-01T00:00:00Z\",\"validTo\":\"2023-12-31T23:59:59Z\"}"),
          JsonNodeTrx.Commit.NO);
      writer.commit();
      writer.moveTo(first);
      writer.remove();
      writer.moveTo(field(writer, second, "dept"));
      writer.setObjectKeyName("team");
      writer.commit();
      writer.revertTo(1);
      writer.commit();
      writer.commit();
    }
    for (final int start : new int[] {1, 3}) {
      final Path targetPath = directory.resolve("target-" + start);
      clearCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = create(targetPath, versioning, hash, dewey);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource");
          final var writer = target.beginNodeTrx()) {
        final var primitiveDefinitions = new HashSet<>(definitions());
        primitiveDefinitions.remove(projection());
        final var controller = (JsonIndexController) target.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(primitiveDefinitions, writer);
        controller.createProjectionIndexesAtLoadStart(Set.of(projection()), writer);
        for (int revision = start; revision <= source.getMostRecentRevisionNumber(); revision++) {
          try (final var reader = source.beginNodeReadOnlyTrx(revision)) {
            final JsonIdentityDelta delta;
            if (revision == start) {
              delta = JsonIdentityDeltaReader.snapshot(reader, 1);
            } else {
              try (final var base = source.beginNodeReadOnlyTrx(revision - 1)) {
                delta = JsonIdentityDeltaReader.between(base, reader, revision - start + 1);
              }
            }
            if (revision <= start + 1) {
              for (final String faultPhase : List.of("identities-staged", "links-installed", "derived-state-finalized",
                  "before-publish")) {
                JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
                  if (phase.equals(faultPhase)) {
                    throw new IllegalStateException("indexed import fault at " + faultPhase);
                  }
                };
                assertThrows(IllegalStateException.class,
                    () -> ((InternalJsonNodeTrx) writer).importRevision(delta, reader));
                assertEquals(revision - start, target.getMostRecentRevisionNumber());
                assertEquals(definitions(),
                    target.getWtxIndexController(writer.getRevisionNumber()).getIndexes().getIndexDefs(),
                    "failed import preserves logical declarations at " + faultPhase);
                JsonNodeTrxImpl.replayTestHook = null;
              }
            }
            ((InternalJsonNodeTrx) writer).importRevision(delta, reader);
          }
        }
      }
      clearCaches();
      try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
          final var targetDb = Databases.openJsonDatabase(targetPath);
          final var source = sourceDb.beginResourceSession("resource");
          final var target = targetDb.beginResourceSession("resource")) {
        for (int revision = start; revision <= source.getMostRecentRevisionNumber(); revision++) {
          try (final var original = source.beginNodeReadOnlyTrx(revision);
              final var copied = target.beginNodeReadOnlyTrx(revision - start + 1)) {
            assertSnapshot(original, copied, start - 1);
            assertIndexes(source, original);
            assertIndexes(target, copied);
          }
          assertPaths(source, revision, target, revision - start + 1);
        }
      }
    }
  }

  private static void assertIndexes(final JsonResourceSession session, final JsonNodeReadOnlyTrx reader) {
    final var controller = session.getRtxIndexController(reader.getRevisionNumber());
    final var names = new LongOpenHashSet();
    final var scores = new LongOpenHashSet();
    final var values = new LongOpenHashSet();
    reader.moveToDocumentRoot();
    final var axis = new DescendantAxis(reader);
    while (axis.hasNext()) {
      axis.nextLong();
      if (reader.getKind().playsObjectKeyRole()) {
        final String name = reader.getName().getLocalName();
        if (name.equals("dept")) {
          names.add(reader.getNodeKey());
          if (reader.getKind() == NodeKind.OBJECT_NAMED_STRING && reader.getValue().equals("C")) {
            values.add(reader.getNodeKey());
          }
        } else if (name.equals("score")) {
          scores.add(reader.getNodeKey());
        }
      }
    }
    assertEquals(names,
        collect(controller.openNameIndex(reader.getStorageEngineReader(),
            controller.getIndexes()
                      .getIndexDef(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID(), IndexType.NAME),
            controller.createNameFilter(Set.of("dept")))));
    assertEquals(scores,
        collect(controller.openPathIndex(reader.getStorageEngineReader(),
            controller.getIndexes().getIndexDef(0, IndexType.PATH),
            controller.createPathFilter(Set.of("/[]/score"), reader))));
    assertEquals(values, collect(controller.openCASIndex(reader.getStorageEngineReader(),
        controller.getIndexes().getIndexDef(0, IndexType.CAS),
        controller.createCASFilter(Set.of("/[]/dept"), new Str("C"), SearchMode.EQUAL, new JsonPCRCollector(reader)))));

    final var keys = new LongArrayList();
    final var numbers = new LongArrayList();
    final List<Instant> starts = new ArrayList<>();
    final List<Instant> ends = new ArrayList<>();
    reader.moveTo(1);
    if (reader.moveToFirstChild()) {
      do {
        final long key = reader.getNodeKey();
        keys.add(key);
        reader.moveTo(field(reader, key, "score"));
        numbers.add(reader.getNumberValue().longValue());
        reader.moveTo(field(reader, key, "validFrom"));
        starts.add(Instant.parse(reader.getValue()));
        reader.moveTo(field(reader, key, "validTo"));
        ends.add(Instant.parse(reader.getValue()));
        reader.moveTo(key);
      } while (reader.moveToRightSibling());
    }
    final var handle = ProjectionIndexCatalog.load(session, reader.getRevisionNumber(), projection());
    assertNotNull(handle);
    final var projectedKeys = new LongArrayList();
    final var projectedNumbers = new LongArrayList();
    for (final byte[] payload : handle.rowGroupPayloads(
        ProjectionIndexCatalog.rowGroupMaterializer(session, reader.getRevisionNumber(), 0, handle.rowGroupCount()))) {
      final var page = ProjectionIndexRowGroupPage.deserialize(payload);
      for (int row = 0; row < page.getRowCount(); row++) {
        projectedKeys.add(page.recordKeys()[row]);
        projectedNumbers.add(page.numericColumn(0)[row]);
      }
    }
    assertEquals(keys, projectedKeys, "projection record identities in document order");
    assertEquals(numbers, projectedNumbers, "projection scalar payloads");
    final var domain = new IntervalDomain();
    final var intervals = ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain);
    for (int year = 2020; year <= 2023; year++) {
      final Instant point = Instant.parse(year + "-06-01T00:00:00Z");
      final var expected = new LongOpenHashSet();
      for (int row = 0; row < keys.size(); row++) {
        if (!point.isBefore(starts.get(row)) && !point.isAfter(ends.get(row))) {
          expected.add(keys.getLong(row));
        }
      }
      final var actual = new LongOpenHashSet();
      intervals.stab(domain.point(point), actual::add);
      assertEquals(expected, actual, "valid-time revision " + reader.getRevisionNumber() + " point " + point);
    }
  }

  private static LongSet collect(final Iterator<NodeReferences> references) {
    final var result = new LongOpenHashSet();
    while (references.hasNext()) {
      final var keys = references.next().getNodeKeys().getLongIterator();
      while (keys.hasNext()) {
        result.add(keys.next());
      }
    }
    return result;
  }

  private static long field(final JsonNodeReadOnlyTrx reader, final long parent, final String name) {
    assertTrue(reader.moveTo(parent));
    assertTrue(reader.moveToFirstChild());
    do {
      if (reader.getKind().playsObjectKeyRole() && name.equals(reader.getName().getLocalName())) {
        return reader.getNodeKey();
      }
    } while (reader.moveToRightSibling());
    throw new AssertionError("Missing field " + name + " under " + parent);
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType versioning,
      final HashType hash, final boolean dewey) {
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder("resource")
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .versioningApproach(versioning)
                                                 .hashKind(hash)
                                                 .useDeweyIDs(dewey)
                                                 .buildPathStatistics(true)
                                                 .validTimePaths("validFrom", "validTo")
                                                 .build());
    return database;
  }
}
