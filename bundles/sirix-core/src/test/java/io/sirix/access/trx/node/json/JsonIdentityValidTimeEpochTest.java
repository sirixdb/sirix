package io.sirix.access.trx.node.json;

import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.index.interval.json.ValidTimeMoveTestSupport;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.jspecify.annotations.Nullable;

import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertPaths;
import static io.sirix.access.trx.node.json.JsonIdentityImportTest.assertSnapshot;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonIdentityValidTimeEpochTest {
  private static final List<Instant> POINTS = List.of(Instant.parse("1900-01-01T00:00:00Z"),
      Instant.parse("2024-06-01T00:00:00Z"), Instant.parse("3000-01-01T00:00:00Z"));

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    JsonNodeTrxImpl.replayTestHook = null;
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void directBoundsTopologyAndRetryMatchColdDocumentEvidence(final VersioningType version, final HashType hash,
      final boolean dewey) {
    final Path sourcePath = directory.resolve("source");
    final Path targetPath = directory.resolve("target");
    try (final var database = create(sourcePath, version, hash, dewey);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
          {"left":[{"id":1,"validFrom":"2020-01-01T00:00:00Z","validFrom":"2022-01-01T00:00:00Z",
                    "validTo":"2025-01-01T00:00:00Z"},
                   {"id":2,"validFrom":"2020-01-01T00:00:00Z","validTo":"2025-01-01T00:00:00Z"}],
           "right":[],"named":{"id":3,"validFrom":"2020-01-01T00:00:00Z","validTo":"2025-01-01T00:00:00Z"}}
          """), JsonNodeTrx.Commit.NO);
      source.getWtxIndexController(1).createIndexes(Set.of(definition()), writer);
      writer.commit();
      final long first = ValidTimeMoveTestSupport.objectKey(writer, 1);
      final long second = ValidTimeMoveTestSupport.objectKey(writer, 2);
      final long named = ValidTimeMoveTestSupport.objectKey(writer, 3);
      final long left = field(writer, 1, "left");
      final long right = field(writer, 1, "right");
      assertTrue(writer.moveTo(field(writer, first, "validTo")));
      writer.setStringValue("2026-01-01T00:00:00Z");
      writer.commit();
      assertTrue(writer.moveTo(field(writer, first, "validFrom")));
      writer.remove();
      writer.commit();
      assertTrue(writer.moveTo(first));
      writer.insertObjectRecordAsFirstChild("validFrom", new StringValue("2019-01-01T00:00:00Z"));
      final long duplicate = writer.getNodeKey();
      writer.commit();
      assertTrue(writer.moveTo(first));
      assertTrue(writer.moveToLastChild());
      writer.moveSubtreeToRightSibling(duplicate);
      writer.commit();
      final long from = field(writer, first, "validFrom");
      assertTrue(writer.moveTo(from));
      writer.setObjectKeyName("archived");
      writer.commit();
      assertTrue(writer.moveTo(from));
      writer.setObjectKeyName("validFrom");
      writer.commit();
      assertTrue(writer.moveTo(duplicate));
      writer.remove();
      writer.commit();
      assertTrue(writer.moveTo(from));
      writer.setStringValue("malformed");
      writer.commit();
      assertTrue(writer.moveTo(from));
      writer.replaceObjectRecordValue(new NumberValue(7));
      writer.commit();
      assertTrue(writer.moveTo(field(writer, first, "validFrom")));
      writer.replaceObjectRecordValue(new StringValue("2022-01-01T00:00:00Z"));
      final long restoredFrom = field(writer, first, "validFrom");
      writer.commit();
      assertTrue(writer.moveTo(field(writer, first, "validTo")));
      writer.remove();
      writer.commit();
      assertTrue(writer.moveTo(first));
      writer.insertObjectRecordAsLastChild("validTo", new StringValue("2025-01-01T00:00:00Z"));
      writer.commit();
      assertTrue(writer.moveTo(right));
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      assertTrue(writer.moveTo(left));
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      assertTrue(writer.moveTo(1));
      writer.moveSubtreeToFirstChild(named);
      writer.commit();
      assertTrue(writer.moveTo(first));
      writer.moveSubtreeToFirstChild(named);
      writer.commit();
      assertTrue(writer.moveTo(second));
      writer.moveSubtreeToFirstChild(restoredFrom);
      writer.commit();
      assertTrue(writer.moveTo(first));
      writer.moveSubtreeToFirstChild(restoredFrom);
      writer.commit();
      assertTrue(writer.moveTo(named));
      writer.remove();
      writer.commit();
      writer.revertTo(1);
      writer.commit();
      assertEquals(21, source.getMostRecentRevisionNumber());
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, version, hash, dewey);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      target.getWtxIndexController(1).createIndexes(Set.of(definition()), writer);
      final var importer = (InternalJsonNodeTrx) writer;
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
      }
      for (int revision = 2; revision <= 21; revision++) {
        try (final var before = source.beginNodeReadOnlyTrx(revision - 1);
            final var after = source.beginNodeReadOnlyTrx(revision)) {
          final var delta = JsonIdentityDeltaReader.between(before, after, revision);
          JsonNodeTrxImpl.replayTestHook = (phase, transaction) -> {
            if (phase.equals("derived-state-finalized")) {
              throw new IllegalStateException("injected valid-time failure");
            }
          };
          assertEquals("injected valid-time failure",
              assertThrows(IllegalStateException.class, () -> importer.importRevision(delta, after)).getMessage());
          JsonNodeTrxImpl.replayTestHook = null;
          assertEquals(revision - 1, target.getMostRecentRevisionNumber());
          assertSnapshot(before, writer, 0);
          try (final var committed = target.beginNodeReadOnlyTrx(revision - 1)) {
            assertEvidence(committed);
          }
          importer.importRevision(delta, after);
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 21; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          assertSnapshot(original, copied, 0);
          assertEvidence(original);
          assertEvidence(copied);
          if (revision == 4 || revision == 5) {
            final long first = ValidTimeMoveTestSupport.objectKey(copied, 1);
            assertTrue(copied.moveTo(field(copied, first, "validFrom")));
            assertEquals(revision == 4
                ? "2019-01-01T00:00:00Z"
                : "2022-01-01T00:00:00Z", copied.getValue(), "duplicate selection follows final document order");
          }
        }
        assertPaths(source, revision, target, revision);
      }
    }
  }

  private static void assertEvidence(final JsonNodeReadOnlyTrx reader) {
    final LongOpenHashSet expected = new LongOpenHashSet();
    final LongOpenHashSet inexact = new LongOpenHashSet();
    final LongOpenHashSet unorderedParents = new LongOpenHashSet();
    final Long2ObjectOpenHashMap<LongOpenHashSet> members = new Long2ObjectOpenHashMap<>();
    final List<LongOpenHashSet> hits = List.of(new LongOpenHashSet(), new LongOpenHashSet(), new LongOpenHashSet());
    reader.moveToDocumentRoot();
    final var axis = new DescendantAxis(reader);
    while (axis.hasNext()) {
      final long key = axis.nextLong();
      if (reader.getKind() != NodeKind.OBJECT && reader.getKind() != NodeKind.OBJECT_NAMED_OBJECT) {
        continue;
      }
      final long parent = reader.getParentKey();
      final boolean inverted = reader.getLeftSiblingKey() > key
          || reader.getRightSiblingKey() >= 0 && reader.getRightSiblingKey() < key;
      Instant from = null;
      Instant to = null;
      int fromCount = 0;
      int toCount = 0;
      if (reader.moveToFirstChild()) {
        do {
          final String name = reader.getName().getLocalName();
          if (name.equals("validFrom")) {
            fromCount++;
            if (from == null) {
              from = instant(reader);
            }
          } else if (name.equals("validTo")) {
            toCount++;
            if (to == null) {
              to = instant(reader);
            }
          }
        } while (reader.moveToRightSibling());
      }
      assertTrue(reader.moveTo(key));
      final boolean duplicate = fromCount > 1 || toCount > 1;
      if (!duplicate && (from == null && to == null || from != null && to != null && from.isAfter(to))) {
        continue;
      }
      expected.add(key);
      members.computeIfAbsent(parent, ignored -> new LongOpenHashSet()).add(key);
      if (duplicate || from == null || to == null) {
        inexact.add(key);
      }
      if (inverted) {
        unorderedParents.add(parent);
      }
      for (int point = 0; point < POINTS.size(); point++) {
        if (duplicate || (from == null || !POINTS.get(point).isBefore(from))
            && (to == null || !POINTS.get(point).isAfter(to))) {
          hits.get(point).add(key);
        }
      }
    }
    final var domain = new IntervalDomain();
    final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain);
    final LongArrayList registrations = new LongArrayList();
    tree.forEachRef(registrations::add);
    assertEquals(expected.size(), registrations.size());
    assertEquals(expected, new LongOpenHashSet(registrations));
    for (int point = 0; point < POINTS.size(); point++) {
      final LongArrayList actual = new LongArrayList();
      tree.stab(domain.point(POINTS.get(point)), actual::add);
      assertEquals(hits.get(point).size(), actual.size());
      assertEquals(hits.get(point), new LongOpenHashSet(actual));
    }
    final LongOpenHashSet verification = new LongOpenHashSet();
    ValidTimeIntervalIndexFactory.createVerificationStore(reader.getStorageEngineReader(), 0)
                                 .scan(0, 0, 0, verification::add);
    assertEquals(inexact, verification);
    final var membership = ValidTimeIntervalIndexFactory.createMembershipStore(reader.getStorageEngineReader(), 0);
    final LongArrayList allMembers = new LongArrayList();
    membership.forEachRef(allMembers::add);
    assertEquals(expected.size(), allMembers.size());
    assertEquals(expected, new LongOpenHashSet(allMembers));
    for (final var entry : members.long2ObjectEntrySet()) {
      final LongOpenHashSet actual = new LongOpenHashSet();
      membership.scan(entry.getLongKey(), 0, 0, actual::add);
      assertEquals(entry.getValue(), actual);
    }
    final var order = ValidTimeIntervalIndexFactory.createOrderStore(reader.getStorageEngineReader(), 0);
    for (final long parent : unorderedParents) {
      final LongOpenHashSet guards = new LongOpenHashSet();
      order.scan(parent, 0, 0, guards::add);
      assertEquals(LongOpenHashSet.of(0), guards);
    }
  }

  private static @Nullable Instant instant(final JsonNodeReadOnlyTrx reader) {
    if (reader.getKind() != NodeKind.OBJECT_NAMED_STRING) {
      return null;
    }
    try {
      return Instant.parse(reader.getValue());
    } catch (final DateTimeParseException malformed) {
      return null;
    }
  }

  private static long field(final JsonNodeReadOnlyTrx reader, final long object, final String name) {
    assertTrue(reader.moveTo(object));
    assertTrue(reader.moveToFirstChild());
    do {
      if (name.equals(reader.getName().getLocalName())) {
        return reader.getNodeKey();
      }
    } while (reader.moveToRightSibling());
    throw new AssertionError("Missing field " + name);
  }

  private static IndexDef definition() {
    return IndexDefs.createValidTimeIdxDef(Set.of(parse("/left/[]/validFrom", PathParser.Type.JSON),
        parse("/left/[]/validTo", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType version, final HashType hash,
      final boolean dewey) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final var database = Databases.openJsonDatabase(path);
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                          .storageType(StorageType.FILE_CHANNEL)
                                                          .versioningApproach(version)
                                                          .hashKind(hash)
                                                          .useDeweyIDs(dewey)
                                                          .buildPathStatistics(true)
                                                          .validTimePaths("validFrom", "validTo")
                                                          .build()));
    return database;
  }
}
