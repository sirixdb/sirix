package io.sirix.budget;

import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.io.StorageType;
import io.sirix.service.json.replay.JsonIdentityDeltaReader;
import io.sirix.service.json.replay.JsonReplayGraphValidator;
import io.sirix.service.json.replay.JsonReplaySnapshotOracle;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static io.sirix.budget.EngineWorkCounters.REPLAY;
import static io.sirix.budget.EngineWorkCounters.REPLAY_VALID_TIME_BOUND_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class JsonValidTimeIdentityWorkBudgetTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(version -> Stream.of(16, 4096).map(width -> Arguments.of(version, width)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void unrelatedEditsDoNotScanWideBounds(final VersioningType version, final int width) throws Exception {
    final Path sourcePath = directory.resolve("source");
    final long object;
    try (final var database = create(sourcePath, version);
        final var source = database.beginResourceSession("resource");
        final var writer = source.beginNodeTrx()) {
      final StringBuilder json = new StringBuilder(width * 16).append(
          "[{\"validFrom\":\"2020-01-01T00:00:00Z\",\"validTo\":\"2025-01-01T00:00:00Z\",\"values\":[0]");
      for (int field = 0; field < width; field++) {
        json.append(",\"f").append(field).append("\":0");
      }
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append("}]").toString()),
          JsonNodeTrx.Commit.NO);
      assertTrue(writer.moveTo(1));
      assertTrue(writer.moveToFirstChild());
      object = writer.getNodeKey();
      final long values = field(writer, object, "values");
      final long unrelated = field(writer, object, "f0");
      final long to = field(writer, object, "validTo");
      writer.commit();
      assertTrue(writer.moveTo(values));
      writer.insertNumberValueAsLastChild(1);
      writer.commit();
      assertTrue(writer.moveTo(unrelated));
      writer.setNumberValue(1);
      writer.commit();
      assertTrue(writer.moveTo(object));
      writer.insertObjectRecordAsFirstChild("noise", new StringValue("ignored"));
      final long noise = writer.getNodeKey();
      writer.commit();
      assertTrue(writer.moveTo(noise));
      writer.remove();
      writer.commit();
      assertTrue(writer.moveTo(to));
      writer.setStringValue("2024-01-01T00:00:00Z");
      writer.commit();
    }
    clearCaches();
    final Path targetPath = directory.resolve("target");
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = create(targetPath, version);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource");
        final var writer = target.beginNodeTrx()) {
      target.getWtxIndexController(1).createIndexes(Set.of(definition()), writer);
      final var importer = (InternalJsonNodeTrx) writer;
      try (final var first = source.beginNodeReadOnlyTrx(1)) {
        importer.importRevision(JsonIdentityDeltaReader.snapshot(first, 1), first);
      }
      for (int revision = 2; revision <= 6; revision++) {
        try (final var before = source.beginNodeReadOnlyTrx(revision - 1);
            final var after = source.beginNodeReadOnlyTrx(revision)) {
          final var delta = JsonIdentityDeltaReader.between(before, after, revision);
          final var work = WorkCapture.of(REPLAY).run(() -> importer.importRevision(delta, after));
          if (revision < 6) {
            work.assertZero(REPLAY_VALID_TIME_BOUND_FIELDS,
                "nested, scalar and boundary-only non-bound edits must not scan retained object fields");
          } else {
            work.assertExactly(REPLAY_VALID_TIME_BOUND_FIELDS, 2L * (width + 3),
                "a direct bound update must inspect the old and final fields once, proving the counter is live");
          }
        }
      }
    }
    clearCaches();
    try (final var sourceDb = Databases.openJsonDatabase(sourcePath);
        final var targetDb = Databases.openJsonDatabase(targetPath);
        final var source = sourceDb.beginResourceSession("resource");
        final var target = targetDb.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 6; revision++) {
        try (final var original = source.beginNodeReadOnlyTrx(revision);
            final var copied = target.beginNodeReadOnlyTrx(revision)) {
          JsonReplayGraphValidator.validate(copied);
          final var expected = JsonReplaySnapshotOracle.snapshot(original);
          final var actual = JsonReplaySnapshotOracle.snapshot(copied);
          expected.remove(0);
          actual.remove(0);
          assertEquals(expected, actual);
          assertEquals(original.getMaxNodeKey(), copied.getMaxNodeKey());
          final var domain = new IntervalDomain();
          final var tree = ValidTimeIntervalIndexFactory.createReaderTree(copied.getStorageEngineReader(), 0, domain);
          final LongOpenHashSet hits = new LongOpenHashSet();
          tree.stab(domain.point(Instant.parse("2024-06-01T00:00:00Z")), hits::add);
          assertEquals(revision < 6
              ? LongOpenHashSet.of(object)
              : new LongOpenHashSet(), hits);
          hits.clear();
          tree.stab(domain.point(Instant.parse("2023-06-01T00:00:00Z")), hits::add);
          assertEquals(LongOpenHashSet.of(object), hits);
          final LongOpenHashSet unverified = new LongOpenHashSet();
          ValidTimeIntervalIndexFactory.createVerificationStore(copied.getStorageEngineReader(), 0)
                                       .scan(0, 0, 0, unverified::add);
          assertTrue(unverified.isEmpty(), "unique canonical bounds must retain exactness");
          final LongOpenHashSet members = new LongOpenHashSet();
          ValidTimeIntervalIndexFactory.createMembershipStore(copied.getStorageEngineReader(), 0)
                                       .scan(1, 0, 0, members::add);
          assertEquals(LongOpenHashSet.of(object), members);
        }
      }
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
    return IndexDefs.createValidTimeIdxDef(Set.of(parse("/[]/validFrom", PathParser.Type.JSON),
        parse("/[]/validTo", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
  }

  private static Database<JsonResourceSession> create(final Path path, final VersioningType version) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final var database = Databases.openJsonDatabase(path);
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                          .storageType(StorageType.FILE_CHANNEL)
                                                          .versioningApproach(version)
                                                          .hashKind(HashType.ROLLING)
                                                          .useDeweyIDs(false)
                                                          .buildPathStatistics(true)
                                                          .validTimePaths("validFrom", "validTo")
                                                          .build()));
    return database;
  }
}
