package io.sirix.index.interval.json;

import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.index.interval.json.ValidTimeMoveTestSupport.Keys;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeContainerMoveTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"vf,first", "vf,right", "vf,left", "vt,first", "vt,right", "vt,left"})
  void containerMovesPublishActualIntervalsAndMembership(final String duplicate, final String mode) {
    final Path path = directory.resolve("moves");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final Keys keys;
    try (var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeDiffs(false)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(ValidTimeMoveTestSupport.rows(duplicate)),
            JsonNodeTrx.Commit.NO);
        keys = ValidTimeMoveTestSupport.keys(writer);
        final IndexDef definition = IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        writer.commit();
        assertEvidence(session, 1, keys, 0);
        for (int step = 1; step <= 5; step++) {
          ValidTimeMoveTestSupport.mutate(writer, keys, mode, step);
          writer.commit();
          assertEvidence(session, step + 1, keys, step);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(path); var session = database.beginResourceSession("rows")) {
      for (int step = 0; step <= 5; step++) {
        assertEvidence(session, step + 1, keys, step);
      }
    }
  }

  private static void assertEvidence(final JsonResourceSession session, final int revision, final Keys keys,
      final int step) {
    try (var reader = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(!session.getRtxIndexController(revision).getIndexes().getIndexDefs().isEmpty());
      final long third = step >= 3
          ? ValidTimeMoveTestSupport.objectKey(reader, 3)
          : -1;
      final long fourth = step >= 5
          ? ValidTimeMoveTestSupport.objectKey(reader, 4)
          : -1;
      final long[] records = step >= 5
          ? new long[] {keys.record(), keys.nested(), third, fourth}
          : step >= 3
              ? new long[] {keys.record(), keys.nested(), third}
              : new long[] {keys.record(), keys.nested()};
      final var domain = new IntervalDomain();
      final var tree = ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain);
      final LongArrayList registrations = new LongArrayList();
      tree.forEachRef(registrations::add);
      assertEquals(records.length, registrations.size());
      assertEquals(new LongOpenHashSet(records), new LongOpenHashSet(registrations));
      for (final Instant point : new Instant[] {Instant.parse("2024-01-01T00:00:00Z"),
          Instant.parse("1900-01-01T00:00:00Z"), Instant.parse("3000-01-01T00:00:00Z")}) {
        final LongArrayList hits = new LongArrayList();
        tree.stab(domain.point(point), hits::add);
        final long[] expected = point.equals(Instant.parse("2024-01-01T00:00:00Z"))
            ? records
            : new long[] {keys.record()};
        assertEquals(expected.length, hits.size());
        assertEquals(new LongOpenHashSet(expected), new LongOpenHashSet(hits));
      }
      final LongArrayList unverified = new LongArrayList();
      ValidTimeIntervalIndexFactory.createVerificationStore(reader.getStorageEngineReader(), 0)
                                   .scan(0, 0, 0, unverified::add);
      assertEquals(LongArrayList.of(keys.record()), unverified);
      assertMembers(reader, keys.root(), step == 1
          ? new long[0]
          : step >= 3
              ? new long[] {keys.record(), third}
              : new long[] {keys.record()});
      assertMembers(reader, keys.destination(), step == 1
          ? new long[] {keys.record()}
          : step >= 5
              ? new long[] {fourth}
              : new long[0]);
      assertMembers(reader, keys.record(), step >= 4
          ? new long[0]
          : new long[] {keys.nested()});
      assertMembers(reader, keys.wrapper(), new long[0]);
      if (step >= 4) {
        assertMembers(reader, ValidTimeMoveTestSupport.ownerKey(reader, keys.wrapper()), new long[] {keys.nested()});
      }
      final LongArrayList allMembers = new LongArrayList();
      ValidTimeIntervalIndexFactory.createMembershipStore(reader.getStorageEngineReader(), 0)
                                   .forEachRef(allMembers::add);
      assertEquals(records.length, allMembers.size());
      assertEquals(new LongOpenHashSet(records), new LongOpenHashSet(allMembers));
      for (final long record : records) {
        assertTrue(reader.moveTo(record));
        assertTrue(new LongOpenHashSet(members(reader, reader.getParentKey())).contains(record));
      }
    }
  }

  private static void assertMembers(final JsonNodeReadOnlyTrx reader, final long parent, final long[] expected) {
    final long[] actual = members(reader, parent);
    Arrays.sort(actual);
    final long[] sorted = expected.clone();
    Arrays.sort(sorted);
    assertEquals(LongArrayList.wrap(sorted), LongArrayList.wrap(actual),
        "parent " + parent + " revision " + reader.getRevisionNumber());
  }

  private static long[] members(final JsonNodeReadOnlyTrx reader, final long parent) {
    final LongArrayList members = new LongArrayList();
    ValidTimeIntervalIndexFactory.createMembershipStore(reader.getStorageEngineReader(), 0)
                                 .scan(parent, 0, 0, members::add);
    return members.toLongArray();
  }
}
