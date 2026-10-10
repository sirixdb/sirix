package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Slab writes depend on changed rows only; old cells survive barriers, reopen, and reverted bases.
 */
final class ChangeLogWorkBudgetTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @CsvSource({"256,FULL", "4096,FULL", "256,DIFFERENTIAL", "4096,DIFFERENTIAL", "256,INCREMENTAL", "4096,INCREMENTAL",
      "256,SLIDING_SNAPSHOT", "4096,SLIDING_SNAPSHOT"})
  void changesAreBoundedAndReplayable(final int rows, final VersioningType versioning) {
    final var path = directory.toPath().resolve("log");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final long[] records = new long[20];
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("data");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 20).append('[');
        for (int i = 0; i < rows; i++) {
          if (i != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"tags\":[],\"n\":").append(i).append('}');
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(field("g"), field("n"), field("lo"), field("hi"), field("tags/[]")),
            List.of(Type.LON, Type.LON, Type.STR, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("sum(n)"));
        writer.commit();
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        for (int i = 0; i < records.length; i++) {
          records[i] = writer.getNodeKey();
          set(writer, records[i], i + 100);
          session.getWtxIndexController(writer.getRevisionNumber()).notifyBeforePageFlush();
          writer.moveTo(records[i]);
          writer.moveToRightSibling();
        }
        writer.commit();
        final ChangeLogStorage.WriteWork work = ChangeLogStorage.lastWrite();
        assertEquals(20, work.records());
        assertTrue(work.bytes() <= 2048, "Twenty numeric row changes must fit one slab chunk: " + work);
        assertEquals((work.bytes() + 2047) / 2048 + 1, work.slots(), "Exactly chunks plus one directory");
        writer.revertTo(1);
        set(writer, records[0], 700);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        set(writer, records[0], 800);
        writer.commit();
        writer.commit(); // A no-op revision is covered by an empty slab with an explicit base.
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"g\":1,\"n\":900,\"tags\":[]}"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("data")) {
      try (var reader = session.beginNodeReadOnlyTrx(5)) {
        final ChangeLogSlab slab = ChangeLogStorage.read(reader.getStorageEngineReader(), 0, 5);
        assertEquals(1, slab.entries().size());
        assertNull(slab.entries().getFirst().before());
        assertArrayEquals(new String[0], (String[]) slab.entries().getFirst().after().text[4]);
      }
      for (int revision = 1; revision <= 4; revision++) {
        try (var reader = session.beginNodeReadOnlyTrx(revision)) {
          final ChangeLogSlab slab = ChangeLogStorage.read(reader.getStorageEngineReader(), 0, revision);
          assertEquals(revision == 3
              ? 1
              : revision - 1, slab.baseRevision());
          assertEquals(revision == 2
              ? 20
              : revision == 3
                  ? 1
                  : 0,
              slab.entries().size());
          for (int i = 0; i < slab.entries().size(); i++) {
            final ChangeLogSlab.Entry entry = slab.entries().get(i);
            assertEquals(records[i], entry.record());
            assertEquals(i, entry.before().numbers[1]);
            assertEquals(revision == 3
                ? 800
                : i + 100, entry.after().numbers[1]);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void noLogWorkWithoutConsumerAndFreshProvenanceAfterRedeclaration(final VersioningType versioning) {
    final var path = directory.toPath().resolve("optional-log");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("data");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1.5},{\"g\":1,\"n\":2.5}]"),
            JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long record = writer.getNodeKey();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(field("g"), field("n"), field("lo"), field("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR),
            0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        writer.commit();
        // Neither no-op commits nor incremental column maintenance may initialize the container.
        set(writer, record, 2);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        writer.commit();
        assertTrue(writer.getStorageEngineWriter()
                         .getActualRevisionRootPage()
                         .getChangeLogIndexPageReference()
                         .isVirginStructuralPlaceholder());
        writer.moveTo(record);
        writer.moveToFirstChild();
        writer.moveToRightSibling();
        writer.moveToFirstChild();
        writer.setNumberValue(1.5);
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(
            session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]", List.of("g"), List.of("sum(n)"));
        writer.commit(); // Revision 3 has an exceptional numeric row.
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertEquals(ChangeLogSlab.NON_INTEGRAL, log.provenance(record)[2]);
        }
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(aggregate), writer);
        set(writer, record, 4);
        writer.commit();
        writer.commit(); // An uncovered no-op revision must not acquire a slab either.
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("sum(n)"));
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertNull(log.provenance(record), "Dropped-period numeric change must remove stale exceptional flags");
        }
        set(writer, record, 6);
        writer.commit();
        final ChangeLogSlab slab = ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 6);
        assertEquals(1, slab.entries().size());
        assertEquals(4, slab.entries().getFirst().before().numbers[1]);
        assertEquals(ChangeLogSlab.PRESENT, slab.entries().getFirst().before().flags[1]);
        assertEquals(6, slab.entries().getFirst().after().numbers[1]);
        for (int revision = 4; revision <= 5; revision++) {
          final int uncovered = revision;
          assertThrows(NullPointerException.class,
              () -> ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, uncovered));
        }
        // Resumption does not erase the previously covered slab or alter the older sparse flags.
        assertTrue(ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 3).entries().isEmpty());
        writer.revertTo(3);
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertEquals(ChangeLogSlab.NON_INTEGRAL, log.provenance(record)[2]);
        }
        writer.rollback();
      }
    }
  }

  private static void set(final JsonNodeTrx writer, final long record, final int value) {
    writer.moveTo(record);
    writer.moveToFirstChild();
    while (!"n".equals(writer.getName().getLocalName())) {
      writer.moveToRightSibling();
    }
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static Path<QNm> field(final String name) {
    return Path.parse("/[]/" + name, PathParser.Type.JSON);
  }
}
