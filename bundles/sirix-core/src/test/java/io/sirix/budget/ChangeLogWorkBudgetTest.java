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
import io.sirix.index.aggregate.AggregateSummary;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.aggregate.TemporalAggregateDomain;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.aggregate.TemporalAggregateParts;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.index.projection.ProjectionChangeLog;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Slab writes depend on changed rows only; old cells survive barriers, reopen, and reverted bases.
 */
@Isolated
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
        assertArrayEquals(new String[0], (String[]) Objects.requireNonNull(slab.entries().getFirst().after()).text[4]);
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
            assertEquals(i, Objects.requireNonNull(entry.before()).numbers[1]);
            assertEquals(revision == 3
                ? 800
                : i + 100, Objects.requireNonNull(entry.after()).numbers[1]);
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
          assertEquals(ChangeLogSlab.NON_INTEGRAL, Objects.requireNonNull(log.provenance(record))[2]);
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
        assertEquals(4, Objects.requireNonNull(slab.entries().getFirst().before()).numbers[1]);
        assertEquals(ChangeLogSlab.PRESENT, Objects.requireNonNull(slab.entries().getFirst().before()).flags[1]);
        assertEquals(6, Objects.requireNonNull(slab.entries().getFirst().after()).numbers[1]);
        for (int revision = 4; revision <= 5; revision++) {
          final int uncovered = revision;
          assertThrows(NullPointerException.class,
              () -> ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, uncovered));
        }
        // Resumption does not erase the previously covered slab or alter the older sparse flags.
        assertTrue(ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 3).entries().isEmpty());
        writer.revertTo(3);
        try (ChangeLogStorage log = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
          assertEquals(ChangeLogSlab.NON_INTEGRAL, Objects.requireNonNull(log.provenance(record))[2]);
        }
        writer.rollback();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {256, 4096})
  void unchangedWriterReadsSkipSlabWorkAndAggregateReplay(final int rows) {
    final var path = directory.toPath().resolve("unchanged-barrier");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("data")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("data");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 20).append('[');
        for (int row = 0; row < rows; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"n\":").append(row).append('}');
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(field("g"), field("n"), field("lo"), field("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR),
            0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(
            session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]", List.of("g"), List.of("sum(n)"));
        writer.commit();
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        for (int row = 0; row < rows; row++) {
          final long record = writer.getNodeKey();
          set(writer, record, row + 100);
          writer.moveTo(record);
          writer.moveToRightSibling();
        }
        final var controller = session.getWtxIndexController(writer.getRevisionNumber());
        final long before = ProjectionChangeLog.slabsBuilt();
        final var handle =
            controller.openProjectionIndex(writer.getStorageEngineWriter(), new String[] {"[]"}, new String[] {"n"});
        assertNotNull(handle);
        assertEquals(before + 1, ProjectionChangeLog.slabsBuilt());
        final ChangeLogStorage.WriteWork logWork = ChangeLogStorage.lastWrite();
        final TemporalAggregateIndexes.MaintenanceWork aggregateWork = TemporalAggregateIndexes.lastMaintenance();
        assertEquals(rows, logWork.records());
        assertTrue(logWork.slots() > 0);
        assertEquals(rows, aggregateWork.changedRecords());
        assertTrue(aggregateWork.nodesWritten() > 0);
        for (int read = 0; read < 4; read++) {
          assertSame(handle,
              controller.openProjectionIndex(writer.getStorageEngineWriter(), new String[] {"[]"}, new String[] {"n"}));
          assertEquals(before + 1, ProjectionChangeLog.slabsBuilt(), "Unchanged reads must sort/build zero slabs");
          assertSame(logWork, ChangeLogStorage.lastWrite(), "Unchanged reads must encode/write zero slabs");
          assertSame(aggregateWork, TemporalAggregateIndexes.lastMaintenance(),
              "Unchanged reads must replay zero aggregate batches");
        }
        set(writer, first, 999);
        assertNotNull(
            controller.openProjectionIndex(writer.getStorageEngineWriter(), new String[] {"[]"}, new String[] {"n"}));
        assertEquals(before + 2, ProjectionChangeLog.slabsBuilt());
        final ChangeLogSlab slab = ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 2);
        assertEquals(rows, slab.entries().size());
        assertEquals(0, Objects.requireNonNull(slab.entries().getFirst().before()).numbers[1]);
        assertEquals(999, Objects.requireNonNull(slab.entries().getFirst().after()).numbers[1]);
        try (var storage = new TemporalAggregateStorage.Reader(writer.getStorageEngineWriter(), aggregate.getID())) {
          final AggregateSummary[] summary = new AggregateSummary[1];
          storage.groups(Long.MAX_VALUE,
              (key, head) -> summary[0] = new TemporalAggregateParts(aggregate.getTemporalAggregateSpec(),
                  new TemporalAggregateDomain().height()).point(storage.parts(), head.parts(), 1));
          assertNotNull(summary[0]);
          assertEquals(rows, summary[0].count());
          assertEquals(BigInteger.valueOf((long) rows * (rows - 1) / 2 + rows * 100L + 899), summary[0].sum(0));
        }
        writer.commit();
        assertEquals(before + 2, ProjectionChangeLog.slabsBuilt());
        writer.commit();
        assertEquals(before + 3, ProjectionChangeLog.slabsBuilt(),
            "A new covered revision must publish its empty slab");
        assertEquals(0, ChangeLogStorage.lastWrite().records());
        assertTrue(ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 3).entries().isEmpty());
        assertEquals(2, ChangeLogStorage.read(writer.getStorageEngineWriter(), 0, 3).baseRevision());
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
