package io.sirix.index.interval.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.SubtreePrinter;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeIndexRebuildTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "2", "3", "4", "5"})
  void explicitMaintenanceRebuildsObsoleteRootsOnceAndRebindsMaintenance(final String format) throws Exception {
    rebuildAndMaintain(format, false, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "2", "3", "4", "5"})
  void writerRebuildsObsoleteRootsOnCommitAndRebindsMaintenance(final String format) throws Exception {
    rebuildAndMaintain(format, false, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "2", "3", "4", "5"})
  void revertingAfterExplicitUpgradeRebuildsRepresentedCatalogue(final String format) throws Exception {
    rebuildAndMaintain(format, true, false);
  }

  @Test
  void revertingToRevisionWithoutIndexesPersistsEmptyCatalogue() {
    final Path databasePath = directory.resolve("database");
    final long objectKey;
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.commit();
        final int originalRevision = session.getMostRecentRevisionNumber();
        writer.moveToDocumentRoot();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        objectKey = writer.getNodeKey();
        final IndexDef definition = IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        writer.commit();
        writer.revertTo(originalRevision);
        writer.commit();
        assertTrue(
            session.getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes().getIndexDefs().isEmpty());
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      assertTrue(session.getRtxIndexController(3).getIndexes().getIndexDefs().isEmpty());
      assertNotNull(session.getRtxIndexController(2).getIndexes().getIndexDef(0, IndexType.VALIDTIME));
      try (var reader = session.beginNodeReadOnlyTrx(2)) {
        final IntervalDomain domain = new IntervalDomain();
        final LongOpenHashSet matches = new LongOpenHashSet();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain)
                                     .stabHalfOpen(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(objectKey), matches);
      }
      try (var writer = session.beginNodeTrx()) {
        assertTrue(session.getWtxIndexController(writer.getRevisionNumber()).getIndexes().getIndexDefs().isEmpty());
      }
    }
  }

  private void rebuildAndMaintain(final String format, final boolean revert, final boolean upgradeOnCommit)
      throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Path catalogue;
    final int oldRevision;
    final long objectKey;
    final long endKey;
    final long unchangedKey;
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
             {"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        objectKey = writer.getNodeKey();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToRightSibling());
        endKey = writer.getNodeKey();
        assertTrue(writer.moveTo(objectKey));
        assertTrue(writer.moveToRightSibling());
        unchangedKey = writer.getNodeKey();
        final IndexDef definition = IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, domain)
                                     .delete(objectKey, domain.lowerBound(Instant.parse("2023-01-01T00:00:00Z")),
                                         domain.upperBound(Instant.parse("2025-01-01T00:00:00Z")));
        writer.commit();
        oldRevision = session.getMostRecentRevisionNumber();
        catalogue = session.getResourceConfig()
                           .getResource()
                           .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                           .resolve(oldRevision + ".xml");
      }
    }
    final Node<?> persisted;
    try (var input = Files.newInputStream(catalogue)) {
      persisted = IndexController.deserialize(input).getFirstChild();
    }
    final Node<?> definitionNode = persisted.getFirstChild();
    final QNm formatName = new QNm("validTimeFormat");
    definitionNode.deleteAttribute(formatName);
    if (!format.isEmpty()) {
      definitionNode.setAttribute(formatName, new Str(format));
    }
    try (var output = new PrintStream(Files.newOutputStream(catalogue))) {
      final SubtreePrinter printer = new SubtreePrinter(output);
      printer.print(persisted);
      printer.end();
    }
    Databases.clearGlobalCaches();
    final int rebuiltId;
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      final byte[] obsoleteCatalogue = Files.readAllBytes(catalogue);
      assertEquals(oldRevision, session.getMostRecentRevisionNumber());
      final long[] history = session.getHistoryTimestamps();
      assertTrue(session.getRtxIndexController(oldRevision).getIndexes().getIndexDefs().isEmpty());
      try (var reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(oldRevision, reader.getRevisionNumber());
        assertTrue(reader.moveTo(objectKey));
        assertTrue(reader.isObject());
      }
      assertEquals(oldRevision, session.getMostRecentRevisionNumber());
      assertArrayEquals(history, session.getHistoryTimestamps());
      assertArrayEquals(obsoleteCatalogue, Files.readAllBytes(catalogue));
      if (upgradeOnCommit) {
        try (var writer = session.beginNodeTrx()) {
          assertEquals(oldRevision, session.getMostRecentRevisionNumber());
          assertTrue(session.getRtxIndexController(oldRevision).getIndexes().getIndexDefs().isEmpty());
          assertTrue(session.getWtxIndexController(writer.getRevisionNumber())
                            .getIndexes()
                            .getIndexDefs()
                            .stream()
                            .noneMatch(IndexDef::needsValidTimeRebuild));
          writer.commit();
        }
      } else {
        session.rebuildValidTimeIndexes();
      }
      assertEquals(oldRevision + 1, session.getMostRecentRevisionNumber());
      session.rebuildValidTimeIndexes();
      assertEquals(oldRevision + 1, session.getMostRecentRevisionNumber());
      final IndexDef rebuilt =
          session.getRtxIndexController(oldRevision + 1).getIndexes().getIndexDefs().iterator().next();
      assertFalse(rebuilt.needsValidTimeRebuild());
      rebuiltId = rebuilt.getID();
      assertNotEquals(0, rebuiltId);
      assertTrue(session.getRtxIndexController(oldRevision).getIndexes().getIndexDefs().isEmpty());
      try (var reader = session.beginNodeReadOnlyTrx()) {
        final LongOpenHashSet matches = new LongOpenHashSet();
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), rebuiltId, domain)
                                     .stab(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(objectKey, unchangedKey), matches);
      }
    }
    if (revert) {
      try (var database = Databases.openJsonDatabase(databasePath);
          var session = database.beginResourceSession("rows");
          var writer = session.beginNodeTrx()) {
        writer.revertTo(oldRevision);
        writer.commit();
      }
      Databases.clearGlobalCaches();
    }
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      final int revision = oldRevision + (revert
          ? 2
          : 1);
      assertEquals(revision, session.getMostRecentRevisionNumber());
      final IndexDef definition =
          session.getRtxIndexController(revision).getIndexes().getIndexDef(rebuiltId, IndexType.VALIDTIME);
      assertFalse(definition.needsValidTimeRebuild());
      try (var reader = session.beginNodeReadOnlyTrx()) {
        final LongOpenHashSet matches = new LongOpenHashSet();
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), rebuiltId, domain)
                                     .stab(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(objectKey, unchangedKey), matches);
      }
      try (var writer = session.beginNodeTrx()) {
        assertTrue(writer.moveTo(endKey));
        writer.setStringValue("2023-06-01T00:00:00Z");
        writer.commit();
      }
      try (var reader = session.beginNodeReadOnlyTrx()) {
        final LongOpenHashSet matches = new LongOpenHashSet();
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), rebuiltId, domain)
                                     .stab(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(unchangedKey), matches);
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      assertEquals(oldRevision + (revert
          ? 3
          : 2), session.getMostRecentRevisionNumber());
      assertFalse(session.getRtxIndexController(session.getMostRecentRevisionNumber())
                         .getIndexes()
                         .getIndexDef(rebuiltId, IndexType.VALIDTIME)
                         .needsValidTimeRebuild());
      try (var reader = session.beginNodeReadOnlyTrx()) {
        final LongOpenHashSet matches = new LongOpenHashSet();
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), rebuiltId, domain)
                                     .stab(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(unchangedKey), matches);
      }
    }
  }
}
