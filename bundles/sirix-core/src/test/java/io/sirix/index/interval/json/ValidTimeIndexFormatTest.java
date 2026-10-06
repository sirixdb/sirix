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
import io.sirix.access.trx.node.json.JsonIndexController;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ValidTimeIndexFormatTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "2", "3", "4", "5", "7"})
  void unsupportedFormatsAreExcludedFromReadsAndRejectedByWriters(final String format) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Path catalogue;
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        final IndexDef definition = IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0,
            IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        writer.commit();
        catalogue = session.getResourceConfig()
                           .getResource()
                           .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                           .resolve("1.xml");
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
    final IndexDef unsupported = new IndexDef(IndexDef.DbType.JSON);
    unsupported.init(definitionNode);
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      final byte[] catalogueBytes = Files.readAllBytes(catalogue);
      final long[] history = session.getHistoryTimestamps();
      assertTrue(session.getRtxIndexController(1).getIndexes().getIndexDefs().isEmpty());
      assertThrows(UnsupportedOperationException.class, session::beginNodeTrx);
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertArrayEquals(history, session.getHistoryTimestamps());
      assertArrayEquals(catalogueBytes, Files.readAllBytes(catalogue));
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("fresh")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("fresh"); var writer = session.beginNodeTrx()) {
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        assertThrows(UnsupportedOperationException.class, () -> controller.createIndexes(Set.of(unsupported), writer));
        assertThrows(UnsupportedOperationException.class,
            () -> controller.createIndexListeners(Set.of(unsupported), writer));
        assertTrue(controller.getIndexes().getIndexDefs().isEmpty());
      }
    }
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
}
