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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeIndexRebuildTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "2", "3"})
  void openingRebuildsObsoleteRootsOnceAndRebindsMaintenance(final String format) throws Exception {
    final Path databasePath = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Path catalogue;
    final int oldRevision;
    final long objectKey;
    final long endKey;
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL)
          .validTimePaths("vf", "vt").build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        objectKey = writer.getNodeKey();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToRightSibling());
        endKey = writer.getNodeKey();
        final IndexDef definition = IndexDefs.createValidTimeIdxDef(
            Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(definition), writer);
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, domain)
            .delete(objectKey, domain.lowerBound(Instant.parse("2023-01-01T00:00:00Z")),
                domain.upperBound(Instant.parse("2025-01-01T00:00:00Z")));
        writer.commit();
        oldRevision = session.getMostRecentRevisionNumber();
        catalogue = session.getResourceConfig().getResource()
            .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath()).resolve(oldRevision + ".xml");
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
      assertEquals(oldRevision + 1, session.getMostRecentRevisionNumber());
      final IndexDef rebuilt = session.getRtxIndexController(oldRevision + 1).getIndexes().getIndexDefs().iterator().next();
      assertFalse(rebuilt.needsValidTimeRebuild());
      rebuiltId = rebuilt.getID();
      assertNotEquals(0, rebuiltId);
      assertTrue(session.getRtxIndexController(oldRevision).getIndexes().getIndexDefs().isEmpty());
      try (var reader = session.beginNodeReadOnlyTrx()) {
        final LongOpenHashSet matches = new LongOpenHashSet();
        final IntervalDomain domain = new IntervalDomain();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), rebuiltId, domain)
            .stab(domain.point(Instant.parse("2024-01-01T00:00:00Z")), matches::add);
        assertEquals(LongOpenHashSet.of(objectKey), matches);
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
        assertTrue(matches.isEmpty());
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      assertEquals(oldRevision + 2, session.getMostRecentRevisionNumber());
      assertFalse(session.getRtxIndexController(oldRevision + 2).getIndexes()
          .getIndexDef(rebuiltId, IndexType.VALIDTIME).needsValidTimeRebuild());
    }
  }
}
