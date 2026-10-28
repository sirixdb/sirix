package io.sirix.access.trx.page;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.SegmentDictionaryLane;
import io.sirix.io.StorageType;
import io.sirix.io.IOStorage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.NamePage;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class StorageEpochDifferentialTest {
  private static final long SEED = 0x51A1_2026L;
  private static final int RECORDS = 3072;
  private static final User USER = new User("epoch-differential", new UUID(0, 2));
  private final boolean savedChunked = ChunkedBodyConfig.setEnabledForTesting(true);
  private final int savedChunkTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(4096);
  private final String savedLane = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
  private final String savedGlobal = System.getProperty("sirix.projection.globalDict");

  @TempDir
  Path directory;

  @AfterEach
  void restore() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
    ChunkedBodyConfig.setEnabledForTesting(savedChunked);
    ChunkedBodyConfig.setTargetChunkBytesForTesting(savedChunkTarget);
    restoreProperty(SegmentDictionaryLane.ENABLED_PROPERTY, savedLane);
    restoreProperty("sirix.projection.globalDict", savedGlobal);
    ProjectionBulkLoad.clearActive();
    Databases.clearGlobalCaches();
  }

  @Test
  void randomEditsMatchAcrossStorageCompressionAndDictionaryLanes() throws IOException {
    for (final StorageType storage : new StorageType[] {StorageType.FILE_CHANNEL, StorageType.MEMORY_MAPPED}) {
      for (final StringCompressionType compression : new StringCompressionType[] {StringCompressionType.NONE,
          StringCompressionType.FSST}) {
        for (final boolean lane : new boolean[] {false, true}) {
          final String context = "seed=" + SEED + ", " + storage + ", " + compression + ", lane=" + lane;
          System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, Boolean.toString(lane));
          System.setProperty("sirix.projection.globalDict", "never");
          final Path casePath = directory.resolve(storage + "-" + compression + "-" + lane);
          final Path seed = casePath.resolve("seed");
          createSeed(seed, storage, compression);
          final Path plainPath = casePath.resolve("plain");
          final Path rotatingPath = casePath.resolve("rotating");
          copy(seed, plainPath);
          copy(seed, rotatingPath);
          final Result plain;
          final Result rotating;
          try {
            plain = exercise(plainPath, false, context);
            rotating = exercise(rotatingPath, true, context);
          } catch (final RuntimeException failure) {
            throw new AssertionError(context, failure);
          }
          assertEquals(0, plain.rotations(), context);
          assertTrue(rotating.rotations() >= 1, context);
          assertEquals(plain.revisions().size(), rotating.revisions().size(), context);
          for (int revision = 0; revision < plain.revisions().size(); revision++) {
            compare(plain.revisions().get(revision), rotating.revisions().get(revision), context + ", rev=" + revision);
          }
          compare(plain.revisions().getLast(), coldRead(plainPath, context), context + ", plain reopen");
          compare(rotating.revisions().getLast(), coldRead(rotatingPath, context), context + ", rotating reopen");
        }
      }
    }
  }

  private static void createSeed(final Path path, final StorageType storage, final StringCompressionType compression)
      throws IOException {
    Files.createDirectories(path.getParent());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(storage)
                                                              .stringCompressionType(compression)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .hashKind(HashType.NONE)
                                                              .versioningApproach(VersioningType.FULL)
                                                              .useDeweyIDs(false)
                                                              .storeNodeHistory(false)
                                                              .customCommitTimestamps(true)
                                                              .build()));
    }
    Databases.clearGlobalCaches();
  }

  private static IndexDef projection() {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/code", PathParser.Type.JSON), parse("/[]/id", PathParser.Type.JSON)),
        List.of(Type.STR, Type.INT), 0, IndexDef.DbType.JSON);
  }

  private static Result exercise(final Path path, final boolean rotating, final String context) throws IOException {
    StorageEpochTestSupport.setPlainTransactionBound(rotating
        ? StorageEpochTestSupport.IMPORT_BOUND
        : Integer.MAX_VALUE);
    final List<Snapshot> snapshots = new ArrayList<>(2);
    int rotations;
    final List<Row> rows = new ArrayList<>(RECORDS + 96);
    final Random random = new Random(SEED);
    final StringBuilder json = new StringBuilder(RECORDS * 100).append('[');
    for (int i = 0; i < RECORDS; i++) {
      final Row row = new Row(0L, "code-" + (i % 8), value(random, i), i);
      rows.add(row);
      if (i > 0) {
        json.append(',');
      }
      json.append(object(row));
    }
    json.append(']');
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource");
        final JsonNodeTrx writer = session.beginNodeTrx()) {
      final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
      controller.createProjectionIndexAtLoadStart(projection(), writer, RECORDS);
      ParallelBulkJsonImporter.assembleBytes(writer,
          new ByteArrayInputStream(json.toString().getBytes(StandardCharsets.UTF_8)), 1 << 20, 1);
      assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), context);
      assertTrue(writer.moveToFirstChild(), context);
      for (int i = 0; i < rows.size(); i++) {
        final Row row = rows.get(i);
        rows.set(i, new Row(writer.getNodeKey(), row.code(), row.value(), row.id()));
        if (i + 1 < rows.size()) {
          assertTrue(writer.moveToRightSibling(), context);
        }
      }
      rotations = StorageEpochTestSupport.rotatedEpochs(writer);
      if (rotating) {
        assertTrue(rotations >= 1, context + ", initial import must rotate");
      }
      writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      snapshots.add(read(session, rows, context));
      for (int op = 0; op < 96; op++) {
        final int index = random.nextInt(rows.size());
        final Row row = rows.get(index);
        assertTrue(writer.moveTo(row.key()), context);
        switch (random.nextInt(3)) {
          case 0 -> {
            writer.insertObjectRecordAsLastChild("extra" + op, new StringValue(value(random, RECORDS + op)));
          }
          case 1 -> {
            assertTrue(writer.moveToFirstChild() && writer.moveToRightSibling(), context);
            final String value = value(random, RECORDS + op);
            if (row.value() == null) {
              writer.moveToParent();
              writer.moveToFirstChild();
              writer.insertObjectRecordAsRightSibling("v", new StringValue(value));
            } else {
              writer.setStringValue(value);
            }
            rows.set(index, new Row(row.key(), row.code(), value, row.id()));
          }
          case 2 -> {
            if (row.value() != null) {
              assertTrue(writer.moveToFirstChild() && writer.moveToRightSibling(), context);
              writer.remove();
              rows.set(index, new Row(row.key(), row.code(), null, row.id()));
            }
          }
          default -> throw new AssertionError(context);
        }
      }
      rotations += StorageEpochTestSupport.rotatedEpochs(writer);
      writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
      snapshots.add(read(session, rows, context));
    }
    Databases.clearGlobalCaches();
    return new Result(rotations, snapshots);
  }

  private static Snapshot coldRead(final Path path, final String context) throws IOException {
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      return read(session, null, context);
    }
  }

  private static Snapshot read(final JsonResourceSession session, final @Nullable List<Row> expected,
      final String context) throws IOException {
    try (final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final List<String> scan = new ArrayList<>();
      final DescendantAxis axis = new DescendantAxis(reader);
      while (axis.hasNext()) {
        axis.nextLong();
        scan.add(reader.getNodeKey() + ":" + reader.getKind() + ":" + reader.getName() + ":" + reader.getValue());
      }
      if (expected != null) {
        assertTrue(reader.moveToDocumentRoot() && reader.moveToFirstChild(), context);
        assertEquals(expected.size(), reader.getChildCount(), context);
        for (int i = 0; i < expected.size(); i += 97) {
          final Row row = expected.get(i);
          assertTrue(reader.moveTo(row.key()) && reader.moveToFirstChild(), context);
          assertEquals(row.code(), reader.getValue(), context);
          assertTrue(reader.moveToRightSibling(), context);
          if (row.value() != null) {
            assertEquals(row.value(), reader.getValue(), context);
            assertTrue(reader.moveToRightSibling(), context);
          }
          assertEquals(row.id(), reader.getNumberValue().intValue(), context);
        }
      }
      final List<String> points = new ArrayList<>();
      for (long key = 0; key <= reader.getMaxNodeKey(); key += 113) {
        points.add(key + ":" + (reader.moveTo(key)
            ? reader.getKind() + ":" + reader.getValue()
            : "absent"));
      }
      final StorageEngineReader engine = reader.getStorageEngineReader();
      final NamePage names = engine.getNamePage(engine.getActualRevisionRootPage());
      final long tableId = names.getLatestFsstSymbolTableId(DatabaseType.JSON);
      final byte[] table = tableId > 0
          ? names.getFsstSymbolTable(tableId, DatabaseType.JSON, engine)
          : new byte[0];
      final byte[] metadata = ProjectionIndexHOTStorage.readBlob(engine, 0, 0);
      assertNotNull(metadata, context);
      final ProjectionIndexMetadata projection = ProjectionIndexMetadata.parse(metadata);
      final List<byte[]> groups =
          ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(engine, 0, projection.rowGroupCount());
      final long root =
          ((NodeStorageEngineReader) engine).getReader().getRevisionFileData(reader.getRevisionNumber()).offset();
      final Path data =
          session.getResourceConfig().resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                                  .resolve("sirix.data");
      final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      try (final FileChannel file = FileChannel.open(data)) {
        while (length.hasRemaining()) {
          assertTrue(file.read(length, root + length.position()) > 0, context);
        }
      }
      final long frontier = root + Integer.BYTES + length.flip().getInt();
      final List<String> frames = new ArrayList<>();
      int dictionaryCarriers = 0;
      try (final FileChannel file = FileChannel.open(data)) {
        for (long offset = IOStorage.DATA_REGION_START; offset < frontier;) {
          length.clear();
          while (length.hasRemaining()) {
            assertTrue(file.read(length, offset + length.position()) > 0, context);
          }
          final int size = length.flip().getInt();
          final Page page = ((NodeStorageEngineReader) engine).getReader()
                                                              .readRecordPageLazily(new PageReference().setKey(offset),
                                                                  session.getResourceConfig());
          try {
            if (page instanceof KeyValueLeafPage leaf && leaf.hasGlobalStringTags()
                && !leaf.getReferencesMap().isEmpty()) {
              dictionaryCarriers++;
            }
            frames.add(page.getClass().getSimpleName() + (page instanceof KeyValueLeafPage leaf
                ? ":" + leaf.getIndexType() + ":" + leaf.getPageKey() + ":carriers=" + leaf.getReferencesMap().size()
                : "") + ":" + size);
          } finally {
            page.close();
          }
          offset = (offset + Integer.BYTES + size + 7) & ~7L;
        }
      }
      if (Boolean.getBoolean(SegmentDictionaryLane.ENABLED_PROPERTY)) {
        assertTrue(dictionaryCarriers > 0, context + ", dictionary-tagged pages must carry overflow records");
      }
      return new Snapshot(frontier, scan, points, tableId, table, groups, frames);
    }
  }

  private static void compare(final Snapshot plain, final Snapshot rotating, final String context) {
    assertEquals(plain.frames(), rotating.frames(), context + ", frame census");
    assertEquals(plain.frontier(), rotating.frontier(), context + ", frontier");
    assertEquals(plain.scan(), rotating.scan(), context + ", full scan");
    assertEquals(plain.points(), rotating.points(), context + ", point reads");
    assertEquals(plain.tableId(), rotating.tableId(), context + ", FSST table id");
    assertArrayEquals(plain.table(), rotating.table(), context + ", FSST table bytes");
    assertEquals(plain.groups().size(), rotating.groups().size(), context + ", projection groups");
    for (int i = 0; i < plain.groups().size(); i++) {
      assertArrayEquals(plain.groups().get(i), rotating.groups().get(i), context + ", projection group " + i);
    }
  }

  private static String object(final Row row) {
    return "{\"code\":\"" + row.code() + "\",\"v\":\"" + row.value() + "\",\"id\":" + row.id() + ",\"flag\":true"
        + (row.id() == 1200
            ? ",\"huge\":" + "1234567890".repeat(400)
            : "")
        + "}";
  }

  private static String value(final Random random, final int ordinal) {
    if (ordinal == 0 || (ordinal >= RECORDS && ordinal % 127 == 0)) {
      final StringBuilder value = new StringBuilder(4096);
      for (int i = 0; i < 4096; i++) {
        value.append((char) ('a' + random.nextInt(26)));
      }
      return value.toString();
    }
    return "https://sirix.io/differential/".repeat((ordinal < 64 || ordinal >= RECORDS) && random.nextInt(9) == 0
        ? 50
        : 1) + random.nextInt(100);
  }

  private static void copy(final Path source, final Path target) throws IOException {
    try (final Stream<Path> paths = Files.walk(source)) {
      final Iterator<Path> iterator = paths.iterator();
      while (iterator.hasNext()) {
        final Path path = iterator.next();
        final Path destination = target.resolve(source.relativize(path));
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else {
          Files.copy(path, destination);
        }
      }
    }
  }

  private static void restoreProperty(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private record Row(long key, String code, @Nullable String value, int id) {
  }
  private record Snapshot(long frontier, List<String> scan, List<String> points, long tableId, byte[] table,
      List<byte[]> groups, List<String> frames) {
  }
  private record Result(int rotations, List<Snapshot> revisions) {
  }
}
