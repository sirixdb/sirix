package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.index.IndexType;
import io.sirix.io.IOStorage;
import io.sirix.page.PageReference;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import java.time.Instant;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.StringCompressionType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class StorageEpochStoreIdentityTest {
  private static final int RECORDS = 12000;
  private static final User USER = new User("store-twin", new UUID(0L, 1L));

  @TempDir
  Path directory;

  @AfterEach
  void restoreBound() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
    Databases.clearGlobalCaches();
  }

  @Test
  void mappedEpochsMatchThePlainStore() throws IOException {
    compare(StorageType.MEMORY_MAPPED, StringCompressionType.NONE, false, false, false);
  }

  @Test
  void deflatedEpochsMatchThePlainStore() throws IOException {
    compare(StorageType.FILE_CHANNEL, StringCompressionType.NONE, true, false, false);
  }

  @Test
  void firstFsstTableAndStoreMatchThePlainTransaction() throws IOException {
    compare(StorageType.FILE_CHANNEL, StringCompressionType.FSST, false, false, false);
  }

  @Test
  void supersededOverflowCarriersNeverReachTheDataFile() throws IOException {
    compare(StorageType.FILE_CHANNEL, StringCompressionType.NONE, false, true, false);
  }

  @Test
  void oversizedFsstInputsBecomeInlineLikeThePlainTwin() throws IOException {
    compare(StorageType.FILE_CHANNEL, StringCompressionType.FSST, false, true, false);
  }

  @Test
  void mappedOverflowCarriersMatchThePlainTwin() throws IOException {
    compare(StorageType.MEMORY_MAPPED, StringCompressionType.NONE, false, true, false);
  }

  @Test
  void reusedFsstTableKeepsOversizedEditsInlineLikeThePlainTwin() throws IOException {
    compare(StorageType.FILE_CHANNEL, StringCompressionType.FSST, false, true, true);
  }

  private void compare(final StorageType storage, final StringCompressionType compression, final boolean deflate,
      final boolean oversized, final boolean previousTable) throws IOException {
    final Path seed = directory.resolve("seed");
    createSeed(seed, storage, compression, deflate);
    final Path plainPath = directory.resolve("plain");
    final Path boundedPath = directory.resolve("bounded");
    copySeed(seed, plainPath);
    copySeed(seed, boundedPath);
    final Census plain = load(plainPath, compression, false, oversized, previousTable);
    final Census bounded = load(boundedPath, compression, true, oversized, previousTable);
    assertTrue(bounded.rotations() >= 3, "multiple epochs must be exercised");
    assertEquals(0, plain.rotations());
    assertEquals(plain.tableId(), bounded.tableId());
    assertArrayEquals(plain.table(), bounded.table());
    assertEquals(plain.pages().size(), bounded.pages().size(), "page count");
    for (int i = 0; i < plain.frames().size(); i++) {
      assertEquals(plain.pages().get(i), bounded.pages().get(i), "page kind, key, and encoded length at " + i);
      assertArrayEquals(plain.frames().get(i), bounded.frames().get(i), "persisted page " + i);
    }
    assertEquals(plain.frontier(), bounded.frontier(), "logical data-file frontier");
    assertEquals(plain.values(), bounded.values(), "cold document values");
    if (compression == StringCompressionType.FSST) {
      assertTrue(bounded.tableId() > 0);
      assertTrue(bounded.table().length > 0);
      if (oversized) {
        assertEquals(0, bounded.documentCarriers(), "compressed strings stay inline");
      }
    }
  }

  private static void createSeed(final Path path, final StorageType storage, final StringCompressionType compression,
      final boolean deflate) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER)) {
      final ResourceConfiguration.Builder config = ResourceConfiguration.newBuilder("resource")
                                                                        .storageType(storage)
                                                                        .stringCompressionType(compression)
                                                                        .customCommitTimestamps(true);
      config.byteHandlerPipeline(deflate
          ? new ByteHandlerPipeline(new DeflateCompressor())
          : new ByteHandlerPipeline());
      assertTrue(database.createResource(config.build()));
    }
    Databases.clearGlobalCaches();
  }

  private static void copySeed(final Path source, final Path target) throws IOException {
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

  private Census load(final Path path, final StringCompressionType compression, final boolean bounded,
      final boolean oversized, final boolean previousTable) throws IOException {
    int rotations;
    Path dataPath;
    StorageEpochTestSupport.setPlainTransactionBound(bounded
        ? StorageEpochTestSupport.IMPORT_BOUND
        : Integer.MAX_VALUE);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER)) {
      try (final JsonResourceSession session = database.beginResourceSession("resource");
          final JsonNodeTrx writer = session.beginNodeTrx()) {
        dataPath = session.getResourceConfig().resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                                           .resolve("sirix.data");
        writer.insertArrayAsFirstChild();
        final long array = writer.getNodeKey();
        final List<Long> keys = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
          assertTrue(writer.moveTo(array));
          writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"v\":\"" + value(i, 0, oversized) + "\"}"),
              JsonNodeTrx.Commit.NO);
          assertTrue(writer.moveTo(array) && writer.moveToLastChild());
          keys.add(writer.getNodeKey());
        }
        if (previousTable) {
          writer.commit(null, Instant.parse("2019-01-01T00:00:00Z"));
        }
        if (bounded && oversized) {
          writer.getStorageEngineWriter().asyncFlushStorageEpoch();
          writer.getStorageEngineWriter().awaitPendingAsyncFlush();
        }
        for (int pass = 1; pass <= 3; pass++) {
          for (int i = 0; i < RECORDS; i += 251) {
            assertTrue(writer.moveTo(keys.get(i)) && writer.moveToFirstChild());
            writer.setStringValue(value(i, pass, oversized && compression == StringCompressionType.FSST));
          }
        }
        if (oversized && compression == StringCompressionType.NONE) {
          assertTrue(writer.moveTo(keys.get(1)));
          writer.remove();
          if (bounded) {
            writer.getStorageEngineWriter().asyncFlushStorageEpoch();
            writer.getStorageEngineWriter().awaitPendingAsyncFlush();
          }
        }
        rotations = StorageEpochTestSupport.rotatedEpochs(writer);
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path, USER);
        final JsonResourceSession session = database.beginResourceSession("resource");
        final JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      final List<String> values = new ArrayList<>(RECORDS);
      assertTrue(reader.moveToDocumentRoot() && reader.moveToFirstChild() && reader.moveToFirstChild());
      do {
        final long key = reader.getNodeKey();
        assertTrue(reader.moveToFirstChild());
        values.add(reader.getValue());
        assertTrue(reader.moveTo(key));
      } while (reader.moveToRightSibling());
      assertEquals(RECORDS - (oversized && compression == StringCompressionType.NONE
          ? 1
          : 0), values.size());
      final StorageEngineReader engine = reader.getStorageEngineReader();
      final NamePage names = engine.getNamePage(engine.getActualRevisionRootPage());
      final long id = names.getLatestFsstSymbolTableId(DatabaseType.JSON);
      final byte[] table = id > 0
          ? names.getFsstSymbolTable(id, DatabaseType.JSON, engine)
          : new byte[0];
      final long rootOffset =
          ((NodeStorageEngineReader) engine).getReader().getRevisionFileData(reader.getRevisionNumber()).offset();
      final ByteBuffer length = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      try (final FileChannel file = FileChannel.open(dataPath)) {
        while (length.hasRemaining()) {
          assertTrue(file.read(length, rootOffset + length.position()) > 0);
        }
      }
      final long frontier = rootOffset + Integer.BYTES + length.flip().getInt();
      final List<String> pages = new ArrayList<>();
      final List<byte[]> frames = new ArrayList<>();
      int documentCarriers = 0;
      try (final FileChannel file = FileChannel.open(dataPath)) {
        long offset = IOStorage.DATA_REGION_START;
        while (offset < frontier) {
          final ByteBuffer header = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
          while (header.hasRemaining()) {
            assertTrue(file.read(header, offset + header.position()) > 0);
          }
          final int size = header.flip().getInt();
          final ByteBuffer payload = ByteBuffer.allocate(size);
          while (payload.hasRemaining()) {
            assertTrue(file.read(payload, offset + Integer.BYTES + payload.position()) > 0);
          }
          frames.add(payload.array());
          final Page page =
              ((NodeStorageEngineReader) engine).getReader()
                                                .read(new PageReference().setKey(offset), session.getResourceConfig());
          if (page instanceof final KeyValueLeafPage leaf && leaf.getIndexType() == IndexType.DOCUMENT) {
            documentCarriers += leaf.getReferencesMap().size();
          }
          pages.add(page.getClass().getSimpleName() + (page instanceof KeyValueLeafPage leaf
              ? ":" + leaf.getIndexType() + ":" + leaf.getPageKey() + ":carriers=" + leaf.getReferencesMap().size()

              : "") + "=" + size);
          page.close();
          offset = (offset + 4 + size + 7) & ~7L;
        }
      }
      return new Census(frontier, values, id, table, rotations, pages, frames, documentCarriers);
    }
  }

  private static String value(final int record, final int pass, final boolean oversized) {
    return "https://sirix.io/records/common-prefix/".repeat(oversized
        ? 40
        : 1) + record + "/publication/" + pass;
  }

  private record Census(long frontier, List<String> values, long tableId, byte[] table, int rotations,
      List<String> pages, List<byte[]> frames, int documentCarriers) {
  }
}
