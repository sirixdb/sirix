/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.ram;

import io.sirix.access.ResourceConfiguration;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.Reader;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.StoreFeatures;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The storage-provider contract of {@link RAMStorage}, checked at the {@link Reader}/{@link Writer}
 * level: writes store an owned encoding, reads hand out caller-owned pages, committed state is
 * shared by every storage opened on the resource and dropped only on removal.
 */
final class RAMStorageContractTest {

  @TempDir
  Path directory;

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(8L * 1024 * 1024 * 1024);
  }

  @AfterEach
  void discard() {
    RAMStorage.discardUnder(directory);
  }

  @Test
  void readsHandOutCallerOwnedPages() {
    final ResourceConfiguration configuration = configuration("owned", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      assertFalse(writer.returnsSharedPages(), "every read decodes a page its caller owns");
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, revisionRoot(1_000L), bytes);

      final Page first = writer.read(reference, null);
      final Page second = writer.read(reference, null);
      assertNotSame(first, second, "two reads of one key must not share a page instance");
      first.close();
      assertEquals(1_000L, ((RevisionRootPage) writer.read(reference, null)).getRevisionTimestampMicros(),
          "closing a page a reader received must not affect later reads");
    }
    try (Reader reader = storage.createReader()) {
      assertFalse(reader.returnsSharedPages());
    }
  }

  @Test
  void readsVerifyThePayloadAgainstTheReferenceHash() {
    final ResourceConfiguration configuration = ResourceConfiguration.newBuilder("checked")
                                                                     .storageType(StorageType.IN_MEMORY)
                                                                     .verifyChecksumsOnRead(true)
                                                                     .build();
    configuration.resourcePath = directory.resolve("checked");
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, revisionRoot(1_000L), bytes);
      assertInstanceOf(RevisionRootPage.class, writer.read(reference, configuration), "control: matching hash");

      final PageReference mismatched = new PageReference();
      mismatched.setKey(reference.getKey());
      mismatched.setHash(reference.getHashAsLong() + 1);
      assertThrows(SirixCorruptionException.class, () -> writer.read(mismatched, configuration));
    }
  }

  /** Byte-handler pipelines a key-value leaf page is stored through. */
  enum Pipeline {
    NONE, DEFLATE, LZ4
  }

  @ParameterizedTest
  @EnumSource(Pipeline.class)
  void closingTheWrittenLeafPageLeavesTheStoredPageIntact(final Pipeline kind) throws IOException {
    final ByteHandlerPipeline pipeline = switch (kind) {
      case NONE -> new ByteHandlerPipeline();
      case DEFLATE -> new ByteHandlerPipeline(new DeflateCompressor());
      case LZ4 -> {
        assumeTrue(FFILz4Compressor.isNativeAvailable(), "liblz4 is not available");
        yield new ByteHandlerPipeline(new FFILz4Compressor());
      }
    };
    final ResourceConfiguration configuration = ResourceConfiguration.newBuilder("leaf-" + kind)
                                                                     .storageType(StorageType.IN_MEMORY)
                                                                     .byteHandlerPipeline(pipeline)
                                                                     .build();
    configuration.resourcePath = directory.resolve("leaf-" + kind);
    final RAMStorage storage = new RAMStorage(configuration);
    final byte[] record = {3, 1, 4, 1, 5, 9};
    if (pipeline.supportsMemorySegments()) {
      final MemorySegment encoded = pipeline.compress(MemorySegment.ofArray(record));
      try (var decoded = storage.getByteHandler().decompressScoped(encoded)) {
        assertArrayEquals(record, decoded.segment().toArray(ValueLayout.JAVA_BYTE),
            "the exposed handler decodes the configured encoding");
      }
    } else {
      final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
      try (OutputStream output = pipeline.serialize(encoded)) {
        output.write(record);
      }
      try (InputStream input = storage.getByteHandler().deserialize(new ByteArrayInputStream(encoded.toByteArray()))) {
        assertArrayEquals(record, input.readAllBytes(), "the exposed handler decodes the configured encoding");
      }
    }
    final PageReference reference = new PageReference();
    final KeyValueLeafPage written = new KeyValueLeafPage(41L, IndexType.DOCUMENT, configuration, 1, null, null, false);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      // Non-leaf metadata must use the same configured encoding as leaf payloads.
      writer.write(configuration, new PageReference(), revisionRoot(1_234L), bytes);
      assertEquals(1_234L, writer.readRevisionRootPage(0, configuration).getRevisionTimestampMicros());
      written.setSlot(record, 7);
      writer.write(configuration, reference, written, bytes);
      assertFalse(written.isClosed(), "writing must leave the writer's page with the writer");
      // The writer's commit cleanup closes the pages it wrote: the store must not depend on them.
      written.close();
      assertThrows(IllegalStateException.class, () -> writer.write(configuration, new PageReference(), written, bytes),
          "a closed page has nothing left to store");
    }
    for (int read = 0; read < 2; read++) {
      try (Reader reader = storage.createReader()) {
        final KeyValueLeafPage stored = assertInstanceOf(KeyValueLeafPage.class, reader.read(reference, configuration));
        try {
          assertNotSame(written, stored);
          assertFalse(stored.isClosed());
          assertEquals(41L, stored.getPageKey());
          assertArrayEquals(record, stored.getSlotAsByteArray(7));
        } finally {
          // The caller owns what it read; releasing it must not affect the next read.
          stored.close();
        }
      }
    }
  }

  @Test
  void everyWrittenRevisionRootStaysReadable() {
    final ResourceConfiguration configuration = configuration("revision-growth", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    final int revisions = 40;
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final RevisionRootPage first = revisionRoot(0L);
      for (int revision = 0; revision < revisions; revision++) {
        final RevisionRootPage root = revision == 0
            ? first
            : new RevisionRootPage(first, revision);
        root.setCommitTimestamp(Instant.ofEpochSecond(0, revision * 1_000L));
        writer.write(configuration, new PageReference(), root, bytes);
      }
    }
    try (Reader reader = storage.createReader()) {
      for (int revision = 0; revision < revisions; revision++) {
        assertEquals(revision, reader.readRevisionRootPage(revision, configuration).getRevision());
        assertEquals(Instant.ofEpochSecond(0, revision * 1_000L), reader.readRevisionRootPageCommitTimestamp(revision));
      }
    }
  }

  @Test
  void writtenPageInstanceStaysWithTheWriter() {
    final ResourceConfiguration configuration = configuration("isolated", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final RevisionRootPage written = revisionRoot(1_000L);
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, written, bytes);

      // The writer owns its instance: changing or closing it after the write must not reach the store.
      written.assignCommitTimestampMicros(Instant.ofEpochSecond(0, 2_000_000L));
      written.close();
      final RevisionRootPage stored = writer.readRevisionRootPage(0, configuration);
      assertNotSame(written, stored);
      assertEquals(1_000L, stored.getRevisionTimestampMicros());
    }
  }

  @Test
  void writtenRevisionRootsAreReadableAndOthersAreNot() {
    final ResourceConfiguration configuration = configuration("revisions", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, revisionRoot(1_234L), bytes);

      assertEquals(Instant.ofEpochSecond(0, 1_234_000L), writer.getRevisionFileData(0).timestamp());
      assertEquals(Instant.ofEpochSecond(0, 1_234_000L), writer.readRevisionRootPageCommitTimestamp(0));
      assertEquals(0, writer.readRevisionRootPage(0, configuration).getRevision());

      assertThrows(SirixIOException.class, () -> writer.getRevisionFileData(1));
      assertThrows(SirixIOException.class, () -> writer.readRevisionRootPage(-1, configuration));
      assertThrows(SirixIOException.class, () -> writer.readRevisionRootPageCommitTimestamp(7));
    }
  }

  @Test
  void unknownKeysFailInsteadOfReturningNothing() {
    final RAMStorage storage = new RAMStorage(configuration("unknown", UUID.randomUUID()));
    try (Reader reader = storage.createReader()) {
      for (final long key : new long[] {0L, 1L, -15L, IOStorage.DATA_REGION_START, IOStorage.DATA_REGION_START + 63,
          IOStorage.DATA_REGION_START + 64, IOStorage.DATA_REGION_START + 65, 1L << 20, Long.MAX_VALUE}) {
        final PageReference reference = new PageReference();
        reference.setKey(key);
        assertThrows(SirixIOException.class, () -> reader.read(reference, null), "key " + key);
      }
      assertThrows(SirixIOException.class, reader::readUberPageReference, "no uber page committed yet");
    }
  }

  @Test
  void committedUberPageReadsBackAsCommitted() {
    final ResourceConfiguration configuration = configuration("uber", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    assertFalse(storage.exists());
    assertTrue(storage.getOpeningUberPage().isBootstrap());

    final UberPage committed = new UberPage();
    committed.addFeatures(new StoreFeatures(0, 0, 4));
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final PageReference reference = new PageReference();
      writer.writeUberPageReference(configuration, reference, committed, bytes);
    }
    assertTrue(storage.exists());

    try (Reader reader = storage.createReader()) {
      final UberPage opened = (UberPage) reader.readUberPageReference().getPage();
      assertNotSame(committed, opened);
      assertFalse(opened.isBootstrap(), "a committed uber page is never the bootstrap page");
      assertEquals(committed.getRevisionCount(), opened.getRevisionCount());
      assertEquals(new StoreFeatures(0, 0, 4), opened.getFeatures());
      // The storage engine re-reads the committed uber page through the reference it was handed.
      final UberPage reread =
          assertInstanceOf(UberPage.class, reader.read(reader.readUberPageReference(), configuration));
      assertFalse(reread.isBootstrap());
      assertEquals(committed.getRevisionCount(), reread.getRevisionCount());
    }
  }

  @Test
  void manyWrittenPagesAllReadBack() {
    final ResourceConfiguration configuration = configuration("grow", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    final int pages = 1_000;
    final PageReference[] references = new PageReference[pages];
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      for (int i = 0; i < pages; i++) {
        references[i] = new PageReference();
        writer.write(configuration, references[i], revisionRoot(i), bytes);
      }
    }
    try (Reader reader = storage.createReader()) {
      for (int i = 0; i < pages; i++) {
        assertEquals(i, ((RevisionRootPage) reader.read(references[i], configuration)).getRevisionTimestampMicros());
      }
    }
  }

  @Test
  void truncateEmptiesTheStoreAndTruncateToRefuses() {
    final ResourceConfiguration configuration = configuration("truncate", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    final PageReference reference = new PageReference();
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      writer.write(configuration, reference, revisionRoot(1L), bytes);
      writer.writeUberPageReference(configuration, new PageReference(), new UberPage(), bytes);
      assertFalse(writer.supportsTruncateTo());
      assertThrows(UnsupportedOperationException.class, () -> writer.truncateTo(0));
      assertTrue(storage.exists(), "a refused rollback changes nothing");

      writer.truncate();
      assertFalse(storage.exists());
      assertThrows(SirixIOException.class, () -> writer.read(reference, configuration));
      assertThrows(SirixIOException.class, () -> writer.getRevisionFileData(0));

      // A truncated store accepts and serves new writes.
      final PageReference rewritten = new PageReference();
      writer.write(configuration, rewritten, revisionRoot(2L), bytes);
      assertEquals(2L, ((RevisionRootPage) writer.read(rewritten, configuration)).getRevisionTimestampMicros());
      assertEquals(Instant.ofEpochSecond(0, 2_000L), writer.readRevisionRootPageCommitTimestamp(0));
    }
  }

  /** TXN-615 clauses 3/8: discarding one resource clears retained providers, not its neighbours. */
  @Test
  void discardClearsRetainedProvidersOnlyWithinTheRequestedDirectory() {
    final ResourceConfiguration retired = configuration("retired", UUID.randomUUID());
    final ResourceConfiguration neighbour = configuration("retired-neighbour", UUID.randomUUID());
    final RAMStorage retiredStorage = new RAMStorage(retired);
    final RAMStorage neighbourStorage = new RAMStorage(neighbour);
    final PageReference retiredPage = new PageReference();
    final PageReference neighbourPage = new PageReference();
    try (Writer retiredWriter = retiredStorage.createWriter();
        Writer neighbourWriter = neighbourStorage.createWriter();
        BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      retiredWriter.write(retired, retiredPage, revisionRoot(1L), bytes);
      neighbourWriter.write(neighbour, neighbourPage, revisionRoot(2L), bytes);
      retiredWriter.writeUberPageReference(retired, new PageReference(), new UberPage(), bytes);
      neighbourWriter.writeUberPageReference(neighbour, new PageReference(), new UberPage(), bytes);
    }
    RAMStorage.discardUnder(retired.resourcePath);
    assertFalse(retiredStorage.exists());
    assertTrue(neighbourStorage.exists());
    try (Reader retiredReader = retiredStorage.createReader();
        Reader neighbourReader = neighbourStorage.createReader()) {
      assertThrows(SirixIOException.class, () -> retiredReader.read(retiredPage, retired));
      assertThrows(SirixIOException.class, () -> retiredReader.getRevisionFileData(0));
      assertEquals(2L,
          ((RevisionRootPage) neighbourReader.read(neighbourPage, neighbour)).getRevisionTimestampMicros());
    }
    assertEquals(0, retiredStorage.getRevisionIndexHolder().get().size());
  }

  /**
   * TXN-615 / low-allocation storage contract: adding small revision roots uses bounded bytes per
   * revision, including both staged and published metadata. A full-history copy on each append
   * violates the budget; the bound allows 8 KiB per small root, well above its encoded payload.
   */
  @Test
  void revisionHistoryAllocationIsBoundedPerAppend() {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    appendHistory(configuration("warmup", UUID.randomUUID()), 1024);
    final ResourceConfiguration configuration = configuration("bounded-history", UUID.randomUUID());
    final long before = bean.getCurrentThreadAllocatedBytes();
    appendHistory(configuration, 8192);
    final long allocated = bean.getCurrentThreadAllocatedBytes() - before;
    assertTrue(allocated <= 8192L * 8192,
        "8192 small revisions allocated " + allocated + " bytes (8 KiB/revision budget)");
  }

  private static void appendHistory(final ResourceConfiguration configuration, final int count) {
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final RevisionRootPage first = revisionRoot(0L);
      for (int revision = 0; revision < count; revision++) {
        final RevisionRootPage root = revision == 0
            ? first
            : new RevisionRootPage(first, revision);
        root.setCommitTimestamp(Instant.ofEpochSecond(0, revision * 1_000L));
        writer.write(configuration, new PageReference(), root, bytes);
        writer.writeUberPageReference(configuration, new PageReference(), new UberPage(revision + 1), bytes);
      }
    }
    assertEquals(count, storage.getRevisionIndexHolder().get().size());
  }

  private ResourceConfiguration configuration(final String name, final UUID identity) {
    final ResourceConfiguration configuration =
        ResourceConfiguration.newBuilder(name).storageType(StorageType.IN_MEMORY).resourceUuid(identity).build();
    configuration.resourcePath = directory.resolve(name);
    return configuration;
  }

  private static RevisionRootPage revisionRoot(final long timestampMicros) {
    // Serialization stamps the commit timestamp (or the current time when none is set).
    final RevisionRootPage page = new RevisionRootPage();
    page.setCommitTimestamp(Instant.ofEpochSecond(0, timestampMicros * 1_000L));
    return page;
  }
}
