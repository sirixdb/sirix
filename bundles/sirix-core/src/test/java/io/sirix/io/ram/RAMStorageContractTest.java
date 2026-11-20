/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.ram;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
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
import static org.junit.jupiter.api.Assertions.assertSame;
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
      assertEquals(1_000L, ((RevisionRootPage) writer.read(reference, null)).getRevisionTimestamp(),
          "closing a page a reader received must not affect later reads");
    }
    try (Reader reader = storage.createReader()) {
      assertFalse(reader.returnsSharedPages());
    }
    assertSame(configuration.byteHandlePipeline, storage.getByteHandler());
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
  void closingTheWrittenLeafPageLeavesTheStoredPageIntact(final Pipeline kind) {
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
    final PageReference reference = new PageReference();
    final KeyValueLeafPage written = new KeyValueLeafPage(41L, IndexType.DOCUMENT, configuration, 1, null, null, false);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
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
  void revisionNumbersBeyondTwiceTheTableStillFit() {
    final ResourceConfiguration configuration = configuration("revision-jump", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final RevisionRootPage first = revisionRoot(0L);
      writer.write(configuration, new PageReference(), first, bytes);
      // A revision far past the table's doubling (16 -> 32) must grow it to fit, not overflow it.
      final RevisionRootPage far = new RevisionRootPage(first, 100);
      far.setCommitTimestamp(Instant.ofEpochMilli(5L));
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, far, bytes);
      assertEquals(reference.getKey(), writer.getRevisionFileData(100).offset());
      assertEquals(100, writer.readRevisionRootPage(100, configuration).getRevision());
    }
  }

  @Test
  void revisionTableGrowsWithoutLosingEarlierRevisions() {
    final ResourceConfiguration configuration = configuration("revision-growth", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    final int revisions = 40;
    final long[] keys = new long[revisions];
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final RevisionRootPage first = revisionRoot(0L);
      for (int revision = 0; revision < revisions; revision++) {
        final RevisionRootPage root = revision == 0
            ? first
            : new RevisionRootPage(first, revision);
        root.setCommitTimestamp(Instant.ofEpochMilli(revision));
        final PageReference reference = new PageReference();
        writer.write(configuration, reference, root, bytes);
        keys[revision] = reference.getKey();
      }
    }
    try (Reader reader = storage.createReader()) {
      for (int revision = 0; revision < revisions; revision++) {
        assertEquals(keys[revision], reader.getRevisionFileData(revision).offset());
        assertEquals(revision, reader.readRevisionRootPage(revision, configuration).getRevision());
      }
      assertEquals(revisions, storage.getRevisionIndexHolder().get().size());
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
      assertTrue(reference.getKey() >= IOStorage.DATA_REGION_START,
          "page keys must never alias the uber-page beacon offsets");
      assertTrue(reference.hasHash(), "the reference carries the stored payload's hash, as on a file");

      // The writer owns its instance: changing or closing it after the write must not reach the store.
      written.setRevisionTimestamp(2_000L);
      written.close();
      final RevisionRootPage stored = writer.readRevisionRootPage(0, configuration);
      assertNotSame(written, stored);
      assertEquals(1_000L, stored.getRevisionTimestamp());
    }
  }

  @Test
  void revisionRecordsDescribeEveryWrittenRevisionRoot() {
    final ResourceConfiguration configuration = configuration("revisions", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final PageReference reference = new PageReference();
      writer.write(configuration, reference, revisionRoot(1_234L), bytes);

      final RevisionFileData data = writer.getRevisionFileData(0);
      assertEquals(reference.getKey(), data.offset());
      assertEquals(reference.getHashAsLong(), data.pageHash());
      assertEquals(Instant.ofEpochMilli(1_234L), data.timestamp());
      assertEquals(Instant.ofEpochMilli(1_234L), writer.readRevisionRootPageCommitTimestamp(0));
      assertEquals(0, writer.readRevisionRootPage(0, configuration).getRevision());
      assertEquals(1, storage.getRevisionIndexHolder().get().size());

      assertThrows(SirixIOException.class, () -> writer.getRevisionFileData(1));
      // 16 is the first revision past the initial revision table.
      assertThrows(SirixIOException.class, () -> writer.getRevisionFileData(16));
      assertThrows(SirixIOException.class, () -> writer.readRevisionRootPage(-1, configuration));
      assertThrows(SirixIOException.class, () -> writer.readRevisionRootPageCommitTimestamp(7));
    }
  }

  @Test
  void unknownKeysFailInsteadOfReturningNothing() {
    final RAMStorage storage = new RAMStorage(configuration("unknown", UUID.randomUUID()));
    try (Reader reader = storage.createReader()) {
      // DATA_REGION_START + 64 is the first key past the initial page table.
      for (final long key : new long[] {IOStorage.DATA_REGION_START, IOStorage.DATA_REGION_START - 1,
          IOStorage.DATA_REGION_START + 64, 0L, -15L, Long.MAX_VALUE}) {
        final PageReference reference = new PageReference();
        reference.setKey(key);
        assertThrows(SirixIOException.class, () -> reader.read(reference, null), "key " + key);
      }
      final PageReference beacon = new PageReference();
      beacon.setKey(IOStorage.PRIMARY_BEACON_OFFSET);
      assertThrows(SirixIOException.class, () -> reader.read(beacon, null), "no uber page committed yet");
      assertThrows(SirixIOException.class, reader::readUberPageReference);
    }
  }

  @Test
  void uberPageIsServedFreshFromBothBeaconsAndAsAPage() {
    final ResourceConfiguration configuration = configuration("uber", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    assertFalse(storage.exists());
    assertTrue(storage.getOpeningUberPage().isBootstrap());

    final UberPage committed = new UberPage();
    committed.addFeatures(new StoreFeatures(0, 0, 4));
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      final PageReference reference = new PageReference();
      writer.writeUberPageReference(configuration, reference, committed, bytes);
      assertEquals(IOStorage.SECONDARY_BEACON_OFFSET, reference.getKey(),
          "keyed like a file writer's reference after both beacon copies");
      assertTrue(reference.hasHash(), "the reference carries the uber payload's hash, as on a file");
    }
    assertTrue(storage.exists());

    try (Reader reader = storage.createReader()) {
      final UberPage opened = (UberPage) reader.readUberPageReference().getPage();
      assertNotSame(committed, opened);
      assertFalse(opened.isBootstrap(), "a committed uber page is never the bootstrap page");
      assertEquals(committed.getRevisionCount(), opened.getRevisionCount());
      assertEquals(new StoreFeatures(0, 0, 4), opened.getFeatures());
      for (final long offset : new long[] {IOStorage.PRIMARY_BEACON_OFFSET, IOStorage.SECONDARY_BEACON_OFFSET}) {
        final PageReference beacon = new PageReference();
        beacon.setKey(offset);
        final UberPage read = assertInstanceOf(UberPage.class, reader.read(beacon, configuration));
        assertFalse(read.isBootstrap());
        assertEquals(committed.getRevisionCount(), read.getRevisionCount());
      }
    }
  }

  @Test
  void committedStateBelongsToTheResourceUntilDiscarded() {
    final UUID identity = UUID.randomUUID();
    final RAMStorage first = new RAMStorage(configuration("shared", identity));
    commitUberPage(first, configuration("shared", identity));
    first.close();

    final RAMStorage reopened = new RAMStorage(configuration("shared", identity));
    assertTrue(reopened.exists(), "a later storage on the same resource sees its committed state");
    assertFalse(reopened.getOpeningUberPage().isBootstrap());

    assertFalse(new RAMStorage(configuration("shared", UUID.randomUUID())).exists(),
        "a resource recreated at the same path must not inherit its predecessor's revisions");
    assertFalse(new RAMStorage(configuration("shared", identity)).exists(), "the replaced store is gone for good");

    commitUberPage(new RAMStorage(configuration("other", identity)), configuration("other", identity));
    RAMStorage.discardUnder(directory.resolve("shared"));
    assertTrue(new RAMStorage(configuration("other", identity)).exists(),
        "discarding one resource leaves its siblings alone");
    RAMStorage.discardUnder(directory);
    assertFalse(new RAMStorage(configuration("other", identity)).exists(),
        "discarding a database directory drops every resource below it");
  }

  @Test
  void pageTableGrowsWithoutLosingEarlierPages() {
    final ResourceConfiguration configuration = configuration("grow", UUID.randomUUID());
    final RAMStorage storage = new RAMStorage(configuration);
    final int pages = 1_000;
    final PageReference[] references = new PageReference[pages];
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      for (int i = 0; i < pages; i++) {
        references[i] = new PageReference();
        writer.write(configuration, references[i], revisionRoot(i), bytes);
        if (i > 0) {
          assertEquals(references[i - 1].getKey() + 1, references[i].getKey(), "keys are dense");
        }
      }
    }
    try (Reader reader = storage.createReader()) {
      for (int i = 0; i < pages; i++) {
        assertEquals(i, ((RevisionRootPage) reader.read(references[i], configuration)).getRevisionTimestamp());
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

      assertSame(writer, writer.truncate());
      assertFalse(storage.exists());
      assertThrows(SirixIOException.class, () -> writer.read(reference, configuration));
      assertThrows(SirixIOException.class, () -> writer.getRevisionFileData(0));
      assertEquals(0, storage.getRevisionIndexHolder().get().size());

      final PageReference rewritten = new PageReference();
      writer.write(configuration, rewritten, revisionRoot(2L), bytes);
      assertEquals(IOStorage.DATA_REGION_START, rewritten.getKey(), "a truncated store starts over");
    }
  }

  private ResourceConfiguration configuration(final String name, final UUID identity) {
    final ResourceConfiguration configuration =
        ResourceConfiguration.newBuilder(name).storageType(StorageType.IN_MEMORY).resourceUuid(identity).build();
    configuration.resourcePath = directory.resolve(name);
    return configuration;
  }

  private static void commitUberPage(final RAMStorage storage, final ResourceConfiguration configuration) {
    try (Writer writer = storage.createWriter(); BytesOut<MemorySegment> bytes = Bytes.elasticHeapByteBuffer()) {
      writer.writeUberPageReference(configuration, new PageReference(), new UberPage(), bytes);
    }
  }

  private static RevisionRootPage revisionRoot(final long timestamp) {
    // Serialization stamps the commit timestamp (or the current time when none is set).
    final RevisionRootPage page = new RevisionRootPage();
    page.setCommitTimestamp(Instant.ofEpochMilli(timestamp));
    return page;
  }
}
