/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.io.IOTestHelper;
import io.sirix.io.Reader;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageKind;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.settings.Constants;
import io.sirix.settings.StringCompressionType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Final-commit cache, ownership, and wire invariants for the empty byte-handler pipeline. */
final class FinalCommitEmptyPipelineTest {

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(8L * 1024 * 1024 * 1024);
  }

  @Test
  @DisplayName("Empty final-commit preparation retains no identity copy and cold-reads the file append")
  void emptyPipelinePreparesInParallelAndEncodesOnlyIntoSynchronousAppendScratch(@TempDir final Path tempDir) {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("final-empty-pipeline").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                 .build();
    config.resourcePath = tempDir.resolve("resource");

    final byte[] first = {3, 1, 4, 1, 5, 9};
    final byte[] second = new byte[PageConstants.MAX_RECORD_SIZE];
    for (int i = 0; i < second.length; i++) {
      second[i] = (byte) (i * 17 + 11);
    }

    final KeyValueLeafPage page = new KeyValueLeafPage(41L, IndexType.DOCUMENT, config, 1, null, null, false);
    KeyValueLeafPage coldPage = null;
    final FileChannelStorage storage = new FileChannelStorage(config, Caffeine.newBuilder().buildAsync());
    try {
      page.setSlot(first, 7);
      page.setSlot(second, 501);

      assertFalse(NodeStorageEngineWriter.prepareFinalCommitKeyValuePage(config, page),
          "an empty pipeline must prepare records/references without encoding a retained wire image");
      assertTrue(page.isAddedReferences(), "overflow discovery and record materialization stay in the parallel pass");
      assertNull(page.getCompressedSegment());
      assertNull(page.getBytes());
      assertArrayEquals(first, page.getSlotAsByteArray(7), "preparation must preserve retry/rollback state");
      assertArrayEquals(second, page.getSlotAsByteArray(501), "preparation must preserve retry/rollback state");

      final PageReference writtenReference = new PageReference();
      try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
          Writer writer = storage.createWriter()) {
        writer.write(config, writtenReference, page, appendBuffer);
        writer.flushBufferedWrites(appendBuffer);
        IOTestHelper.writeRevisionZeroRoot(writer, config, appendBuffer);
        writer.writeUberPageReference(config, new PageReference(), new UberPage(), appendBuffer);
      }

      assertNull(page.getCompressedSegment(),
          "the synchronous FileChannel scratch must not publish an owned empty-pipeline copy");
      assertNull(page.getBytes());

      final PageReference coldReference = new PageReference(writtenReference);
      try (Reader reader = storage.createReader()) {
        coldPage = (KeyValueLeafPage) reader.read(coldReference, config);
      }
      assertEquals(page.getPageKey(), coldPage.getPageKey());
      assertEquals(page.getRevision(), coldPage.getRevision());
      assertArrayEquals(first, coldPage.getSlotAsByteArray(7));
      assertArrayEquals(second, coldPage.getSlotAsByteArray(501));
    } finally {
      if (coldPage != null) {
        coldPage.close();
      }
      page.close();
      storage.close();
    }
  }

  @Test
  @DisplayName("A configured handler keeps the parallel final-commit encoded cache")
  void nonEmptyPipelineStillRetainsOwnedParallelCache() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("final-nonempty-pipeline")
                                                                    .byteHandlerPipeline(new ByteHandlerPipeline(
                                                                        new PrefixMemorySegmentHandler()))
                                                                    .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(42L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      page.setSlot(new byte[] {2, 7, 1, 8, 2, 8}, 17);

      assertTrue(NodeStorageEngineWriter.prepareFinalCommitKeyValuePage(config, page));
      final MemorySegment encoded = page.getCompressedSegment();
      assertNotNull(encoded, "configured handlers must retain their owned pre-serialized result");
      assertNull(page.getBytes());
      assertEquals(PrefixMemorySegmentHandler.MARKER, encoded.get(ValueLayout.JAVA_BYTE, 0L));
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("The preparation verdict reports actual cache absence for unresolved overflow")
  void nonEmptyPipelineWithUnresolvedOverflowReportsNoCache() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("final-unresolved-overflow")
                                                                      .byteHandlerPipeline(new ByteHandlerPipeline(
                                                                          new PrefixMemorySegmentHandler()))
                                                                      .build();
    final KeyValueLeafPage page = new KeyValueLeafPage(45L, IndexType.DOCUMENT, config, 1, null, null, false);
    try {
      final byte[] value = {8, 6, 7, 5, 3, 0, 9};
      page.setSlot(value, 19);
      page.getReferencesMap().put((page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + 19L, new PageReference());

      assertFalse(NodeStorageEngineWriter.prepareFinalCommitKeyValuePage(config, page),
          "the return value must describe actual cache presence, not merely pipeline configuration");
      assertNull(page.getCompressedSegment());
      assertNull(page.getBytes());
      assertArrayEquals(value, page.getSlotAsByteArray(19));
    } finally {
      page.close();
    }
  }

  @Test
  @DisplayName("Default MemorySegment sinks retain identity; synchronous scratch does not")
  void memorySegmentSinkRetentionContractIsExplicit() {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("memory-segment-retention").byteHandlerPipeline(new ByteHandlerPipeline())
                                                                     .build();
    final KeyValueLeafPage retainingPage = new KeyValueLeafPage(43L, IndexType.DOCUMENT, config, 1, null, null, false);
    final KeyValueLeafPage scratchPage = new KeyValueLeafPage(44L, IndexType.DOCUMENT, config, 1, null, null, false);
    try (MemorySegmentBytesOut retainingSink = new MemorySegmentBytesOut(128 * 1024);
        MemorySegmentBytesOut synchronousScratch = MemorySegmentBytesOut.synchronousScratch(128 * 1024)) {
      retainingPage.setSlot(new byte[] {1, 6, 1, 8}, 3);
      scratchPage.setSlot(new byte[] {0, 3, 3, 9}, 4);

      assertTrue(retainingSink.retainsEmptyPipelineIdentityCache());
      PageKind.KEYVALUELEAFPAGE.serializePage(config, retainingSink, retainingPage, SerializationType.DATA);
      assertNotNull(retainingPage.getCompressedSegment(), "ordinary sinks keep the conservative owned-copy contract");

      assertFalse(synchronousScratch.retainsEmptyPipelineIdentityCache());
      PageKind.KEYVALUELEAFPAGE.serializePage(config, synchronousScratch, scratchPage, SerializationType.DATA);
      assertTrue(synchronousScratch.writePosition() > 0L);
      assertNull(scratchPage.getCompressedSegment(),
          "synchronous scratch must leave the exact wire prefix only in its caller-owned buffer");
      assertNull(scratchPage.getBytes());
    } finally {
      retainingPage.close();
      scratchPage.close();
    }
  }

  @Test
  @DisplayName("An overlong child is durable before its empty-pipeline parent and survives a cold reopen")
  void overlongChildBeforeParentSurvivesColdReopen(@TempDir final Path tempDir) {
    final Path databasePath = tempDir.resolve("overlong-database");
    final String resource = "overlong-empty-pipeline";
    final String value = "overlong-value-0123456789".repeat(8_000);
    final long nodeKey;

    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(resource)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .stringCompressionType(StringCompressionType.NONE)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(resource);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertArrayAsFirstChild();
        nodeKey = wtx.insertStringValueAsFirstChild(value).getNodeKey();
        wtx.commit();
      }
    }

    // Eliminate every process-local page/revision shortcut. The value is larger than the 512-byte
    // encoded-record ceiling, so this read can succeed only if recursive commit wrote its OverflowPage
    // child, installed that disk key in the leaf wire, and then wrote the parent KVL.
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(nodeKey));
      assertEquals(value, rtx.getValue());
    } finally {
      Databases.clearGlobalCaches();
    }
  }

  /** Deterministic owned-result handler proving that only the empty pipeline bypasses retention. */
  private static final class PrefixMemorySegmentHandler implements ByteHandler {

    private static final byte MARKER = (byte) 0xA7;

    @Override
    public OutputStream serialize(final OutputStream toSerialize) {
      return toSerialize;
    }

    @Override
    public InputStream deserialize(final InputStream toDeserialize) {
      return toDeserialize;
    }

    @Override
    public ByteHandler getInstance() {
      return new PrefixMemorySegmentHandler();
    }

    @Override
    public MemorySegment compress(final MemorySegment source) {
      final int sourceLength = Math.toIntExact(source.byteSize());
      final MemorySegment encoded = MemorySegment.ofArray(new byte[sourceLength + 1]);
      encoded.set(ValueLayout.JAVA_BYTE, 0L, MARKER);
      MemorySegment.copy(source, 0L, encoded, 1L, sourceLength);
      return encoded;
    }

    @Override
    public boolean supportsMemorySegments() {
      return true;
    }
  }
}
