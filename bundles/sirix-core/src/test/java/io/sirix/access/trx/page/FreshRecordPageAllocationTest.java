/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexType;
import io.sirix.io.IOTestHelper;
import io.sirix.io.Reader;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fresh writer-page allocation, ownership, and cold-wire regressions. */
final class FreshRecordPageAllocationTest {

  private static FrameSlotAllocator allocator;

  @BeforeAll
  static void initializeAllocator() {
    final MemorySegmentAllocator configuredAllocator = Allocators.getInstance();
    allocator = assertInstanceOf(FrameSlotAllocator.class, configuredAllocator,
        "the default HFT allocator must expose exact frame-allocation telemetry");
    allocator.init(8L * 1024 * 1024 * 1024);
  }

  @Test
  @DisplayName("Fresh writer pages allocate only their two owned frames and release them on close")
  void freshPagesDoNotAllocateAndImmediatelyReleaseLegacyBuffers() {
    final int frameClass = FrameSlotAllocator.indexForSize(PageLayout.INITIAL_PAGE_SIZE);

    for (final boolean storeDeweyIds : new boolean[] {false, true}) {
      final ResourceConfiguration config =
          new ResourceConfiguration.Builder("fresh-allocation-" + storeDeweyIds).useDeweyIDs(storeDeweyIds).build();
      final long allocationsBefore = allocator.allocateCount(frameClass);
      final long releasesBefore = allocator.releaseCount(frameClass);

      final PageContainer container =
          NodeStorageEngineWriter.createFreshRecordPage(73L, IndexType.DOCUMENT, config, 11);
      final KeyValueLeafPage complete = (KeyValueLeafPage) container.getComplete();
      final KeyValueLeafPage modified = (KeyValueLeafPage) container.getModified();
      try {
        assertEquals(2L, allocator.allocateCount(frameClass) - allocationsBefore,
            "only the complete and modified slotted frames may be allocated");
        assertEquals(0L, allocator.releaseCount(frameClass) - releasesBefore,
            "fresh construction must not churn compatibility buffers through the allocator");
        assertNotNull(complete.getSlottedPage());
        assertNotNull(modified.getSlottedPage());
        assertEquals(PageLayout.INITIAL_PAGE_SIZE, complete.getSlottedPage().byteSize());
        assertEquals(PageLayout.INITIAL_PAGE_SIZE, modified.getSlottedPage().byteSize());
        assertEquals(storeDeweyIds, complete.areDeweyIDsStored());
        assertEquals(storeDeweyIds, modified.areDeweyIDsStored());
      } finally {
        complete.close();
        modified.close();
      }

      assertEquals(2L, allocator.releaseCount(frameClass) - releasesBefore,
          "both writer-owned frames must be returned exactly once when their pages close");
    }
  }

  @Test
  @DisplayName("Fresh-page failure cleanup attempts both closes and retains the exact primary failure")
  void constructionFailureCleanupRetainsPrimaryAndSuppressesBothCloseFailures() {
    final RuntimeException primaryFailure = new IllegalStateException("second page construction failed");
    final RuntimeException modifiedCloseFailure = new IllegalArgumentException("modified close failed");
    final Error completeCloseFailure = new AssertionError("complete close failed");
    final AtomicInteger closeAttempts = new AtomicInteger();
    final AutoCloseable modified = () -> {
      closeAttempts.incrementAndGet();
      throw modifiedCloseFailure;
    };
    final AutoCloseable complete = () -> {
      closeAttempts.incrementAndGet();
      throw completeCloseFailure;
    };

    NodeStorageEngineWriter.closeFreshPageAfterFailure(modified, primaryFailure);
    NodeStorageEngineWriter.closeFreshPageAfterFailure(complete, primaryFailure);

    assertEquals(2, closeAttempts.get(), "a failed first cleanup must not skip the second page");
    final Throwable[] suppressed = primaryFailure.getSuppressed();
    assertEquals(2, suppressed.length);
    assertSame(modifiedCloseFailure, suppressed[0]);
    assertSame(completeCloseFailure, suppressed[1]);

    NodeStorageEngineWriter.closeFreshPageAfterFailure(() -> {
      throw primaryFailure;
    }, primaryFailure);
    assertEquals(2, primaryFailure.getSuppressed().length,
        "self-suppression must never replace or mutate the original failure");
  }

  @Test
  @DisplayName("Fresh writer pages retain exact slot and Dewey bytes after a cold file reopen")
  void freshPageWireRoundTripIsExactWithAndWithoutDeweyIds(@TempDir final Path tempDir) {
    for (final boolean storeDeweyIds : new boolean[] {false, true}) {
      final ResourceConfiguration config =
          new ResourceConfiguration.Builder("fresh-cold-" + storeDeweyIds)
                                                                          .byteHandlerPipeline(
                                                                              new ByteHandlerPipeline())
                                                                          .useDeweyIDs(storeDeweyIds)
                                                                          .build();
      config.resourcePath = tempDir.resolve("resource-" + storeDeweyIds);

      final byte[] deweyId = {1, 3, 5, 8, 13, 21};
      final int recordLength = PageConstants.MAX_RECORD_SIZE - (storeDeweyIds
          ? deweyId.length + PageLayout.DEWEY_ID_TRAILER_SIZE
          : 0);
      final byte[] record = new byte[recordLength];
      for (int i = 0; i < record.length; i++) {
        record[i] = (byte) (i * 29 + 7);
      }
      final PageContainer container =
          NodeStorageEngineWriter.createFreshRecordPage(91L, IndexType.DOCUMENT, config, 17);
      final KeyValueLeafPage complete = (KeyValueLeafPage) container.getComplete();
      final KeyValueLeafPage modified = (KeyValueLeafPage) container.getModified();
      KeyValueLeafPage coldPage = null;
      final FileChannelStorage storage = new FileChannelStorage(config, Caffeine.newBuilder().buildAsync());
      try {
        assertFalse(complete.isSlotSet(37));
        assertFalse(modified.isSlotSet(37));
        modified.setSlot(record, 37);
        if (storeDeweyIds) {
          modified.setDeweyId(deweyId, 37);
        }
        assertEquals(PageConstants.MAX_RECORD_SIZE, PageLayout.getDirDataLength(modified.getSlottedPage(), 37),
            "the record body and optional Dewey metadata must exercise the exact inline boundary");

        final PageReference writtenReference = new PageReference();
        try (BytesOut<?> appendBuffer = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
            Writer writer = storage.createWriter()) {
          writer.write(config, writtenReference, modified, appendBuffer);
          writer.flushBufferedWrites(appendBuffer);
          IOTestHelper.writeRevisionZeroRoot(writer, config, appendBuffer);
          writer.writeUberPageReference(config, new PageReference(), new UberPage(), appendBuffer);
        }

        try (Reader reader = storage.createReader()) {
          coldPage = (KeyValueLeafPage) reader.read(new PageReference(writtenReference), config);
        }

        assertEquals(91L, coldPage.getPageKey());
        assertEquals(17, coldPage.getRevision());
        assertEquals(IndexType.DOCUMENT, coldPage.getIndexType());
        assertEquals(storeDeweyIds, coldPage.areDeweyIDsStored());
        assertTrue(coldPage.isSlotSet(37));
        assertArrayEquals(record, coldPage.getSlotAsByteArray(37));
        if (storeDeweyIds) {
          assertArrayEquals(deweyId, coldPage.getDeweyIdAsByteArray(37));
        } else {
          assertNull(coldPage.getDeweyIdAsByteArray(37));
        }
      } finally {
        if (coldPage != null) {
          coldPage.close();
        }
        complete.close();
        modified.close();
        storage.close();
      }
    }
  }
}
