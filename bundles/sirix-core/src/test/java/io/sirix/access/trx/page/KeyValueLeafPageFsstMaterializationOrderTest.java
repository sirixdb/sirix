/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.settings.Constants;
import io.sirix.utils.FSSTCompressor;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class KeyValueLeafPageFsstMaterializationOrderTest {
  private static final long PAGE_KEY = 7L;
  private static final int INLINE_SLOT = 9;
  private static final int PENDING_SLOT = 5;
  private static final int PADDING_SLOT = 1;
  private static final byte[] TABLE = {1, 8, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(value = HashType.class, names = {"NONE", "ROLLING"})
  void materializationUsesCommitsCompressionOrderAcrossEveryEntryPoint(final HashType hash) {
    for (final boolean fused : new boolean[] {false, true}) {
      for (final int budget : new int[] {200, 204, 512}) {
        for (int route = 0; route < 3; route++) {
          final Path path = directory.resolve(hash + "-" + fused + "-" + budget + "-" + route);
          assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
          try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
            assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                .storageType(StorageType.FILE_CHANNEL)
                .hashKind(hash)
                .byteHandlerPipeline(new ByteHandlerPipeline())
                .build()));
            try (final JsonResourceSession session = database.beginResourceSession("resource");
                final JsonNodeTrx writer = session.beginNodeTrx()) {
              final ResourceConfiguration config = session.getResourceConfig();
              final KeyValueLeafPage plain = fixture(config, fused, budget);
              final KeyValueLeafPage rotating = fixture(config, fused, budget);
              boolean adopted = false;
              try {
                NodeStorageEngineWriter.prepareFinalCommitKeyValuePage(config, plain);
                final Set<Long> expected = budget < 512 ? Set.of(nodeKey(PENDING_SLOT)) : Set.of();
                assertEquals(expected, plain.getReferencesMap().keySet(), "unchanged commit is the layout oracle");
                final NodeStorageEngineWriter engine = (NodeStorageEngineWriter) writer.getStorageEngineWriter();
                if (route == 0) {
                  assertEquals(1, rotating.materializePendingRecords(config), "count only pending encodes");
                } else if (route == 1) {
                  engine.stageOverflowCarriersOfLiveLeaf(rotating);
                } else {
                  engine.adoptDocumentLeafPage(rotating);
                  adopted = true;
                }
                NodeStorageEngineWriter.prepareFinalCommitKeyValuePage(config, rotating);
                assertEquals(expected, rotating.getReferencesMap().keySet(),
                    "materialization must spend append capacity in commit order, route=" + route);
                assertNotNull(rotating.getSlot(INLINE_SLOT));
                assertArrayEquals(plain.getSlotAsByteArray(INLINE_SLOT), rotating.getSlotAsByteArray(INLINE_SLOT));
                assertEquals(plain.getFsstSymbolTableId(), rotating.getFsstSymbolTableId());
                if (budget == 512) {
                  assertArrayEquals(plain.getSlotAsByteArray(PENDING_SLOT), rotating.getSlotAsByteArray(PENDING_SLOT));
                }
              } finally {
                plain.close();
                if (!adopted) {
                  rotating.close();
                }
              }
            }
          }
          Databases.clearGlobalCaches();
        }
      }
    }
  }

  private static KeyValueLeafPage fixture(final ResourceConfiguration config, final boolean fused, final int budget) {
    final byte[] inline = "abcdefgh".repeat(48).getBytes(StandardCharsets.US_ASCII);
    final byte[] pending = "abcdefgh".repeat(128).getBytes(StandardCharsets.US_ASCII);
    assertEquals(49, FSSTCompressor.encode(inline, TABLE).length);
    assertEquals(129, FSSTCompressor.encode(pending, TABLE).length);
    final KeyValueLeafPage page = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 1, null, null, false);
    page.serializeNewRecord(string(config, fused, INLINE_SLOT, inline), nodeKey(INLINE_SLOT), INLINE_SLOT);
    page.setFsstSymbolTable(TABLE);
    page.setFsstSymbolTableId(3L);
    final MemorySegment scratch = MemorySegment.ofArray(new byte[1024]);
    while (page.getSlottedPage().byteSize() < KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY
        || remaining(page) > budget) {
      final int desired = page.getSlottedPage().byteSize() < KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY
          ? 512 : Math.min(512, remaining(page) - budget);
      int length = Math.max(0, desired - 55);
      int bytes;
      do {
        bytes = string(config, false, PADDING_SLOT, new byte[length++]).serializeToHeap(scratch, 0L);
      } while (bytes < desired);
      assertEquals(desired, bytes, "padding must leave the specified append budget");
      final long offset = page.prepareHeapForDirectWriteOrOverflow(bytes, 0);
      assertNotEquals(KeyValueLeafPage.DIRECT_WRITE_OVERFLOW, offset);
      MemorySegment.copy(scratch, 0L, page.getSlottedPage(), offset, bytes);
      page.completeDirectWrite(NodeKind.STRING_VALUE.getId(), nodeKey(PADDING_SLOT), PADDING_SLOT, bytes, null);
    }
    assertEquals(budget, remaining(page));
    page.setRecord(string(config, fused, PENDING_SLOT, pending));
    return page;
  }

  private static int remaining(final KeyValueLeafPage page) {
    return (int) page.getSlottedPage().byteSize() - PageLayout.HEAP_START - PageLayout.getHeapEnd(page.getSlottedPage());
  }

  private static FlyweightNode string(final ResourceConfiguration config, final boolean fused, final int slot,
      final byte[] value) {
    return fused
        ? new ObjectNamedStringNode(nodeKey(slot), 0L, 0L, 0L, 1, 1L, 0, 0, 0L, value,
            config.nodeHashFunction, (byte[]) null, false, null)
        : new StringNode(nodeKey(slot), 0L, 0, 0, 0L, 0L, 0L, value, config.nodeHashFunction, (byte[]) null,
            false, null);
  }

  private static long nodeKey(final int slot) {
    return (PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
  }
}
