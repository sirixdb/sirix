/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.node.json.JsonDocumentRootNode;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Async-snapshot ownership regressions for pending flyweight records. */
final class KeyValueLeafPageFlyweightSnapshotOwnershipTest {

  private static final LongHashFunction HASH_FUNCTION = LongHashFunction.xx3();

  @BeforeAll
  static void initializeAllocator() {
    Allocators.getInstance().init(256L * 1024 * 1024);
  }

  @Test
  void serializingADeepCopyDoesNotRebindOrLoseTheSourceFlyweight() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("flyweight-snapshot-alias").build();
    final KeyValueLeafPage source = newPage(config);
    final JsonDocumentRootNode sourceRoot = newRoot();
    KeyValueLeafPage copy = null;
    JsonDocumentRootNode copiedRoot = null;
    try {
      source.setRecord(sourceRoot);

      final KeyValueLeafPage snapshot = source.deepCopy();
      copy = snapshot;
      final JsonDocumentRootNode snapshotRoot = assertInstanceOf(JsonDocumentRootNode.class, snapshot.getRecord(0));
      copiedRoot = snapshotRoot;
      assertNotSame(sourceRoot, snapshotRoot,
          "an async copy must not share a flyweight whose mutable binding crosses the epoch boundary");

      snapshot.addReferences(config);

      assertAll("serializing the copy must not mutate the live record", () -> assertFalse(sourceRoot.isBound()),
          () -> assertTrue(snapshotRoot.isBoundTo(snapshot.getSlottedPage())), () -> assertRootFields(sourceRoot));

      source.addReferences(config);

      assertAll("the source must serialize its own pending record rather than skip a foreign binding",
          () -> assertTrue(sourceRoot.isBoundTo(source.getSlottedPage())),
          () -> assertTrue(PageLayout.isSlotPopulated(source.getSlottedPage(), 0)),
          () -> assertTrue(PageLayout.isSlotPopulated(snapshot.getSlottedPage(), 0)),
          () -> assertRootFields(sourceRoot));
    } finally {
      sourceRoot.clearBinding();
      if (copiedRoot != null) {
        copiedRoot.clearBinding();
      }
      if (copy != null) {
        copy.close();
      }
      source.close();
    }
  }

  @Test
  void closeNeverDecodesAFlyweightBoundToAForeignReusedFrame() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("flyweight-close-owner").build();
    final KeyValueLeafPage source = newPage(config);
    final JsonDocumentRootNode sourceRoot = newRoot();
    try (Arena foreignArena = Arena.ofConfined()) {
      source.setRecord(sourceRoot);
      final MemorySegment reusedForeignFrame = corruptForeignFrame(foreignArena);
      sourceRoot.bind(reusedForeignFrame, 0L, 0L, 0);

      assertDoesNotThrow(source::close,
          "closing one snapshot must not materialize a record from another snapshot's reused frame");
      assertAll(() -> assertTrue(source.isClosed()), () -> assertTrue(sourceRoot.isBoundTo(reusedForeignFrame),
          "the frame owner, not this closed page, controls the foreign binding"));
    } finally {
      sourceRoot.clearBinding();
      source.close();
    }
  }

  @Test
  void recordCleanupNeverDecodesAFlyweightBoundToAForeignReusedFrame() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("flyweight-gc-owner").build();
    final KeyValueLeafPage source = newPage(config);
    final JsonDocumentRootNode sourceRoot = newRoot();
    try (Arena foreignArena = Arena.ofConfined()) {
      source.setRecord(sourceRoot);
      final MemorySegment reusedForeignFrame = corruptForeignFrame(foreignArena);
      sourceRoot.bind(reusedForeignFrame, 0L, 0L, 0);

      assertDoesNotThrow(source::clearRecordsForGC);
      assertAll(() -> assertNull(source.getRecord(0)), () -> assertTrue(sourceRoot.isBoundTo(reusedForeignFrame),
          "GC cleanup must clear only this page's reference, not another page's binding"));
    } finally {
      sourceRoot.clearBinding();
      source.close();
    }
  }

  private static KeyValueLeafPage newPage(final ResourceConfiguration config) {
    return new KeyValueLeafPage(0L, IndexType.DOCUMENT, config, 1, null, null, false);
  }

  private static JsonDocumentRootNode newRoot() {
    return new JsonDocumentRootNode(0L, 17L, 17L, 1L, 9L, HASH_FUNCTION);
  }

  private static MemorySegment corruptForeignFrame(final Arena arena) {
    final MemorySegment frame = arena.allocate(256, Long.BYTES);
    // Offset-table bytes and the referenced data are all unterminated varints. The old ownership
    // check called JsonDocumentRootNode.unbind() here and reproduced "Varint too long (>10 bytes)".
    frame.fill((byte) 0x80);
    return frame;
  }

  private static void assertRootFields(final JsonDocumentRootNode root) {
    assertAll(() -> assertEquals(17L, root.getFirstChildKey()), () -> assertEquals(17L, root.getLastChildKey()),
        () -> assertEquals(1L, root.getChildCount()), () -> assertEquals(9L, root.getDescendantCount()));
  }
}
