/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.CompactImageStore;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * Who owns a compact HOT-leaf image's bytes, and what a reader of a recycled one is guaranteed.
 *
 * <p>
 * The image used to live on an automatic arena — an allocation per decode, on the per-fragment read
 * path. It now lives in a block the page owns and gives back, which means the bytes can be handed to
 * another image. These tests pin the two properties that makes safe: the block comes back only when
 * no guard holds the page, and an optimistic reader whose page was closed can never certify reads
 * taken over bytes the next lease overwrote.
 * </p>
 */
@Isolated
final class CompactImageOwnershipTest {

  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-image-ownership")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .build();

  private static final byte[] KEY_ONE = {7, 1};

  private static final byte[] KEY_TWO = {7, 2};

  @AfterEach
  void resetStore() {
    CompactImageStore.getInstance().setPoisonOnReleaseForTesting(false);
    CompactImageStore.getInstance().setRefuseLeasesForTesting(false);
  }

  @Test
  @DisplayName("a decoded compact image holds one pooled block and gives it back when it closes")
  void aCompactImageLeasesOneBlockAndReturnsIt() throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    final byte[] wire = compactWire(new byte[] {31}, new byte[] {32});
    final int leasedBefore = store.leasedBlockCount();
    final HOTLeafPage compact = decode(wire);
    try {
      assertTrue(compact.isCompactImage());
      assertTrue(compact.slots().isNative());
      assertEquals(leasedBefore + 1, store.leasedBlockCount(), "the image owns exactly one block");
      assertTrue(compact.slots().byteSize() >= compact.getUsedSlotsSize(), "the block holds the whole image");
    } finally {
      compact.close();
    }
    assertEquals(leasedBefore, store.leasedBlockCount(), "closing the page returned its block");
  }

  @Test
  @DisplayName("a guarded compact image keeps its block until the last guard goes")
  void aGuardedImageKeepsItsBlock() throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    final HOTLeafPage compact = decode(compactWire(new byte[] {31}, new byte[] {32}));
    final int leasedWithImage = store.leasedBlockCount();
    assertTrue(compact.acquireGuard());
    compact.close();
    assertTrue(compact.isOrphaned(), "the close is recorded");
    assertEquals(leasedWithImage, store.leasedBlockCount(), "eviction cannot recycle a guarded image's bytes");
    assertArrayEquals(new byte[] {31}, compact.copyStoredValue(compact.findEntry(KEY_ONE)),
        "the guarded reader still reads its own image");
    compact.releaseGuard();
    assertEquals(leasedWithImage - 1, store.leasedBlockCount(), "the last guard returns the block");
  }

  @Test
  @DisplayName("an optimistic reader cannot certify reads of a closed image whose block was re-leased")
  void aRecycledBlockInvalidatesTheReadersStamp() throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    store.setPoisonOnReleaseForTesting(true);
    final byte[] wire = compactWire(new byte[] {31}, new byte[] {32});
    final HOTLeafPage first = decode(wire);
    final MemorySegment slots = first.slots();
    // The reader's protocol: binding first, then the stamp, then the content.
    final long binding = first.readStampBinding();
    final long stamp = first.readStamp();
    assertEquals(0L, binding & 1L, "a live image's binding is even");
    assertEquals(0L, stamp & 1L, "a live image's stamp is even");
    assertArrayEquals(new byte[] {31}, first.copyStoredValue(first.findEntry(KEY_ONE)));
    assertTrue(first.validateStamp(binding, stamp), "reads of a live image certify");

    // Eviction. The page publishes closed and invalidates its binding before the block comes back.
    first.close();
    assertFalse(first.validateStamp(binding, stamp), "a closed image certifies nothing");

    // The next image of the same size takes the same block and overwrites those bytes.
    final HOTLeafPage second = decode(compactWire(new byte[] {41}, new byte[] {42}));
    try {
      assertEquals(slots.address(), second.slots().address(), "the block was handed to the next image");
      assertNotEquals((byte) 0x5A, second.slots().get(ValueLayout.JAVA_BYTE, 0L), "the new image overwrote the poison");
      assertArrayEquals(new byte[] {41}, second.copyStoredValue(second.findEntry(KEY_ONE)));
      assertFalse(first.validateStamp(binding, stamp), "and the first reader's stamp still refuses");
    } finally {
      second.close();
    }
  }

  @Test
  @DisplayName("a block larger than the image changes neither the bytes on the wire nor its immutability")
  void aLargerBlockChangesNeitherWireBytesNorImmutability() throws IOException {
    final byte[] wire = compactWire(new byte[] {31}, new byte[] {32});
    final HOTLeafPage compact = decode(wire);
    try {
      assertArrayEquals(wire, serialize(compact), "the image serialises from its used bytes only");
      // An assertion when they are enabled, the state exception otherwise; both carry the contract.
      final Throwable refusal = assertThrows(Throwable.class, () -> compact.put(new byte[] {7, 3}, new byte[] {33}),
          "a compact image refuses mutation however much slack its block has");
      assertTrue(refusal.getMessage() != null && refusal.getMessage().contains("immutable"),
          "the refusal must name the contract, not a bounds failure: " + refusal);
    } finally {
      compact.close();
    }
  }

  @Test
  @DisplayName("with the pool refused the decode still answers, on its own GC-owned memory")
  void aRefusedPoolFallsBackToGcOwnedMemory() throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    store.setRefuseLeasesForTesting(true);
    final int leasedBefore = store.leasedBlockCount();
    final HOTLeafPage compact = decode(compactWire(new byte[] {31}, new byte[] {32}));
    try {
      assertEquals(leasedBefore, store.leasedBlockCount(), "no block was leased");
      assertTrue(compact.slots().isNative());
      assertEquals(compact.getUsedSlotsSize(), compact.slots().byteSize(), "the fallback owns exactly its image");
      assertArrayEquals(new byte[] {31}, compact.copyStoredValue(compact.findEntry(KEY_ONE)));
      assertArrayEquals(new byte[] {32}, compact.copyStoredValue(compact.findEntry(KEY_TWO)));
    } finally {
      compact.close();
    }
  }

  private static HOTLeafPage decode(final byte[] wire) throws IOException {
    final Page page = new PagePersister().deserializeHOTLeafFragment(CONFIG,
        new MemorySegmentBytesIn(MemorySegment.ofArray(wire)), SerializationType.DATA);
    return assertInstanceOf(HOTLeafPage.class, page);
  }

  /** The durable image of a two-entry leaf. */
  private static byte[] compactWire(final byte[] firstValue, final byte[] secondValue) throws IOException {
    final HOTLeafPage source = new HOTLeafPage(1, 7, IndexType.PROJECTION);
    try {
      assertTrue(source.put(KEY_ONE, firstValue));
      assertTrue(source.put(KEY_TWO, secondValue));
      source.setCompleteDump(true);
      return serialize(source);
    } finally {
      source.close();
    }
  }

  private static byte[] serialize(final Page page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      return out.toByteArray();
    }
  }
}
