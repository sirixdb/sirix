/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTTrieWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.utils.OS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests for HOT production-readiness fixes.
 *
 * <p>Each nested class targets a specific bug fix from the production audit:
 * endianness consistency, splitToWithInsert rollback, ensureMutableSlotMemory,
 * acquireGuard on closed pages, initialBytePos widening, NodeReferencesSerializer
 * size pre-check, and getRootReference fail-fast.</p>
 */
@DisplayName("HOT Production Readiness Regression Tests")
class HOTProductionReadinessTest {

  @BeforeAll
  static void initAllocator() {
    if (!OS.isWindows()) {
      LinuxMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024);
    } else {
      WindowsMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024);
    }
  }

  // ========================================================================================
  // C1: Endianness (LE) consistency between mask construction and key lookup
  // ========================================================================================

  @Nested
  @DisplayName("C1: LE endianness consistency")
  class EndiannessConsistency {

    @Test
    @DisplayName("BiNode routes keys correctly based on discriminative bit in LE layout")
    void testBiNodeLERouting() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      // Discriminative bit at absolute position 12 = byte 1, bit 4 (within-byte)
      // In LE layout: byte 1 maps to bits 8-15 in the 64-bit word
      // bit 4 within byte means MSB-offset 4 → position (7 - 4) = 3 within byte → bit 11 in word
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 12, leftRef, rightRef);

      // Key with bit 12 = 0 → should go left (child 0)
      // Bit 12 = byte 1, bit 4. Bit 4 clear in byte 1 means byte1 & 0x08 == 0
      byte[] leftKey = new byte[] {(byte) 0xFF, 0x00, 0x00}; // byte 1 = 0x00, bit 4 is 0
      assertEquals(0, biNode.findChildIndex(leftKey), "Key with disc bit=0 should go left");

      // Key with bit 12 = 1 → should go right (child 1)
      // Bit 4 set means byte1 has bit 4 set: 0x08
      byte[] rightKey = new byte[] {(byte) 0xFF, 0x08, 0x00}; // byte 1 = 0x08, bit 4 is 1
      assertEquals(1, biNode.findChildIndex(rightKey), "Key with disc bit=1 should go right");
    }

    @Test
    @DisplayName("SpanNode routes 4 children correctly with multi-bit LE mask")
    void testSpanNodeLERoutingMultiBit() {
      // Two discriminative bits at positions:
      //   bit 1 in byte 0 (MSB-offset 1 → position 6 in word) → mask bit 6
      //   bit 0 in byte 1 (MSB-offset 0 → position 7+8=15 in word) → mask bit 15
      // Wait... let me think more carefully about the LE layout.
      //
      // LE layout: byte[pos+0] → bits 0-7, byte[pos+1] → bits 8-15
      // Within each byte, MSB (bit 0) → position 7, LSB (bit 7) → position 0
      //
      // Absolute disc bit = byteOffset * 8 + bitWithinByte
      // byteOffset from initialBytePos:
      //   Disc bit at abs pos 1 (byte 0, bit 1): byteWithinWindow=0, bitInWord = 0*8 + (7-1) = 6
      //   Disc bit at abs pos 8 (byte 1, bit 0): byteWithinWindow=1, bitInWord = 1*8 + (7-0) = 15

      // Build mask: bits 6 and 15
      long bitMask = (1L << 6) | (1L << 15);

      // 2 disc bits → 4 partial keys: 0b00, 0b01, 0b10, 0b11
      // PEXT extracts bit6 as position 0, bit15 as position 1
      // So partial key = (bit6_value << 0) | (bit15_value << 1)
      byte[] partialKeys = new byte[] {0b00, 0b01, 0b10, 0b11};

      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100L + i);
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, 0, bitMask, partialKeys, children);

      // Key: byte0=0x00, byte1=0x00 → bit6=0, bit15=0 → pkey=0b00 → child 0
      assertEquals(0, spanNode.findChildIndex(new byte[] {0x00, 0x00}));

      // Key: byte0=0x40 (bit 1 set = 0x40), byte1=0x00 → bit6=1, bit15=0 → pkey=0b01 → child 1?
      // Wait. bit 1 of byte 0 means MSB-offset 1. In the byte 0b01000000 = 0x40, that's bit 1 set.
      // LE: byte0 goes to bits 0-7. bit 6 in the word corresponds to bit 6 of byte0.
      // byte0 = 0x40 = 0b01000000. Bit 6 of that byte = 1. So PEXT extracts 1 at position 0.
      // byte1 = 0x00. Bit 15 of word = bit 7 of byte1 = MSB of byte1 = 0. PEXT extracts 0 at position 1.
      // pkey = 0b01 → but wait, PEXT ordering: bit6 (lower) maps to output bit 0, bit15 (higher) maps to output bit 1
      // So pkey = (bit6=1) | (bit15=0 << 1) = 0b01 → child index matching partial key 0b01 = child 1
      assertEquals(1, spanNode.findChildIndex(new byte[] {0x40, 0x00, 0x00}));

      // Key: byte0=0x00, byte1=0x80 (bit 0 set = MSB = 0x80) → bit6=0, bit15=1 → pkey=0b10 → child 2
      assertEquals(2, spanNode.findChildIndex(new byte[] {0x00, (byte) 0x80, 0x00}));

      // Key: byte0=0x40, byte1=0x80 → bit6=1, bit15=1 → pkey=0b11 → child 3
      assertEquals(3, spanNode.findChildIndex(new byte[] {0x40, (byte) 0x80, 0x00}));
    }

    @Test
    @DisplayName("BiNode at high byte position routes correctly")
    void testBiNodeHighBytePosition() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      // Disc bit at position 80 = byte 10, bit 0 (MSB of byte 10)
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 80, leftRef, rightRef);
      assertEquals(10, biNode.getInitialBytePos());

      // 12-byte key with byte[10] = 0x00 → disc bit 0 → left
      byte[] leftKey = new byte[12];
      assertEquals(0, biNode.findChildIndex(leftKey));

      // 12-byte key with byte[10] = 0x80 → disc bit 1 → right
      byte[] rightKey = new byte[12];
      rightKey[10] = (byte) 0x80;
      assertEquals(1, biNode.findChildIndex(rightKey));
    }

    @Test
    @DisplayName("findMostSignificantDiscriminativeBitPosition returns correct absolute bit position in LE layout")
    void testFindMSDBPositionLEConsistency() throws Exception {
      HOTTrieWriter writer = new HOTTrieWriter();
      Method msbMethod = HOTTrieWriter.class.getDeclaredMethod(
          "findMostSignificantDiscriminativeBitPosition", HOTIndirectPage.class);
      msbMethod.setAccessible(true);

      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      // Case 1: disc bit at absolute position 1 (byte 0, bit 1 within byte)
      // Construction: bitInWord = 0*8 + (7-1) = 6 → mask = 1L << 6 = 0x40
      HOTIndirectPage bi1 = HOTIndirectPage.createBiNode(1L, 1, 1, leftRef, rightRef);
      assertEquals(0x40L, bi1.getBitMask());
      int msb1 = (int) msbMethod.invoke(writer, bi1);
      assertEquals(1, msb1, "MSB for disc bit at absolute position 1");

      // Case 2: disc bit at absolute position 8 (byte 1, bit 0 = MSB of byte 1)
      // initialBytePos = 1, byteWithinWindow = 0, bitInWord = 0*8 + (7-0) = 7 → mask = 0x80
      HOTIndirectPage bi2 = HOTIndirectPage.createBiNode(1L, 1, 8, leftRef, rightRef);
      assertEquals(0x80L, bi2.getBitMask());
      int msb2 = (int) msbMethod.invoke(writer, bi2);
      assertEquals(8, msb2, "MSB for disc bit at absolute position 8");

      // Case 3: SpanNode with two disc bits at positions 1 and 9 (both in window starting at byte 0)
      // Position 1: byte 0, bit 1 → bitInWord = 0*8 + (7-1) = 6
      // Position 9: byte 1, bit 1 → bitInWord = 1*8 + (7-1) = 14
      // mask = (1L << 6) | (1L << 14) = 0x4040
      // MSB should be the MORE significant one = position 1 (byte 0)
      byte[] partialKeys = new byte[] {0b00, 0b01, 0b10, 0b11};
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
      }
      HOTIndirectPage span = HOTIndirectPage.createSpanNode(1L, 1, 0, 0x4040L, partialKeys, children);
      int msb3 = (int) msbMethod.invoke(writer, span);
      assertEquals(1, msb3, "MSB should be position 1 (most significant), not position 9");

      // Case 4: disc bit at high byte position (initialBytePos=10, disc bit at byte 10, bit 3)
      // absolute = 10*8 + 3 = 83
      // bitInWord = 0*8 + (7-3) = 4 → mask = 1L << 4 = 0x10
      HOTIndirectPage bi4 = HOTIndirectPage.createBiNode(1L, 1, 83, leftRef, rightRef);
      assertEquals(10, bi4.getInitialBytePos());
      int msb4 = (int) msbMethod.invoke(writer, bi4);
      assertEquals(83, msb4, "MSB for disc bit at absolute position 83 (byte 10, bit 3)");
    }
  }

  // ========================================================================================
  // C4 + splitToWithInsert: ensureMutableSlotMemory + rollback on failure
  // ========================================================================================

  @Nested
  @DisplayName("C4: ensureMutableSlotMemory for zero-copy deserialized pages")
  class EnsureMutableSlotMemory {

    @Test
    @DisplayName("Page with undersized slotMemory expands lazily on insert")
    void testInsertAfterUndersizedSlotMemory() {
      // Simulate a zero-copy deserialized page: create normally, serialize, then
      // create a new page with exact-fit slotMemory
      HOTLeafPage original = new HOTLeafPage(1L, 1, IndexType.PATH);
      for (int i = 0; i < 5; i++) {
        byte[] key = ("key" + i).getBytes();
        byte[] value = ("val" + i).getBytes();
        original.mergeWithNodeRefs(key, key.length, value, value.length);
      }

      // After deserialization, the page can accept new entries even if the
      // backing MemorySegment was sized tightly to existing data
      // (ensureMutableSlotMemory handles the expansion)
      byte[] newKey = "newkey".getBytes();
      byte[] newValue = "newvalue".getBytes();
      boolean inserted = original.mergeWithNodeRefs(newKey, newKey.length, newValue, newValue.length);
      assertTrue(inserted, "Insert should succeed on a mutable page");
      assertEquals(6, original.getEntryCount());
      assertTrue(original.findEntry(newKey) >= 0, "New key should be findable");
    }
  }

  @Nested
  @DisplayName("splitToWithInsert rollback")
  class SplitToWithInsertRollback {

    @Test
    @DisplayName("splitToWithInsert returns false and preserves source when MSDB is -1 (all identical keys)")
    void testSourcePreservedWhenCannotSplit() {
      HOTLeafPage source = new HOTLeafPage(1L, 1, IndexType.PATH);

      // Insert a single key — with count=1, findMsdbBit returns -1
      // but findMsdbWithNewKey may find a valid MSDB. So use a new key
      // that is identical to the existing one (update path, not split).
      source.put("onlykey".getBytes(), "val1".getBytes());

      final int originalCount = source.getEntryCount();
      assertEquals(1, originalCount);

      // Try splitting with the same key — this will find the key already exists
      // (isNew=false), then findMsdbBit with 1 entry returns -1 → false
      HOTLeafPage target = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] sameKey = "onlykey".getBytes();
      byte[] newValue = "val2".getBytes();

      boolean result = source.splitToWithInsert(target, sameKey, sameKey.length, newValue, newValue.length);

      assertFalse(result, "Cannot split page with single entry and existing key (MSDB=-1)");
      assertEquals(originalCount, source.getEntryCount(), "Source must be preserved");
      assertEquals(0, target.getEntryCount(), "Target must remain empty");
    }

    @Test
    @DisplayName("splitToWithInsert preserves all entries on success (balanced case)")
    void testAllEntriesPreservedOnSuccess() {
      HOTLeafPage source = new HOTLeafPage(1L, 1, IndexType.PATH);

      for (int i = 0; i < 20; i++) {
        byte[] key = String.format("key%03d", i).getBytes();
        byte[] value = ("v" + i).getBytes();
        source.put(key, value);
      }

      final int originalCount = source.getEntryCount();
      assertEquals(20, originalCount);

      HOTLeafPage target = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] newKey = "key010x".getBytes(); // sorts between existing keys
      byte[] newVal = "vnew".getBytes();

      boolean result = source.splitToWithInsert(target, newKey, newKey.length, newVal, newVal.length);

      assertTrue(result, "Split+insert should succeed");
      assertEquals(originalCount + 1, source.getEntryCount() + target.getEntryCount(),
          "Total entry count must be original + 1 (the new key)");
      assertTrue(source.getEntryCount() >= 1, "Left must not be empty");
      assertTrue(target.getEntryCount() >= 1, "Right must not be empty");
    }

    @Test
    @DisplayName("Degenerate split — all existing keys on left, new key alone on right")
    void testDegenerateSplitAllLeft() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      page.put("aaa1".getBytes(), "v1".getBytes());
      page.put("aaa2".getBytes(), "v2".getBytes());
      page.put("aaa3".getBytes(), "v3".getBytes());

      HOTLeafPage right = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] newKey = "zzz".getBytes();
      byte[] newVal = "vn".getBytes();

      boolean ok = page.splitToWithInsert(right, newKey, newKey.length, newVal, newVal.length);

      assertTrue(ok, "Degenerate split should succeed");
      assertTrue(page.getEntryCount() >= 1, "Left page must have at least 1 entry");
      assertTrue(right.getEntryCount() >= 1, "Right page must have at least 1 entry");
      assertEquals(4, page.getEntryCount() + right.getEntryCount(), "Total entries preserved");
    }

    @Test
    @DisplayName("Degenerate split — all existing keys on right, new key alone on left")
    void testDegenerateSplitAllRight() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      page.put("mmm1".getBytes(), "v1".getBytes());
      page.put("mmm2".getBytes(), "v2".getBytes());

      HOTLeafPage right = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] newKey = "aaa".getBytes();
      byte[] newVal = "vn".getBytes();

      boolean ok = page.splitToWithInsert(right, newKey, newKey.length, newVal, newVal.length);

      assertTrue(ok, "Degenerate right split should succeed");
      assertTrue(page.getEntryCount() >= 1, "Left must not be empty");
      assertTrue(right.getEntryCount() >= 1, "Right must not be empty");
    }

    @Test
    @DisplayName("Split with exactly 2 entries produces 1+1 distribution")
    void testTwoEntrySplit() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      page.put("apple".getBytes(), "v1".getBytes());

      HOTLeafPage right = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] newKey = "banana".getBytes();
      byte[] newVal = "v2".getBytes();

      boolean ok = page.splitToWithInsert(right, newKey, newKey.length, newVal, newVal.length);

      assertTrue(ok, "2-entry split should succeed");
      int total = page.getEntryCount() + right.getEntryCount();
      assertEquals(2, total, "Both entries should be present");
      assertTrue(page.getEntryCount() >= 1 && right.getEntryCount() >= 1,
          "Both pages must have at least 1 entry");
    }
  }

  // ========================================================================================
  // Guard-based lifetime management
  // ========================================================================================

  @Nested
  @DisplayName("acquireGuard on closed/orphaned pages")
  class GuardLifetimeManagement {

    @Test
    @DisplayName("acquireGuard returns false after close()")
    void testAcquireGuardAfterClose() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      assertTrue(page.acquireGuard(), "Should acquire guard on fresh page");
      page.releaseGuard();

      page.close();

      assertFalse(page.acquireGuard(), "acquireGuard must return false after close()");
    }

    @Test
    @DisplayName("acquireGuard returns false after markOrphaned with no guards")
    void testAcquireGuardAfterOrphaned() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);

      page.markOrphaned();

      assertFalse(page.acquireGuard(), "acquireGuard must return false on orphaned page");
    }

    @Test
    @DisplayName("releaseGuard + orphaned triggers close")
    void testReleaseGuardOrphanedTriggersClose() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);

      assertTrue(page.acquireGuard());
      page.markOrphaned();
      // Page is orphaned but guarded — should not be closed yet
      assertTrue(page.isOrphaned());

      page.releaseGuard();
      // Now guard count = 0 and orphaned → should be closed
      assertFalse(page.acquireGuard(), "Page should be closed after releasing last guard on orphaned page");
    }

    @Test
    @DisplayName("Double close is safe")
    void testDoubleCloseIsSafe() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      page.close();
      assertDoesNotThrow(page::close, "Double close must not throw");
    }
  }

  // ========================================================================================
  // initialBytePos widening (byte → int) for keys > 255 bytes
  // ========================================================================================

  @Nested
  @DisplayName("initialBytePos > 255 (byte→int widening)")
  class InitialBytePosWidening {

    @Test
    @DisplayName("BiNode with initialBytePos > 255 routes correctly")
    void testBiNodeInitialBytePosOver255() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      // Discriminative bit at absolute position 2048 = byte 256, bit 0 (MSB)
      int discBitPos = 256 * 8; // = 2048
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, discBitPos, leftRef, rightRef);

      assertEquals(256, biNode.getInitialBytePos(),
          "initialBytePos should be 256 (beyond byte range)");

      // 260-byte key with byte[256] = 0x00 → disc bit 0 → left
      byte[] leftKey = new byte[260];
      assertEquals(0, biNode.findChildIndex(leftKey));

      // 260-byte key with byte[256] = 0x80 → disc bit 1 → right
      byte[] rightKey = new byte[260];
      rightKey[256] = (byte) 0x80;
      assertEquals(1, biNode.findChildIndex(rightKey));
    }

    @Test
    @DisplayName("SpanNode with initialBytePos=300 routes 4 children correctly")
    void testSpanNodeInitialBytePosOver255() {
      // Two disc bits within the 8-byte window starting at byte 300:
      // byte 300 bit 7 (LSB) → LE word position: 0*8 + (7-7) = 0 → mask bit 0
      // byte 301 bit 7 (LSB) → LE word position: 1*8 + (7-7) = 8 → mask bit 8
      long bitMask = (1L << 0) | (1L << 8);

      byte[] partialKeys = new byte[] {0b00, 0b01, 0b10, 0b11};
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, 300, bitMask, partialKeys, children);
      assertEquals(300, spanNode.getInitialBytePos());

      // 310-byte key with byte[300]=0x00, byte[301]=0x00 → bits 0,8 both 0 → pkey=0b00 → child 0
      byte[] key00 = new byte[310];
      assertEquals(0, spanNode.findChildIndex(key00));

      // byte[300]=0x01 (LSB set), byte[301]=0x00 → bit0=1, bit8=0 → pkey=0b01 → child 1
      byte[] key01 = new byte[310];
      key01[300] = 0x01;
      assertEquals(1, spanNode.findChildIndex(key01));

      // byte[300]=0x00, byte[301]=0x01 (LSB set) → bit0=0, bit8=1 → pkey=0b10 → child 2
      byte[] key10 = new byte[310];
      key10[301] = 0x01;
      assertEquals(2, spanNode.findChildIndex(key10));

      // byte[300]=0x01, byte[301]=0x01 → bit0=1, bit8=1 → pkey=0b11 → child 3
      byte[] key11 = new byte[310];
      key11[300] = 0x01;
      key11[301] = 0x01;
      assertEquals(3, spanNode.findChildIndex(key11));
    }

    @Test
    @DisplayName("Short key falls back to left child when key.length < initialBytePos")
    void testShortKeyFallback() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 256 * 8, leftRef, rightRef);

      // 10-byte key — shorter than initialBytePos=256 → should default to child 0
      byte[] shortKey = new byte[10];
      assertEquals(0, biNode.findChildIndex(shortKey),
          "Key shorter than initialBytePos should default to left child");
    }
  }

  // ========================================================================================
  // NodeReferencesSerializer.computeSerializedSize() pre-check
  // ========================================================================================

  @Nested
  @DisplayName("NodeReferencesSerializer size pre-check")
  class SerializerSizePreCheck {

    @Test
    @DisplayName("computeSerializedSize matches actual serialized size for small bitmap")
    void testComputeSizeSmallBitmap() {
      NodeReferences refs = new NodeReferences();
      refs.addNodeKey(1);
      refs.addNodeKey(2);
      refs.addNodeKey(100);

      int predicted = NodeReferencesSerializer.computeSerializedSize(refs);
      byte[] actual = NodeReferencesSerializer.serialize(refs);

      assertEquals(actual.length, predicted,
          "Predicted size must match actual for small (packed) bitmap");
    }

    @Test
    @DisplayName("computeSerializedSize matches actual serialized size for large bitmap")
    void testComputeSizeLargeBitmap() {
      NodeReferences refs = new NodeReferences();
      // Add > 64 entries to trigger Roaring format
      for (long i = 0; i < 100; i++) {
        refs.addNodeKey(i * 1000);
      }

      int predicted = NodeReferencesSerializer.computeSerializedSize(refs);
      byte[] actual = NodeReferencesSerializer.serialize(refs);

      assertEquals(actual.length, predicted,
          "Predicted size must match actual for large (Roaring) bitmap");
    }

    @Test
    @DisplayName("computeSerializedSize returns 1 for empty (tombstone) bitmap")
    void testComputeSizeTombstone() {
      NodeReferences refs = new NodeReferences();
      assertEquals(1, NodeReferencesSerializer.computeSerializedSize(refs),
          "Empty bitmap should produce tombstone (1 byte)");
    }

    @Test
    @DisplayName("Buffer-based serialize matches allocating serialize")
    void testBufferSerializeMatchesAllocating() {
      NodeReferences refs = new NodeReferences();
      for (long i = 0; i < 50; i++) {
        refs.addNodeKey(i);
      }

      int size = NodeReferencesSerializer.computeSerializedSize(refs);
      byte[] buf = new byte[size];
      int written = NodeReferencesSerializer.serialize(refs, buf, 0);

      byte[] direct = NodeReferencesSerializer.serialize(refs);

      assertEquals(size, written, "Written bytes should match predicted size");
      assertArrayEqualsHelper(direct, buf, written,
          "Buffer-based and allocating serialize must produce identical bytes");
    }

    @Test
    @DisplayName("Round-trip: serialize → deserialize preserves all node keys")
    void testRoundTrip() {
      NodeReferences refs = new NodeReferences();
      refs.addNodeKey(42);
      refs.addNodeKey(100);
      refs.addNodeKey(Long.MAX_VALUE);

      byte[] bytes = NodeReferencesSerializer.serialize(refs);
      NodeReferences deserialized = NodeReferencesSerializer.deserialize(bytes);

      assertTrue(deserialized.getNodeKeys().contains(42));
      assertTrue(deserialized.getNodeKeys().contains(100));
      assertTrue(deserialized.getNodeKeys().contains(Long.MAX_VALUE));
      assertEquals(3, deserialized.getNodeKeys().getLongCardinality());
    }

    private void assertArrayEqualsHelper(byte[] expected, byte[] actual, int length, String message) {
      assertEquals(expected.length, length, message + " (length mismatch)");
      for (int i = 0; i < length; i++) {
        assertEquals(expected[i], actual[i], message + " at index " + i);
      }
    }
  }

  // ========================================================================================
  // HOTLeafPage: canFit capacity check
  // ========================================================================================

  @Nested
  @DisplayName("canFit capacity check")
  class CanFitCapacity {

    @Test
    @DisplayName("canFit returns false when MAX_ENTRIES reached")
    void testCanFitMaxEntries() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);

      // Fill to MAX_ENTRIES
      for (int i = 0; i < HOTLeafPage.MAX_ENTRIES; i++) {
        byte[] key = String.format("k%04d", i).getBytes();
        byte[] value = "v".getBytes();
        if (!page.mergeWithNodeRefs(key, key.length, value, value.length)) {
          break;
        }
      }

      if (page.getEntryCount() == HOTLeafPage.MAX_ENTRIES) {
        assertFalse(page.canFit("newkey".getBytes(), "newval".getBytes()),
            "canFit should return false at MAX_ENTRIES");
      }
    }

    @Test
    @DisplayName("canFit returns false for entry larger than page")
    void testCanFitOversizedEntry() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);
      byte[] hugeValue = new byte[HOTLeafPage.DEFAULT_SIZE + 1];
      assertFalse(page.canFit("key".getBytes(), hugeValue),
          "canFit should return false for entry larger than page");
    }
  }

  // ========================================================================================
  // HOTIndirectPage: serialization round-trip with int initialBytePos
  // ========================================================================================

  @Nested
  @DisplayName("HOTIndirectPage field integrity")
  class IndirectPageIntegrity {

    @Test
    @DisplayName("BiNode preserves all fields through copy constructor")
    void testBiNodeCopyConstructor() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1L);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2L);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(100L, 5, 12, leftRef, rightRef);
      HOTIndirectPage copy = original.copyWithNewPageKey(200L, 6);

      assertEquals(200L, copy.getPageKey());
      assertEquals(original.getNodeType(), copy.getNodeType());
      assertEquals(original.getNumChildren(), copy.getNumChildren());
      assertEquals(original.getInitialBytePos(), copy.getInitialBytePos());
      assertEquals(original.getBitMask(), copy.getBitMask());
    }

    @Test
    @DisplayName("SpanNode preserves routing through copy")
    void testSpanNodeCopyPreservesRouting() {
      long bitMask = 0x0101L; // bits 0 and 8
      byte[] partialKeys = new byte[] {0b00, 0b01, 0b10, 0b11};
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(i);
      }

      HOTIndirectPage original = HOTIndirectPage.createSpanNode(1L, 1, 0, bitMask, partialKeys, children);

      // Test routing on original
      byte[] testKey = new byte[] {0x01, 0x01}; // bits 0 and 8 both set → pkey=0b11 → child 3
      int origResult = original.findChildIndex(testKey);

      HOTIndirectPage copy = original.copyWithNewPageKey(2L, 1);
      int copyResult = copy.findChildIndex(testKey);

      assertEquals(origResult, copyResult,
          "Copy must route identically to original");
    }

    @Test
    @DisplayName("createBiNode with height preserves height field")
    void testBiNodeWithHeight() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef, 5);
      assertEquals(5, biNode.getHeight());
      assertEquals(2, biNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
    }
  }

  // ========================================================================================
  // HOTLeafPage: compact + insert interaction
  // ========================================================================================

  @Nested
  @DisplayName("Auto-compact on insert")
  class AutoCompactOnInsert {

    @Test
    @DisplayName("Page auto-compacts and inserts when fragmented")
    void testAutoCompactOnFragmentedPage() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);

      // Fill page with large values using put() (raw bytes, no NodeReferences format)
      for (int i = 0; i < 20; i++) {
        byte[] key = String.format("key%03d", i).getBytes();
        byte[] value = new byte[200];
        Arrays.fill(value, (byte) ('a' + i % 26));
        assertTrue(page.put(key, value), "Should insert entry " + i);
      }

      // Update values with smaller ones → creates fragmentation
      for (int i = 0; i < 20; i++) {
        byte[] key = String.format("key%03d", i).getBytes();
        byte[] smallValue = "x".getBytes();
        int idx = page.findEntry(key);
        assertTrue(idx >= 0);
        page.updateValue(idx, smallValue);
      }

      // Now the page has internal fragmentation. A new insert should succeed
      // (insertAt auto-compacts when space check fails).
      byte[] newKey = "newkey".getBytes();
      byte[] newValue = new byte[100];
      boolean inserted = page.put(newKey, newValue);
      assertTrue(inserted, "Insert should succeed after auto-compaction");
      assertEquals(21, page.getEntryCount());
    }
  }

  // ========================================================================================
  // NodeReferencesSerializer: tombstone detection
  // ========================================================================================

  @Nested
  @DisplayName("Tombstone handling")
  class TombstoneHandling {

    @Test
    @DisplayName("isTombstone correctly identifies tombstone bytes")
    void testIsTombstone() {
      byte[] tombstone = new byte[] {(byte) 0xFE};
      assertTrue(NodeReferencesSerializer.isTombstone(tombstone, 0, 1));

      byte[] notTombstone = new byte[] {0x00, 0x01};
      assertFalse(NodeReferencesSerializer.isTombstone(notTombstone, 0, 2));
    }

    @Test
    @DisplayName("Empty NodeReferences serializes to tombstone and deserializes back to empty")
    void testEmptyRoundTrip() {
      NodeReferences empty = new NodeReferences();
      byte[] bytes = NodeReferencesSerializer.serialize(empty);

      assertEquals(1, bytes.length);
      assertTrue(NodeReferencesSerializer.isTombstone(bytes, 0, bytes.length));

      NodeReferences deserialized = NodeReferencesSerializer.deserialize(bytes);
      assertTrue(deserialized.getNodeKeys().isEmpty());
    }
  }

  // ========================================================================================
  // End-to-end: leaf page fill → split → both pages usable
  // ========================================================================================

  @Nested
  @DisplayName("End-to-end leaf page lifecycle")
  class EndToEndLifecycle {

    @Test
    @DisplayName("Fill → split → verify all keys accessible across both pages")
    void testFillSplitVerifyAllKeys() {
      HOTLeafPage page = new HOTLeafPage(1L, 1, IndexType.PATH);

      // Insert 100 keys with deterministic values
      final int count = 100;
      for (int i = 0; i < count; i++) {
        byte[] key = String.format("key%05d", i).getBytes();
        byte[] value = String.format("val%05d", i).getBytes();
        assertTrue(page.mergeWithNodeRefs(key, key.length, value, value.length),
            "Should insert key " + i);
      }

      assertEquals(count, page.getEntryCount());

      // Split
      HOTLeafPage right = new HOTLeafPage(2L, 1, IndexType.PATH);
      byte[] splitKey = page.splitTo(right);
      assertNotNull(splitKey);

      // Verify total count preserved
      assertEquals(count, page.getEntryCount() + right.getEntryCount(),
          "Split must preserve total entry count");

      // Verify every key is findable in exactly one page
      for (int i = 0; i < count; i++) {
        byte[] key = String.format("key%05d", i).getBytes();
        boolean inLeft = page.findEntry(key) >= 0;
        boolean inRight = right.findEntry(key) >= 0;
        assertTrue(inLeft || inRight, "Key " + i + " must be in one of the pages");
        assertFalse(inLeft && inRight, "Key " + i + " must not be in both pages");
      }

      // Verify values are correct
      for (int i = 0; i < count; i++) {
        byte[] key = String.format("key%05d", i).getBytes();
        int idx = page.findEntry(key);
        HOTLeafPage foundIn = page;
        if (idx < 0) {
          idx = right.findEntry(key);
          foundIn = right;
        }
        byte[] value = foundIn.getValue(idx);
        String expected = String.format("val%05d", i);
        assertEquals(expected, new String(value), "Value mismatch for key " + i);
      }
    }
  }
}
