/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTTrieWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import io.sirix.utils.OS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for HOT Option B Phase 5 helpers (constancy-aware insertion).
 *
 * <p>Tests are unit-level: construct HOTLeafPage + HOTIndirectPage directly in memory,
 * call the helper, assert the result. No full database round-trip needed.
 */
@DisplayName("HOT Option B Phase 5")
class HOTOptionBPhase5Test {

  @BeforeAll
  static void initAllocator() {
    if (!OS.isWindows()) {
      LinuxMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024);
    } else {
      WindowsMemorySegmentAllocator.getInstance().init(64 * 1024 * 1024);
    }
  }

  @Nested
  @DisplayName("Phase 7a — owned-bits metadata")
  class OwnedBitsMetadata {

    private static byte[] hexBytes(String hex) {
      final int len = hex.length() / 2;
      final byte[] b = new byte[len];
      for (int i = 0; i < len; i++) {
        b[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
      }
      return b;
    }

    @Test
    @DisplayName("default leaf has empty owned bits")
    void defaultEmpty() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      assertEquals(0, leaf.getAncestorOwnedBits().length);
      assertEquals(0, leaf.getAncestorOwnedValues().length);
    }

    @Test
    @DisplayName("setAncestorOwnedBits stores + retrieves")
    void setAndGet() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      leaf.setAncestorOwnedBits(new int[]{ 1, 5, 80 }, new byte[]{ 0, 1, 1 });
      assertArrayEquals(new int[]{ 1, 5, 80 }, leaf.getAncestorOwnedBits());
      assertArrayEquals(new byte[]{ 0, 1, 1 }, leaf.getAncestorOwnedValues());
    }

    @Test
    @DisplayName("checkOwnedBitsAgainstKey returns -1 when key matches all owned values")
    void checkMatching() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      // Owned: bit 0 = 1 (MSB of byte 0), bit 7 = 1 (LSB of byte 0).
      leaf.setAncestorOwnedBits(new int[]{ 0, 7 }, new byte[]{ 1, 1 });
      // Key 0x81 = 10000001 → bit 0 = 1, bit 7 = 1. Matches.
      assertEquals(-1, leaf.checkOwnedBitsAgainstKey(hexBytes("81")));
    }

    @Test
    @DisplayName("checkOwnedBitsAgainstKey returns first offending bit")
    void checkOffending() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      leaf.setAncestorOwnedBits(new int[]{ 0, 7 }, new byte[]{ 1, 1 });
      // Key 0x80 = 10000000 → bit 0 = 1, bit 7 = 0. Bit 7 offends.
      assertEquals(7, leaf.checkOwnedBitsAgainstKey(hexBytes("80")));
    }

    @Test
    @DisplayName("setAncestorOwnedBits rejects unsorted input")
    void rejectsUnsorted() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      assertThrows(IllegalArgumentException.class, () ->
          leaf.setAncestorOwnedBits(new int[]{ 5, 2 }, new byte[]{ 0, 1 }));
    }

    @Test
    @DisplayName("setAncestorOwnedBits rejects mismatched lengths")
    void rejectsMismatchedLengths() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      assertThrows(IllegalArgumentException.class, () ->
          leaf.setAncestorOwnedBits(new int[]{ 5 }, new byte[]{ 0, 1 }));
    }

    @Test
    @DisplayName("checkOwnedBitsAgainstKey is no-op for empty owned bits")
    void emptyOwnedBitsAcceptsAll() {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      assertEquals(-1, leaf.checkOwnedBitsAgainstKey(hexBytes("80")));
      assertEquals(-1, leaf.checkOwnedBitsAgainstKey(hexBytes("ff")));
    }
  }

  @Nested
  @DisplayName("detectAllConstancyBreaksOnInsert")
  class DetectAllConstancyBreaks {

    /**
     * Build a leaf with the given hex-encoded keys (e.g. "80a0", "80b0"). Each key gets
     * a value equal to its key bytes (= deterministic placeholder).
     */
    private static HOTLeafPage leafWithKeys(String... hexKeys) {
      final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
      for (final String hex : hexKeys) {
        final byte[] key = hexBytes(hex);
        assertTrue(leaf.mergeWithNodeRefs(key, key.length, key, key.length),
            "Failed to insert key " + hex);
      }
      return leaf;
    }

    private static byte[] hexBytes(String hex) {
      final int len = hex.length() / 2;
      final byte[] b = new byte[len];
      for (int i = 0; i < len; i++) {
        b[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
      }
      return b;
    }

    /**
     * Build a single-disc-bit BiNode indirect with the given disc bit (absolute bit
     * position) and two children. Children's refs are dummies — we only need the
     * mask for testing detector.
     */
    private static HOTIndirectPage biNodeWithDiscBit(int absBit) {
      // Two minimal page references — they're not actually descended.
      final io.sirix.page.PageReference left = new io.sirix.page.PageReference();
      left.setKey(100L);
      final io.sirix.page.PageReference right = new io.sirix.page.PageReference();
      right.setKey(101L);
      return HOTIndirectPage.createBiNode(99L, 1, absBit, left, right, 1);
    }

    @Test
    @DisplayName("no constancy break when new key matches leaf's β-value")
    void noBreakSameBitValue() {
      // Leaf keys all have byte 0 = 0x80 (bit 0 = 1).
      final HOTLeafPage leaf = leafWithKeys("80a0", "80b0", "80c0");
      // BiNode with disc bit at byte 0 bit 1 (absBit 1). All leaf keys' byte 0 = 0x80
      // = 10000000 → bit 1 from MSB-first = 0. Constant.
      final HOTIndirectPage parent = biNodeWithDiscBit(1);
      // New key byte 0 = 0x80 also has bit 1 = 0. Same as leaf. No break.
      final byte[] newKey = hexBytes("80d0");

      final HOTTrieWriter writer = new HOTTrieWriter(() -> 1L);
      final int[] result = writer.detectAllConstancyBreaksOnInsert(
          new HOTIndirectPage[] { parent }, 1, leaf, newKey);
      assertArrayEquals(new int[0], result, "no break expected");
    }

    @Test
    @DisplayName("one constancy break when new key disagrees with leaf's β-value")
    void oneBreakDisagreement() {
      // Leaf keys all have byte 0 = 0x80 → bit 1 = 0 (constant).
      final HOTLeafPage leaf = leafWithKeys("80a0", "80b0", "80c0");
      final HOTIndirectPage parent = biNodeWithDiscBit(1);
      // New key byte 0 = 0xc0 (= 11000000) has bit 1 = 1. Disagrees with leaf's 0.
      final byte[] newKey = hexBytes("c0d0");

      final HOTTrieWriter writer = new HOTTrieWriter(() -> 1L);
      final int[] result = writer.detectAllConstancyBreaksOnInsert(
          new HOTIndirectPage[] { parent }, 1, leaf, newKey);
      assertArrayEquals(new int[] { 1 }, result, "one break at bit 1 expected");
    }

    @Test
    @DisplayName("β-mixed leaf is reported as offending (Phase 5 will partition)")
    void mixedLeafReported() {
      // Leaf is already β-mixed at bit 1: 0x80 (bit 1 = 0) and 0xc0 (bit 1 = 1).
      final HOTLeafPage leaf = leafWithKeys("80a0", "c0b0");
      final HOTIndirectPage parent = biNodeWithDiscBit(1);
      final byte[] newKey = hexBytes("80d0");

      final HOTTrieWriter writer = new HOTTrieWriter(() -> 1L);
      final int[] result = writer.detectAllConstancyBreaksOnInsert(
          new HOTIndirectPage[] { parent }, 1, leaf, newKey);
      assertArrayEquals(new int[] { 1 }, result, "β-mixed leaf reported as offending");
    }

    @Test
    @DisplayName("empty path returns empty array")
    void emptyPath() {
      final HOTLeafPage leaf = leafWithKeys("80a0");
      final byte[] newKey = hexBytes("80b0");
      final HOTTrieWriter writer = new HOTTrieWriter(() -> 1L);
      final int[] result = writer.detectAllConstancyBreaksOnInsert(
          new HOTIndirectPage[0], 0, leaf, newKey);
      assertEquals(0, result.length);
    }

    @Test
    @DisplayName("null key returns empty array")
    void nullKey() {
      final HOTLeafPage leaf = leafWithKeys("80a0");
      final HOTIndirectPage parent = biNodeWithDiscBit(1);
      final HOTTrieWriter writer = new HOTTrieWriter(() -> 1L);
      final int[] result = writer.detectAllConstancyBreaksOnInsert(
          new HOTIndirectPage[] { parent }, 1, leaf, null);
      assertEquals(0, result.length);
    }
  }
}
