/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Round-trip tests for {@link HOTTrieWriter#collectAncestorDiscBits} (Phase 2 prep).
 *
 * <p>The helper decodes the absolute disc-bit positions captured by every indirect on a
 * descent path. Two layouts must be supported: SingleMask (≤8 contiguous key bytes,
 * one 64-bit PEXT mask) and MultiMask (>8 bytes via per-byte extraction positions).
 *
 * <p>Each test builds synthetic HOTIndirectPage(s) with known disc bit positions, runs
 * the helper, and verifies that for every returned absolute bit position {@code β}:
 * <ul>
 *   <li>{@code β} matches one of the planted positions (no spurious bits),</li>
 *   <li>{@code DiscriminativeBitComputer.isBitSet(probeKey, β)} agrees with the bit's
 *       known value at that position in the constructed key.</li>
 * </ul>
 *
 * <p>This is the per-design-doc Phase 1 round-trip gate: bit-position decode matches
 * {@link DiscriminativeBitComputer#isBitSet} semantics for synthetic disc-bit
 * configurations.
 *
 * <p>The test exercises both convention edges:
 * <ul>
 *   <li>BE word: byte at window-offset {@code bo} → long bits [(7-bo)*8 .. (7-bo)*8+7];</li>
 *   <li>MSB-numbered bit-in-byte: bit 0 = MSB, bit 7 = LSB.</li>
 * </ul>
 */
@DisplayName("collectAncestorDiscBits round-trip (Phase 2 prep)")
final class CollectAncestorDiscBitsTest {

  /**
   * Encode an absolute MSB-first bit position {@code (bytePos, bitInByte)} into a SingleMask
   * 64-bit mask given a window starting at {@code initialBytePos}.
   *
   * <p>BE word: long-bit b corresponds to byte (initialBytePos + 7 - b/8), bit-in-byte
   * (7 - b%8). Inverting: long-bit b = (7 - byteOffsetInWindow)*8 + (7 - bitInByte) =
   * 63 - (byteOffsetInWindow*8 + bitInByte).
   */
  private static long bitMaskFor(int initialBytePos, int absBytePos, int bitInByte) {
    final int bo = absBytePos - initialBytePos;
    if (bo < 0 || bo >= 8) {
      throw new IllegalArgumentException(
          "byte " + absBytePos + " not in window starting at " + initialBytePos);
    }
    final int longBit = (7 - bo) * 8 + (7 - bitInByte);
    return 1L << longBit;
  }

  /**
   * Build a SingleMask SpanNode with the given disc-bit positions baked into its mask.
   * The node has 2 dummy children (BiNode-shape) — sufficient to exercise the helper
   * which only reads the mask metadata.
   */
  private static HOTIndirectPage makeSingleMaskNode(int initialBytePos, int[] absBitPositions) {
    long mask = 0L;
    for (final int abs : absBitPositions) {
      final int bytePos = abs / 8;
      final int bitInByte = abs % 8;
      mask |= bitMaskFor(initialBytePos, bytePos, bitInByte);
    }
    final int numDiscBits = Long.bitCount(mask);
    final int[] partials;
    final PageReference[] children;
    if (numDiscBits == 1) {
      // BiNode shape (2 children, 1 disc bit)
      final PageReference left = new PageReference();
      final PageReference right = new PageReference();
      left.setKey(1001L);
      right.setKey(1002L);
      // Compute discriminativeBitPos from the single set long-bit:
      final int b = Long.numberOfTrailingZeros(mask);
      final int bo = 7 - (b / 8);
      final int bi = 7 - (b % 8);
      final int discBitPos = (initialBytePos + bo) * 8 + bi;
      return HOTIndirectPage.createBiNode(1L, 1, discBitPos, left, right, 1);
    }
    // SpanNode with up to 16 children — pick (1 << numDiscBits) limited to 16:
    final int childCount = Math.min(1 << numDiscBits, 16);
    partials = new int[childCount];
    children = new PageReference[childCount];
    for (int i = 0; i < childCount; i++) {
      partials[i] = i;
      children[i] = new PageReference();
      children[i].setKey(2000L + i);
    }
    return HOTIndirectPage.createSpanNode(1L, 1, initialBytePos, mask, partials, children);
  }

  /** Build a MultiMask SpanNode with disc-bits at arbitrary byte positions (non-contiguous). */
  private static HOTIndirectPage makeMultiMaskNode(int[] absBitPositions) {
    // Group disc bits by byte position, in ascending byte order.
    // extractionPositions[i] = byte to gather; extractionMasks per chunk (8 bytes/chunk).
    // Within each gathered byte, set the corresponding bit in the chunk's mask.
    final java.util.TreeMap<Integer, Integer> byteToBitMask = new java.util.TreeMap<>();
    for (final int abs : absBitPositions) {
      final int bytePos = abs / 8;
      final int bitInByte = abs % 8;
      // bit-in-byte mask: bitInByte=0 -> 0x80, ..., bitInByte=7 -> 0x01
      final int bitMaskByte = 0x80 >>> bitInByte;
      byteToBitMask.merge(bytePos, bitMaskByte, (a, b) -> a | b);
    }
    final int numExtractionBytes = byteToBitMask.size();
    final byte[] extractionPositions = new byte[numExtractionBytes];
    final int numChunks = (numExtractionBytes + 7) / 8;
    final long[] extractionMasks = new long[Math.max(1, numChunks)];
    int idx = 0;
    for (final var e : byteToBitMask.entrySet()) {
      extractionPositions[idx] = (byte) (e.getKey() & 0xFF);
      final int chunkIdx = idx / 8;
      final int byteOffsetInChunk = idx % 8;
      // BE chunk packing: extraction-byte at chunk-offset o → long bits [(7-o)*8..(7-o)*8+7]
      extractionMasks[chunkIdx] |= ((long) (e.getValue() & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      idx++;
    }

    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);

    // Pick (1 << totalBits) children up to 16.
    final int childCount = Math.min(Math.max(2, 1 << Math.min(totalBits, 4)), 16);
    final int[] partials = new int[childCount];
    final PageReference[] children = new PageReference[childCount];
    for (int i = 0; i < childCount; i++) {
      partials[i] = i;
      children[i] = new PageReference();
      children[i].setKey(3000L + i);
    }
    final short msb = (short) Arrays.stream(absBitPositions).min().orElse(0);
    return HOTIndirectPage.createSpanNodeMultiMask(2L, 1,
        extractionPositions, extractionMasks, numExtractionBytes, partials, children, 1, msb);
  }

  /**
   * Verify that returned bits match expectations for a SingleMask layout. Window starts at
   * byte 0; planted bits at known positions in bytes [0..7].
   */
  @Test
  @DisplayName("SingleMask: round-trip decode of planted absolute bit positions")
  void singleMaskRoundTrip() {
    // Plant disc bits at positions: byte 0 bit 3 (=3), byte 0 bit 7 (=7),
    // byte 4 bit 0 (=32), byte 7 bit 5 (=61).
    final int[] planted = {3, 7, 32, 61};
    final HOTIndirectPage node = makeSingleMaskNode(0, planted);
    final HOTIndirectPage[] path = {node};
    final HOTTrieWriter w = new HOTTrieWriter();
    final int[] decoded = w.collectAncestorDiscBits(path, 1);

    final int[] expected = planted.clone();
    Arrays.sort(expected);
    assertArrayEquals(expected, decoded,
        "decoded bits must match planted bits exactly (sorted ascending)");

    // Also verify with isBitSet semantics: build a key with KNOWN bit values at planted
    // positions, then verify decoded β matches.
    final byte[] key = new byte[8];
    // Set each planted bit to 1 in the key:
    for (final int abs : planted) {
      key[abs / 8] |= (byte) (0x80 >>> (abs % 8));
    }
    for (final int abs : decoded) {
      assertTrue(DiscriminativeBitComputer.isBitSet(key, abs),
          "isBitSet(key, " + abs + ") must agree with planted set bit");
    }
    // Sanity: every non-planted bit in those bytes should be 0.
    final Set<Integer> plantedSet = new HashSet<>();
    for (final int p : planted) plantedSet.add(p);
    for (int absBit = 0; absBit < 64; absBit++) {
      if (!plantedSet.contains(absBit)) {
        assertEquals(false, DiscriminativeBitComputer.isBitSet(key, absBit),
            "non-planted bit " + absBit + " must be 0");
      }
    }
  }

  /**
   * SingleMask with non-zero {@code initialBytePos}: window starts mid-key. Verifies that
   * the helper correctly decodes the absolute byte position even when the window doesn't
   * start at byte 0.
   */
  @Test
  @DisplayName("SingleMask: round-trip with shifted window (initialBytePos != 0)")
  void singleMaskShiftedWindow() {
    final int initialBytePos = 12; // window covers bytes 12..19
    final int[] planted = {12 * 8 + 1, 12 * 8 + 7, 15 * 8 + 0, 19 * 8 + 4};
    final HOTIndirectPage node = makeSingleMaskNode(initialBytePos, planted);
    final HOTIndirectPage[] path = {node};
    final HOTTrieWriter w = new HOTTrieWriter();
    final int[] decoded = w.collectAncestorDiscBits(path, 1);

    final int[] expected = planted.clone();
    Arrays.sort(expected);
    assertArrayEquals(expected, decoded);
  }

  /**
   * MultiMask: disc bits at non-contiguous byte positions spanning >8 bytes. Forces the
   * MultiMask code path in the helper.
   */
  @Test
  @DisplayName("MultiMask: round-trip decode of disc bits across non-contiguous bytes")
  void multiMaskRoundTrip() {
    // Plant disc bits at positions in bytes 0, 5, 10, 15, 22 — spanning >8 bytes
    // forces MultiMask layout.
    final int[] planted = {2, 5 * 8 + 6, 10 * 8 + 1, 15 * 8 + 7, 22 * 8 + 0};
    final HOTIndirectPage node = makeMultiMaskNode(planted);
    assertEquals(HOTIndirectPage.LayoutType.MULTI_MASK, node.getLayoutType(),
        "must be MultiMask for non-contiguous byte positions");
    final HOTIndirectPage[] path = {node};
    final HOTTrieWriter w = new HOTTrieWriter();
    final int[] decoded = w.collectAncestorDiscBits(path, 1);

    final int[] expected = planted.clone();
    Arrays.sort(expected);
    assertArrayEquals(expected, decoded,
        "MultiMask decode must match planted bits");

    // Verify isBitSet semantics on a probe key with planted bits set.
    final byte[] key = new byte[24];
    for (final int abs : planted) {
      key[abs / 8] |= (byte) (0x80 >>> (abs % 8));
    }
    for (final int abs : decoded) {
      assertTrue(DiscriminativeBitComputer.isBitSet(key, abs),
          "MultiMask isBitSet(key, " + abs + ") must agree with planted set bit");
    }
  }

  /**
   * Multi-level path: union of disc bits across multiple ancestors (typical descent).
   * Verifies dedup and ascending sort across multiple indirects.
   */
  @Test
  @DisplayName("Path: union+dedup of disc bits across multiple ancestors")
  void multiLevelPathUnion() {
    // Ancestor 0: SingleMask with bits {3, 32}
    // Ancestor 1: SingleMask with bits {32, 50}  — bit 32 overlaps
    // Ancestor 2: SingleMask with bits {12, 60}
    // Expected union (sorted): {3, 12, 32, 50, 60}
    final HOTIndirectPage a0 = makeSingleMaskNode(0, new int[] {3, 32});
    final HOTIndirectPage a1 = makeSingleMaskNode(0, new int[] {32, 50});
    final HOTIndirectPage a2 = makeSingleMaskNode(0, new int[] {12, 60});
    final HOTIndirectPage[] path = {a0, a1, a2};

    final HOTTrieWriter w = new HOTTrieWriter();
    final int[] decoded = w.collectAncestorDiscBits(path, 3);

    assertArrayEquals(new int[] {3, 12, 32, 50, 60}, decoded,
        "multi-level decode must union+dedup+sort across ancestors");
  }

  /** Empty path yields empty array. */
  @Test
  @DisplayName("Empty path: returns empty array")
  void emptyPath() {
    final HOTTrieWriter w = new HOTTrieWriter();
    assertNotNull(w.collectAncestorDiscBits(new HOTIndirectPage[0], 0));
    assertEquals(0, w.collectAncestorDiscBits(new HOTIndirectPage[0], 0).length);
  }
}
