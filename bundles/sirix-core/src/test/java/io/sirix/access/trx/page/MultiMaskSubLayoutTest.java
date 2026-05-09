/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Round-trip tests for the Phase 4b.0 helpers in {@link HOTTrieWriter}:
 * {@link HOTTrieWriter#computeRelevantBitsFromPartials} and
 * {@link HOTTrieWriter#extractMultiMaskSubset} together with the existing
 * {@link HOTTrieWriter#computePartialKeyMultiMaskDirect}.
 *
 * <p>The Phase 4b.2 MultiMask-parent path in {@link HOTTrieWriter#buildCompressedHalf}
 * relies on the identity:
 *
 * <pre>{@code
 * computePartialKeyMultiMaskDirect(K, newLayout) ==
 *   (int) Long.expand(Long.compress(oldPartial, relevant), allOnesMask(newLayout.totalBits()))
 * }</pre>
 *
 * where {@code oldPartial = computePartialKeyMultiMaskDirect(K, parent.layout)} and
 * {@code newLayout = extractMultiMaskSubset(parent, relevant)}. This identity is the
 * Java analog of the C++ {@code DiscriminativeBitsRepresentation.extract(relevantBits)}
 * template + {@code Long.compress}/{@code Long.expand} repositioning. If this identity
 * holds, repositioning existing children's partials in a sub-MultiMask layout via the
 * compress→expand round-trip yields the same value as encoding their first key
 * directly under the sub-layout — a necessary precondition for routing soundness.
 */
@DisplayName("MultiMaskSubLayout round-trip (Phase 4b.0)")
final class MultiMaskSubLayoutTest {

  /** Build a single-chunk MultiMask layout from a list of (bytePos, bitInByte) pairs. */
  private static io.sirix.page.HOTIndirectPage buildSingleChunkMultiMask(
      int[][] discBits, int[] partialKeys, io.sirix.page.PageReference[] children) {
    final byte[] extractionPositions = new byte[discBits.length];
    final long[] extractionMasks = new long[1];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < discBits.length; i++) {
      final int bytePos = discBits[i][0];
      final int bitInByte = discBits[i][1];
      extractionPositions[i] = (byte) bytePos;
      // BE chunk packing: extraction-byte at chunk-offset i → long bits ((7-i)*8)..((7-i)*8+7).
      // Within byte: MSB-first bit b → byte bit (7-b).
      final int byteBit = 7 - bitInByte;
      extractionMasks[0] |= (1L << ((7 - i) * 8 + byteBit));
      final int absBitPos = bytePos * 8 + bitInByte;
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }
    return io.sirix.page.HOTIndirectPage.createMultiNodeMultiMask(
        /*pageKey=*/ 1L, /*revision=*/ 0,
        extractionPositions, extractionMasks, discBits.length,
        partialKeys, children, /*height=*/ 1, msbIndex);
  }

  /** Allocate a placeholder PageReference for a leaf (no key needed for the helpers). */
  private static io.sirix.page.PageReference dummyChild(long key) {
    final io.sirix.page.PageReference ref = new io.sirix.page.PageReference();
    ref.setKey(key);
    return ref;
  }

  /**
   * Verify the round-trip identity for ONE (parent, relevant, key) tuple. If it holds,
   * the Phase 4b.2 MultiMask repositioning is provably correct for that tuple.
   */
  private static void assertRoundTrip(io.sirix.page.HOTIndirectPage parent, int relevant,
      byte[] key) {
    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, relevant);
    assertNotNull(sub, "extractMultiMaskSubset must return non-null for MultiMask parent");

    if (sub.totalBits() == 0) return; // degenerate case — nothing to verify

    final int oldPartial = HOTTrieWriter.computePartialKeyMultiMaskDirect(key,
        parent.getExtractionPositions(), parent.getExtractionMasks(),
        parent.getExtractionPositions().length);
    final int newPartialDirect = HOTTrieWriter.computePartialKeyMultiMaskDirect(key,
        sub.extractionPositions(), sub.extractionMasks(), sub.numExtractionBytes());

    final long allOnes = (1L << sub.totalBits()) - 1L;
    final int newPartialFromOld = (int) Long.expand(
        Long.compress(Integer.toUnsignedLong(oldPartial), Integer.toUnsignedLong(relevant)),
        allOnes);

    assertEquals(newPartialDirect, newPartialFromOld,
        () -> "Round-trip mismatch: oldPartial=" + Integer.toBinaryString(oldPartial)
            + " relevant=" + Integer.toBinaryString(relevant)
            + " newDirect=" + Integer.toBinaryString(newPartialDirect)
            + " newFromOld=" + Integer.toBinaryString(newPartialFromOld));
  }

  @Test
  @DisplayName("returns null for SingleMask parent")
  void rejectsSingleMaskParent() {
    final io.sirix.page.PageReference left = dummyChild(10L);
    final io.sirix.page.PageReference right = dummyChild(11L);
    final io.sirix.page.HOTIndirectPage spanNode = io.sirix.page.HOTIndirectPage.createSpanNode(
        /*pageKey=*/ 1L, /*revision=*/ 0, /*initialBytePos=*/ 0, /*bitMask=*/ 0b1100L,
        new int[]{0, 1}, new io.sirix.page.PageReference[]{left, right});
    assertNull(HOTTrieWriter.extractMultiMaskSubset(spanNode, 0b1));
  }

  @Test
  @DisplayName("identity: relevant = all bits → newLayout == parent layout")
  void identityRelevantAllBits() {
    // Parent: bits at (byte 10, bit 1 MSB-first) and (byte 12, bit 3 MSB-first) — 2 disc bits total.
    final int[][] discBits = {{10, 1}, {12, 3}};
    // Two children with partial keys 0b00 and 0b11 (4 children would saturate; 2 is enough).
    final int[] partialKeys = {0b00, 0b11};
    final io.sirix.page.PageReference[] children = {dummyChild(100L), dummyChild(101L)};
    final io.sirix.page.HOTIndirectPage parent = buildSingleChunkMultiMask(discBits, partialKeys, children);

    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, 0b11);
    assertNotNull(sub);
    assertEquals(2, sub.numExtractionBytes());
    assertEquals(2, sub.totalBits());

    // Probe several keys to verify the round-trip identity holds.
    final byte[] keyA = new byte[16];
    keyA[10] = (byte) 0b00000000;
    keyA[12] = (byte) 0b00000000;
    assertRoundTrip(parent, 0b11, keyA);

    final byte[] keyB = new byte[16];
    keyB[10] = (byte) 0b01000000; // sets bit 1 MSB-first in byte 10 — disc bit @ output pos 1
    keyB[12] = (byte) 0b00010000; // sets bit 3 MSB-first in byte 12 — disc bit @ output pos 0
    assertRoundTrip(parent, 0b11, keyB);

    final byte[] keyC = new byte[16];
    keyC[10] = (byte) 0b01000000;
    keyC[12] = (byte) 0b00010000;
    keyC[5] = (byte) 0xFF; // unrelated bytes — should not affect the partial encoding
    assertRoundTrip(parent, 0b11, keyC);
  }

  @Test
  @DisplayName("subset: relevant = single bit → drops the other byte")
  void subsetSingleBit() {
    // Parent: 2 disc bits across 2 bytes. Drop the LOW output bit (bit 0), keep only the HIGH (bit 1).
    final int[][] discBits = {{10, 1}, {12, 3}};
    final int[] partialKeys = {0b00, 0b11};
    final io.sirix.page.PageReference[] children = {dummyChild(100L), dummyChild(101L)};
    final io.sirix.page.HOTIndirectPage parent = buildSingleChunkMultiMask(discBits, partialKeys, children);

    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, 0b10);
    assertNotNull(sub);
    // Only byte 10 retained.
    assertEquals(1, sub.numExtractionBytes());
    assertEquals(1, sub.totalBits());
    assertEquals(10, sub.extractionPositions()[0] & 0xFF);

    // Round-trip on multiple keys.
    final byte[] k1 = new byte[16];
    k1[10] = (byte) 0b01000000;
    assertRoundTrip(parent, 0b10, k1);

    final byte[] k2 = new byte[16];
    k2[10] = 0;
    k2[12] = (byte) 0b00010000;
    assertRoundTrip(parent, 0b10, k2);

    final byte[] k3 = new byte[16];
    k3[10] = (byte) 0b01000000;
    k3[12] = (byte) 0b00010000;
    assertRoundTrip(parent, 0b10, k3);
  }

  @Test
  @DisplayName("subset: relevant = single bit (low) → drops the high byte")
  void subsetSingleBitLow() {
    final int[][] discBits = {{10, 1}, {12, 3}};
    final int[] partialKeys = {0b00, 0b11};
    final io.sirix.page.PageReference[] children = {dummyChild(100L), dummyChild(101L)};
    final io.sirix.page.HOTIndirectPage parent = buildSingleChunkMultiMask(discBits, partialKeys, children);

    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, 0b01);
    assertNotNull(sub);
    // Only byte 12 retained.
    assertEquals(1, sub.numExtractionBytes());
    assertEquals(1, sub.totalBits());
    assertEquals(12, sub.extractionPositions()[0] & 0xFF);

    final byte[] k1 = new byte[16];
    k1[10] = (byte) 0b01000000;
    assertRoundTrip(parent, 0b01, k1);

    final byte[] k2 = new byte[16];
    k2[12] = (byte) 0b00010000;
    assertRoundTrip(parent, 0b01, k2);

    final byte[] k3 = new byte[16];
    k3[10] = (byte) 0b01000000;
    k3[12] = (byte) 0b00010000;
    assertRoundTrip(parent, 0b01, k3);
  }

  @Test
  @DisplayName("multi-bit byte: drop one of two bits within same byte")
  void multiBitByteSubset() {
    // Parent: byte 10 contains bits 1 AND 5 MSB-first; byte 12 contains bit 3 MSB-first.
    // Output bit positions (MSB-first walking, HIGHEST output first):
    //   byte 10 bit 1 → output pos 2 (HIGHEST since 3 total bits, 3-1-0=2)
    //   byte 10 bit 5 → output pos 1
    //   byte 12 bit 3 → output pos 0 (LOWEST)
    final byte[] extractionPositions = {(byte) 10, (byte) 12};
    final long[] extractionMasks = new long[1];
    // Byte 10: bits 1 AND 5 MSB-first → MSB-first byte = 0b01000100 → at chunk-byte 0 → high 8 bits of long.
    extractionMasks[0] |= (1L << (7 * 8 + (7 - 1))); // bit 1
    extractionMasks[0] |= (1L << (7 * 8 + (7 - 5))); // bit 5
    // Byte 12: bit 3 MSB-first → at chunk-byte 1 → second-high 8 bits.
    extractionMasks[0] |= (1L << (6 * 8 + (7 - 3)));
    final int[] partialKeys = {0b000, 0b111};
    final io.sirix.page.PageReference[] children = {dummyChild(100L), dummyChild(101L)};
    final io.sirix.page.HOTIndirectPage parent = io.sirix.page.HOTIndirectPage.createMultiNodeMultiMask(
        /*pageKey=*/ 2L, /*revision=*/ 0,
        extractionPositions, extractionMasks, 2,
        partialKeys, children, /*height=*/ 1, /*msbIndex=*/ (short) (10 * 8 + 1));

    // Drop output bit 1 (= byte 10 bit 5), keep bits 0 and 2.
    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, 0b101);
    assertNotNull(sub);
    assertEquals(2, sub.numExtractionBytes()); // both bytes still have at least one kept bit
    assertEquals(2, sub.totalBits());

    final byte[] k = new byte[16];
    k[10] = (byte) 0b01000100; // both bits 1 and 5 set
    k[12] = (byte) 0b00010000; // bit 3 set
    assertRoundTrip(parent, 0b101, k);

    final byte[] k2 = new byte[16];
    k2[10] = (byte) 0b00000100; // only bit 5 set (will be dropped)
    k2[12] = (byte) 0b00010000;
    assertRoundTrip(parent, 0b101, k2);
  }

  @Test
  @DisplayName("relevant=0 → empty layout (degenerate but safe)")
  void relevantZero() {
    final int[][] discBits = {{10, 1}, {12, 3}};
    final int[] partialKeys = {0b00, 0b11};
    final io.sirix.page.PageReference[] children = {dummyChild(100L), dummyChild(101L)};
    final io.sirix.page.HOTIndirectPage parent = buildSingleChunkMultiMask(discBits, partialKeys, children);

    final HOTTrieWriter.MultiMaskSubLayout sub =
        HOTTrieWriter.extractMultiMaskSubset(parent, 0);
    assertNotNull(sub);
    assertEquals(0, sub.numExtractionBytes());
    assertEquals(0, sub.totalBits());
  }

  @Test
  @DisplayName("computeRelevantBitsFromPartials: adjacent-pair OR of (p[i] & ~p[i-1])")
  void relevantBitsAdjacentPair() {
    // Sorted partials over 4 bits: 0001, 0011, 0111, 1111.
    // Adjacent diffs: 0011 & ~0001 = 0010; 0111 & ~0011 = 0100; 1111 & ~0111 = 1000.
    // Union = 1110.
    final int[] partials = {0b0001, 0b0011, 0b0111, 0b1111};
    final int[] indices = {0, 1, 2, 3};
    assertEquals(0b1110, HOTTrieWriter.computeRelevantBitsFromPartials(partials, indices));
  }

  @Test
  @DisplayName("computeRelevantBitsFromPartials: subset of indices captures only that range's diffs")
  void relevantBitsSubsetOfIndices() {
    // Same partials; consider only indices [1, 2] = 0011 → 0111. Diff = 0100.
    final int[] partials = {0b0001, 0b0011, 0b0111, 0b1111};
    final int[] indices = {1, 2};
    assertEquals(0b0100, HOTTrieWriter.computeRelevantBitsFromPartials(partials, indices));
  }

  @Test
  @DisplayName("computeRelevantBitsFromPartials: singleton range → 0 (no adjacent pair)")
  void relevantBitsSingleton() {
    final int[] partials = {0b1010};
    final int[] indices = {0};
    assertEquals(0, HOTTrieWriter.computeRelevantBitsFromPartials(partials, indices));
  }
}
