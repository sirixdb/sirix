/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.node.NodeKind;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.utils.FSSTCompressor;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FsstAwareSlotCopier}. Builds slot byte layouts that
 * mirror the production flyweight wire format produced by
 * {@code StringNode.writeNewRecord} and {@code ObjectStringNode.writeNewRecord}
 * so the copier is exercised against real-world inputs — not mocks.
 */
class FsstAwareSlotCopierTest {

  private static final int STRING_VALUE_FIELDS = 6;
  private static final int OBJECT_STRING_VALUE_FIELDS = 4;
  private static final int STRING_VALUE_PAYLOAD_FIELD = 5;
  private static final int OBJECT_STRING_VALUE_PAYLOAD_FIELD = 3;

  @Test
  void inactiveWhenSymbolTableIsNull() {
    final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(null);
    assertFalse(copier.active());
    assertNull(copier.decompressSlot(MemorySegment.ofArray(new byte[] { 1, 2, 3 }),
        NodeKind.STRING_VALUE.getId()));
  }

  @Test
  void inactiveWhenSymbolTableIsEmpty() {
    final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(new byte[0]);
    assertFalse(copier.active());
  }

  @Test
  void decompressesStringValueFlyweightSlot() throws Exception {
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final byte[] original = "The quick brown fox jumps over the lazy dog.".getBytes(StandardCharsets.UTF_8);
    final byte[] compressed = FSSTCompressor.encode(original, symbolTable);
    assertTrue(compressed.length < original.length, "Compression should reduce size for this fixture");

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = buildStringValueFlyweightSlot(arena, compressed, /*isCompressed=*/true);
      final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);

      final byte[] rewritten = copier.decompressSlot(slot, NodeKind.STRING_VALUE.getId());

      assertNotNull(rewritten, "Compressed string-value slot must be rewritten");
      assertExtractedPayloadEquals(rewritten, STRING_VALUE_FIELDS, STRING_VALUE_PAYLOAD_FIELD, original);
      assertKindByteUnchanged(rewritten, NodeKind.STRING_VALUE.getId());
    }
  }

  @Test
  void decompressesObjectStringValueFlyweightSlot() throws Exception {
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final byte[] original = "{\"value\":\"The quick brown fox jumps over.\"}".getBytes(StandardCharsets.UTF_8);
    final byte[] compressed = FSSTCompressor.encode(original, symbolTable);
    assertTrue(compressed.length < original.length);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = buildObjectStringValueFlyweightSlot(arena, compressed, /*isCompressed=*/true);
      final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);

      final byte[] rewritten = copier.decompressSlot(slot, NodeKind.OBJECT_STRING_VALUE.getId());

      assertNotNull(rewritten);
      assertExtractedPayloadEquals(rewritten, OBJECT_STRING_VALUE_FIELDS, OBJECT_STRING_VALUE_PAYLOAD_FIELD, original);
      assertKindByteUnchanged(rewritten, NodeKind.OBJECT_STRING_VALUE.getId());
    }
  }

  @Test
  void returnsNullForUncompressedStringValueSlot() {
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final byte[] raw = "some uncompressed value".getBytes(StandardCharsets.UTF_8);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = buildStringValueFlyweightSlot(arena, raw, /*isCompressed=*/false);
      final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);

      final byte[] rewritten = copier.decompressSlot(slot, NodeKind.STRING_VALUE.getId());
      assertNull(rewritten, "Uncompressed slots must not be rewritten");
    }
  }

  @Test
  void returnsNullForNonStringKinds() {
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);

    final MemorySegment slot = MemorySegment.ofArray(new byte[] { 1, 2, 3, 4 });
    assertNull(copier.decompressSlot(slot, NodeKind.BOOLEAN_VALUE.getId()));
    assertNull(copier.decompressSlot(slot, NodeKind.NUMBER_VALUE.getId()));
    assertNull(copier.decompressSlot(slot, NodeKind.OBJECT_KEY.getId()));
  }

  @Test
  void returnsNullForNullSlot() {
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);
    assertNull(copier.decompressSlot(null, NodeKind.STRING_VALUE.getId()));
  }

  @Test
  void preservesStructuralVarintsExactly() {
    // Guard rail: the rewrite must only change the payload. All bytes up to
    // (and including) the offset table, plus every structural varint before
    // the isCompressed flag, must be byte-identical to the source.
    final byte[] symbolTable = buildSymbolTableFromSimilarStrings();
    final byte[] original = "The quick brown fox jumps.".getBytes(StandardCharsets.UTF_8);
    final byte[] compressed = FSSTCompressor.encode(original, symbolTable);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = buildStringValueFlyweightSlot(arena, compressed, /*isCompressed=*/true);
      final byte[] originalSlotBytes = slot.toArray(ValueLayout.JAVA_BYTE);

      final FsstAwareSlotCopier copier = new FsstAwareSlotCopier(symbolTable);
      final byte[] rewritten = copier.decompressSlot(slot, NodeKind.STRING_VALUE.getId());

      assertNotNull(rewritten);
      // Locate the compressed-flag position in the source slot to compute the prefix length.
      final int dataStart = 1 + STRING_VALUE_FIELDS;
      final int payloadOffset = originalSlotBytes[1 + STRING_VALUE_PAYLOAD_FIELD] & 0xFF;
      final int flagPos = dataStart + payloadOffset;
      // All bytes strictly before the flag must match byte-for-byte.
      for (int i = 0; i < flagPos; i++) {
        assertEquals(originalSlotBytes[i], rewritten[i],
            "Byte " + i + " (pre-payload) must be preserved");
      }
      // Flag byte must be zero after rewrite.
      assertEquals((byte) 0, rewritten[flagPos], "isCompressed flag must be 0 after rewrite");
    }
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  /**
   * Build an FSST symbol table from a corpus large enough to yield a
   * non-trivial table (>= MIN_SAMPLES_FOR_TABLE). The corpus contains
   * repeated common substrings so the table includes multi-byte symbols.
   */
  private static byte[] buildSymbolTableFromSimilarStrings() {
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      samples.add((
          "The quick brown fox jumps over the lazy dog — sample " + i
              + " with additional uniform text to bulk up the corpus.")
              .getBytes(StandardCharsets.UTF_8));
    }
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assertNotNull(table);
    assertTrue(table.length > 0, "Symbol table must be non-empty");
    return table;
  }

  /**
   * Assemble a STRING_VALUE flyweight slot. Structural keys are dummy values
   * (no fragment semantics are exercised here — just wire format).
   */
  private static MemorySegment buildStringValueFlyweightSlot(final Arena arena, final byte[] rawValue,
      final boolean isCompressed) {
    final MemorySegment scratch = arena.allocate(4096);
    final int[] heapOffsets = new int[STRING_VALUE_FIELDS];
    final int written = StringNode.writeNewRecord(scratch, 0, heapOffsets, /*nodeKey=*/1000,
        /*parentKey=*/500, /*rightSibKey=*/0, /*leftSibKey=*/0,
        /*prevRev=*/-1, /*lastModRev=*/0,
        rawValue, 0, rawValue.length, isCompressed);
    return scratch.asSlice(0, written);
  }

  private static MemorySegment buildObjectStringValueFlyweightSlot(final Arena arena, final byte[] rawValue,
      final boolean isCompressed) {
    final MemorySegment scratch = arena.allocate(4096);
    final int[] heapOffsets = new int[OBJECT_STRING_VALUE_FIELDS];
    final int written = ObjectStringNode.writeNewRecord(scratch, 0, heapOffsets, /*nodeKey=*/1000,
        /*parentKey=*/500, /*prevRev=*/-1, /*lastModRev=*/0,
        rawValue, 0, rawValue.length, isCompressed);
    return scratch.asSlice(0, written);
  }

  /**
   * Decode the payload of a rewritten flyweight slot and compare against the
   * expected (uncompressed) value. Verifies that (1) the rewrite's compressed
   * flag is 0; (2) the length varint holds the decompressed length; (3) the
   * value bytes match exactly.
   */
  private static void assertExtractedPayloadEquals(final byte[] rewritten, final int fieldCount,
      final int payloadFieldIdx, final byte[] expected) {
    final int dataStart = 1 + fieldCount;
    final int payloadOffset = rewritten[1 + payloadFieldIdx] & 0xFF;
    final int payloadAbs = dataStart + payloadOffset;

    final byte flag = rewritten[payloadAbs];
    assertEquals((byte) 0, flag, "isCompressed must be 0 after decompress");

    // Decode length varint.
    int pos = payloadAbs + 1;
    long raw = 0;
    int shift = 0;
    while (true) {
      final int b = rewritten[pos++] & 0xFF;
      raw |= ((long) (b & 0x7F)) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    final int length = (int) ((raw >>> 1) ^ -(raw & 1)); // zigzag decode

    assertEquals(expected.length, length, "Length field must match decompressed value length");

    final byte[] actual = new byte[length];
    System.arraycopy(rewritten, pos, actual, 0, length);
    assertArrayEquals(expected, actual);
  }

  private static void assertKindByteUnchanged(final byte[] rewritten, final int expectedKindId) {
    assertEquals((byte) expectedKindId, rewritten[0],
        "Kind byte must be preserved byte-for-byte across the rewrite");
  }
}
