/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.utils.OS;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NodeReferencesSerializer}.
 * 
 * <p>
 * Verifies packed format for small sets and Roaring format for large sets.
 * </p>
 */
class NodeReferencesSerializerTest {

  @Test
  void testSerializeDeserializeEmpty() {
    NodeReferences refs = new NodeReferences();

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    assertTrue(NodeReferencesSerializer.isTombstone(bytes, 0, bytes.length));

    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);
    assertFalse(result.hasNodeKeys());
  }

  @Test
  void testSerializeDeserializeSingleKey() {
    NodeReferences refs = new NodeReferences();
    refs.addNodeKey(42L);

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    assertFalse(NodeReferencesSerializer.isTombstone(bytes, 0, bytes.length));

    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);
    assertTrue(result.contains(42L));
    assertEquals(1, result.getNodeKeys().getLongCardinality());
  }

  @Test
  void testSerializeDeserializeMultipleKeys() {
    NodeReferences refs = new NodeReferences();
    refs.addNodeKey(1L);
    refs.addNodeKey(100L);
    refs.addNodeKey(1000L);

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);

    assertTrue(result.contains(1L));
    assertTrue(result.contains(100L));
    assertTrue(result.contains(1000L));
    assertEquals(3, result.getNodeKeys().getLongCardinality());
  }

  @Test
  void testPackedFormatForSmallSets() {
    // Under 64 entries should use packed format
    NodeReferences refs = new NodeReferences();
    for (int i = 0; i < 10; i++) {
      refs.addNodeKey(i);
    }

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    // Packed format: [0x00][count:1][10 * 8 bytes] = 82 bytes
    assertEquals(0x00, bytes[0]); // Packed format marker
    assertEquals(10, bytes[1] & 0xFF); // Count

    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);
    assertEquals(10, result.getNodeKeys().getLongCardinality());
  }

  @Test
  void testRoaringFormatForLargeSets() {
    // Over 64 entries should use Roaring format
    NodeReferences refs = new NodeReferences();
    for (int i = 0; i < 100; i++) {
      refs.addNodeKey(i);
    }

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    assertEquals((byte) 0xFF, bytes[0]); // Roaring format marker

    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);
    assertEquals(100, result.getNodeKeys().getLongCardinality());
    for (int i = 0; i < 100; i++) {
      assertTrue(result.contains(i));
    }
  }

  @Test
  void testSerializeToBuffer() {
    NodeReferences refs = new NodeReferences();
    refs.addNodeKey(123L);
    refs.addNodeKey(456L);

    byte[] buffer = new byte[100];
    int len = NodeReferencesSerializer.serialize(refs, buffer, 10);

    NodeReferences result = NodeReferencesSerializer.deserialize(buffer, 10, len);
    assertTrue(result.contains(123L));
    assertTrue(result.contains(456L));
    assertEquals(2, result.getNodeKeys().getLongCardinality());
  }

  @Test
  void testMerge() {
    NodeReferences a = new NodeReferences();
    a.addNodeKey(1L);
    a.addNodeKey(2L);

    NodeReferences b = new NodeReferences();
    b.addNodeKey(2L);
    b.addNodeKey(3L);

    NodeReferencesSerializer.merge(a, b);

    assertTrue(a.contains(1L));
    assertTrue(a.contains(2L));
    assertTrue(a.contains(3L));
    assertEquals(3, a.getNodeKeys().getLongCardinality());
  }

  @Test
  void testTombstone() {
    NodeReferences refs = new NodeReferences(); // Empty = tombstone

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    assertTrue(NodeReferencesSerializer.isTombstone(bytes, 0, bytes.length));
    assertEquals(1, bytes.length);
    assertEquals((byte) 0xFE, bytes[0]); // Tombstone marker
  }

  @Test
  void testLargeNodeKeys() {
    NodeReferences refs = new NodeReferences();
    refs.addNodeKey(Long.MAX_VALUE);
    refs.addNodeKey(Long.MIN_VALUE);
    refs.addNodeKey(0L);

    byte[] bytes = NodeReferencesSerializer.serialize(refs);
    NodeReferences result = NodeReferencesSerializer.deserialize(bytes);

    assertTrue(result.contains(Long.MAX_VALUE));
    assertTrue(result.contains(Long.MIN_VALUE));
    assertTrue(result.contains(0L));
  }

  @Test
  void testPackedThresholdBoundary() {
    // Exactly 64 entries should use packed format
    NodeReferences refs64 = new NodeReferences();
    for (int i = 0; i < 64; i++) {
      refs64.addNodeKey(i);
    }
    byte[] bytes64 = NodeReferencesSerializer.serialize(refs64);
    assertEquals(0x00, bytes64[0]); // Packed format

    // 65 entries should use Roaring format
    NodeReferences refs65 = new NodeReferences();
    for (int i = 0; i < 65; i++) {
      refs65.addNodeKey(i);
    }
    byte[] bytes65 = NodeReferencesSerializer.serialize(refs65);
    assertEquals((byte) 0xFF, bytes65[0]); // Roaring format
  }

  // ==================== packed-payload fixtures ====================

  private static byte[] packedOf(long... keys) {
    final NodeReferences refs = new NodeReferences();
    for (final long k : keys) {
      refs.addNodeKey(k);
    }
    return NodeReferencesSerializer.serialize(refs);
  }

  private static byte[] singleBit(long key) {
    return packedOf(key); // a one-entry set serializes to [0x00][0x01][key:8]
  }

  private static byte[] roaringChunk(final boolean includeOutOfRangeValue) {
    final NodeReferences references = new NodeReferences();
    for (long bit16 = 0; bit16 <= 64; bit16++) {
      references.addNodeKey(bit16);
    }
    if (includeOutOfRangeValue) {
      references.addNodeKey(1L << 16);
    }
    final byte[] payload = NodeReferencesSerializer.serialize(references);
    assertEquals((byte) 0xFF, payload[0]);
    return payload;
  }

  /** Reference (slow) merge: deserialize both, OR, re-serialize. */
  private static byte[] slowMerge(byte[] existing, byte[] newValue, int off, int len) {
    final NodeReferences a = NodeReferencesSerializer.deserialize(existing);
    final NodeReferences b = NodeReferencesSerializer.deserialize(newValue, off, len);
    NodeReferencesSerializer.merge(a, b);
    return NodeReferencesSerializer.serialize(a);
  }

  /** Run the slot-based single-bit merge against a leaf holding {@code existing}. */
  private static byte @Nullable [] mergeFromSlot(byte[] existing, byte[] newValue, int off, int len) {
    final HOTLeafPage leaf = leafWithValue(existing);
    try {
      return NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf, leaf.valueRef(0), newValue, off, len);
    } finally {
      leaf.close();
    }
  }

  @Test
  void packedSingleBitMerge_insertsAbsentKey_byteIdenticalToSlowPath() {
    final byte[] existing = packedOf(10L, 30L, 50L);
    // Insert before-all, middle, and after-all positions.
    for (final long k : new long[] {5L, 20L, 40L, 60L}) {
      final byte[] nv = singleBit(k);
      final byte[] fast = mergeFromSlot(existing, nv, 0, nv.length);
      assertArrayEquals(slowMerge(existing, nv, 0, nv.length), fast,
          "fast path must be byte-identical to slow path for key " + k);
    }
  }

  @Test
  void packedSingleBitMerge_presentKey_returnsSameReferenceNoOp() {
    final byte[] existing = packedOf(10L, 30L, 50L);
    final byte[] nv = singleBit(30L);
    final byte[] fast = mergeFromSlot(existing, nv, 0, nv.length);
    assertSame(NodeReferencesSerializer.MERGE_UNCHANGED, fast, "present key must be a no-op");
    // Slow path leaves the set unchanged, so existing is already its own serialization.
    assertArrayEquals(slowMerge(existing, nv, 0, nv.length), existing);
  }

  @Test
  void packedSingleBitMerge_honorsOffsetIntoNewBuffer() {
    final byte[] existing = packedOf(100L, 200L);
    final byte[] one = singleBit(150L);
    final byte[] buf = new byte[3 + one.length + 4];
    System.arraycopy(one, 0, buf, 3, one.length);
    final byte[] fast = mergeFromSlot(existing, buf, 3, one.length);
    assertArrayEquals(slowMerge(existing, buf, 3, one.length), fast);
  }

  @Test
  void packedSingleBitMerge_bailsWhenBucketWouldOverflowToRoaring() {
    final long[] keys = new long[64];
    for (int i = 0; i < 64; i++) {
      keys[i] = i;
    }
    final byte[] existing = packedOf(keys); // exactly PACKED_THRESHOLD entries
    assertEquals(0x00, existing[0]);
    final byte[] nv = singleBit(1000L);
    assertNull(mergeFromSlot(existing, nv, 0, nv.length));
  }

  @Test
  void packedSingleBitMerge_bailsOnRoaringExisting() {
    final long[] keys = new long[65];
    for (int i = 0; i < 65; i++) {
      keys[i] = i;
    }
    final byte[] existing = packedOf(keys); // 65 entries -> Roaring format
    assertEquals((byte) 0xFF, existing[0]);
    final byte[] nv = singleBit(1000L);
    assertNull(mergeFromSlot(existing, nv, 0, nv.length));
  }

  @Test
  void packedSingleBitMerge_bailsWhenNewValueIsNotASinglePackedKey() {
    final byte[] existing = packedOf(10L, 20L);
    final byte[] twoKeys = packedOf(5L, 6L);
    assertNull(mergeFromSlot(existing, twoKeys, 0, twoKeys.length));
    final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
    assertNull(mergeFromSlot(existing, tombstone, 0, tombstone.length));
  }

  @Test
  void packedSingleBitMerge_bailsOnTombstoneExisting() {
    // Precondition: callers handle tombstone-existing before the fast path. The method still
    // defensively bails (a tombstone is not PACKED_FORMAT), deferring to the slow path.
    final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
    final byte[] nv = singleBit(7L);
    assertNull(mergeFromSlot(tombstone, nv, 0, nv.length));
  }

  // ==================== mergePackedSingleBitFromSlot (slot-memory twin) ====================

  /** A leaf holding {@code payload} under a single key; caller closes. */
  private static HOTLeafPage leafWithValue(byte[] payload) {
    return leafWithEntry("k".getBytes(StandardCharsets.UTF_8), payload);
  }

  /** A leaf holding one explicit key/value entry; caller closes. */
  private static HOTLeafPage leafWithEntry(final byte[] key, final byte[] payload) {
    if (!OS.isWindows()) {
      Allocators.getInstance().init(64L * 1024 * 1024);
    }
    final HOTLeafPage leaf = new HOTLeafPage(1L, 1, IndexType.CAS);
    assertTrue(leaf.put(key, payload));
    return leaf;
  }

  /** Assert that the allocation-free slot accumulator rejects a malformed stored payload. */
  private static void assertAccumulatorRejects(final byte[] payload) {
    final HOTLeafPage leaf = leafWithValue(payload);
    try {
      final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
      assertThrows(IllegalArgumentException.class, () -> accumulator.addChunk(leaf, leaf.valueRef(0), 0L));
    } finally {
      leaf.close();
    }
  }

  /** Assert that the validated-copy range merge rejects a malformed stored chunk. */
  private static void assertRangeMergeRejects(final byte[] payload) {
    final byte[] prefix = {0x11, 0x22};
    final byte[] composite = {0x11, 0x22, 0x00, 0x00, 0x00, 0x00};
    final HOTLeafPage leaf = leafWithEntry(composite, payload);
    try {
      final HOTRangeCursor cursor = mock(HOTRangeCursor.class);
      when(cursor.hasNext()).thenReturn(true, false);
      when(cursor.currentLeafPage()).thenReturn(leaf);
      when(cursor.currentEntryIndex()).thenReturn(0);
      when(cursor.validateLeaf()).thenReturn(true);
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergeChunksInPrefixRange(cursor, prefix, prefix.length));
    } finally {
      leaf.close();
    }
  }

  @Test
  void packedSingleBitMerge_fromSlot_matchesSlowPath_differentialRandom() {
    final Random rnd = new Random(0x5107);
    for (int trial = 0; trial < 500; trial++) {
      final int n = 1 + rnd.nextInt(63);
      final TreeSet<Long> set = new TreeSet<>();
      while (set.size() < n) {
        set.add((long) rnd.nextInt(1 << 16));
      }
      final byte[] existing = packedOf(set.stream().mapToLong(Long::longValue).toArray());
      final long newKey = rnd.nextInt(1 << 16);
      final byte[] nv = singleBit(newKey);
      final HOTLeafPage leaf = leafWithValue(existing);
      final byte[] fromSlot =
          NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf, leaf.valueRef(0), nv, 0, nv.length);
      if (set.contains(newKey)) {
        assertSame(NodeReferencesSerializer.MERGE_UNCHANGED, fromSlot,
            "present key must return the no-op sentinel at trial " + trial);
      } else {
        assertArrayEquals(slowMerge(existing, nv, 0, nv.length), fromSlot,
            "absent key must match the slow path at trial " + trial);
      }
      leaf.close();
    }
  }

  @Test
  void packedSingleBitMerge_intoCallerScratch_matchesAllocatingApiAndPreservesGuards() {
    final Random rnd = new Random(0xA110C);
    final byte[] scratch = new byte[NodeReferencesSerializer.MAX_PACKED_PAYLOAD_LENGTH + 6];
    final int scratchOffset = 3;

    for (int trial = 0; trial < 500; trial++) {
      final int count = 1 + rnd.nextInt(63);
      final TreeSet<Long> set = new TreeSet<>();
      while (set.size() < count) {
        set.add((long) rnd.nextInt(1 << 16));
      }
      final byte[] existing = packedOf(set.stream().mapToLong(Long::longValue).toArray());
      final byte[] incoming = singleBit(rnd.nextInt(1 << 16));
      final HOTLeafPage leaf = leafWithValue(existing);
      try {
        Arrays.fill(scratch, (byte) 0x5A);
        final byte[] allocatingResult =
            NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf, leaf.valueRef(0), incoming, 0, incoming.length);
        final int resultLength = NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf, leaf.valueRef(0), incoming,
            0, incoming.length, scratch, scratchOffset);

        if (allocatingResult == NodeReferencesSerializer.MERGE_UNCHANGED) {
          assertEquals(NodeReferencesSerializer.PACKED_MERGE_UNCHANGED, resultLength);
          for (final byte value : scratch) {
            assertEquals((byte) 0x5A, value, "unchanged status must not touch scratch");
          }
        } else {
          assertEquals(allocatingResult.length, resultLength);
          assertArrayEquals(allocatingResult, Arrays.copyOfRange(scratch, scratchOffset, scratchOffset + resultLength));
          assertEquals((byte) 0x5A, scratch[scratchOffset - 1]);
          assertEquals((byte) 0x5A, scratch[scratchOffset + resultLength]);
        }
      } finally {
        leaf.close();
      }
    }
  }

  @Test
  void packedSingleBitMerge_intoCallerScratch_validatesBeforeWriting() {
    final byte[] descending = packedOf(10L, 30L);
    System.arraycopy(descending, 2, descending, 2 + Long.BYTES, Long.BYTES);
    final HOTLeafPage leaf = leafWithValue(descending);
    final byte[] scratch = new byte[NodeReferencesSerializer.MAX_PACKED_PAYLOAD_LENGTH];
    Arrays.fill(scratch, (byte) 0x33);
    try {
      assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf,
          leaf.valueRef(0), singleBit(20L), 0, singleBit(20L).length, scratch, 0));
      for (final byte value : scratch) {
        assertEquals((byte) 0x33, value, "corruption failure must not touch caller scratch");
      }
    } finally {
      leaf.close();
    }
  }

  @Test
  void packedSingleBitMerge_intoUndersizedScratchFailsBeforeWriting() {
    final HOTLeafPage leaf = leafWithValue(packedOf(10L, 30L));
    final byte[] scratch = new byte[8];
    Arrays.fill(scratch, (byte) 0x22);
    final byte[] incoming = singleBit(20L);
    try {
      assertThrows(IndexOutOfBoundsException.class, () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf,
          leaf.valueRef(0), incoming, 0, incoming.length, scratch, 0));
      for (final byte value : scratch) {
        assertEquals((byte) 0x22, value, "capacity failure must not touch caller scratch");
      }
    } finally {
      leaf.close();
    }
  }

  @Test
  void packedSingleBitMerge_fromSlot_rejectsNonQualifyingShapes() {
    // Roaring-format existing payload: > PACKED_THRESHOLD entries.
    final NodeReferences big = new NodeReferences();
    for (int i = 0; i < 100; i++) {
      big.addNodeKey(i);
    }
    final byte[] nv = singleBit(7L);
    final HOTLeafPage roaringLeaf = leafWithValue(NodeReferencesSerializer.serialize(big));
    assertNull(
        NodeReferencesSerializer.mergePackedSingleBitFromSlot(roaringLeaf, roaringLeaf.valueRef(0), nv, 0, nv.length));
    roaringLeaf.close();

    // Multi-entry new payload does not qualify as a single-bit merge.
    final byte[] twoKeys = packedOf(1L, 2L);
    final HOTLeafPage packedLeaf = leafWithValue(packedOf(10L, 30L));
    assertNull(NodeReferencesSerializer.mergePackedSingleBitFromSlot(packedLeaf, packedLeaf.valueRef(0), twoKeys, 0,
        twoKeys.length));
    final byte[] scratch = new byte[NodeReferencesSerializer.MAX_PACKED_PAYLOAD_LENGTH];
    Arrays.fill(scratch, (byte) 0x44);
    assertEquals(NodeReferencesSerializer.PACKED_MERGE_NOT_APPLICABLE,
        NodeReferencesSerializer.mergePackedSingleBitFromSlot(packedLeaf, packedLeaf.valueRef(0), twoKeys, 0,
            twoKeys.length, scratch, 0));
    for (final byte value : scratch) {
      assertEquals((byte) 0x44, value, "not-applicable status must not touch scratch");
    }
    packedLeaf.close();

    // Tombstone payloads: rejected by the merge, recognized by the slot-side tombstone probe.
    final HOTLeafPage tombLeaf = leafWithValue(NodeReferencesSerializer.serialize(new NodeReferences()));
    assertNull(NodeReferencesSerializer.mergePackedSingleBitFromSlot(tombLeaf, tombLeaf.valueRef(0), nv, 0, nv.length));
    assertTrue(NodeReferencesSerializer.isTombstone(tombLeaf, tombLeaf.valueRef(0)));
    tombLeaf.close();
  }

  @Test
  void packedSingleBitRemove_fromSlot_removesFirstMiddleAndLastWithoutCopyingInput() {
    final long[] keys = {10L, 30L, 50L, 70L};
    for (final long removed : new long[] {10L, 50L, 70L}) {
      final HOTLeafPage leaf = leafWithValue(packedOf(keys));
      try {
        final byte[] scratch = new byte[64];
        Arrays.fill(scratch, (byte) 0x5A);
        final int offset = 5;
        final int resultLength =
            NodeReferencesSerializer.removePackedSingleBitFromSlot(leaf, leaf.valueRef(0), removed, scratch, offset);

        assertEquals(2 + (keys.length - 1) * Long.BYTES, resultLength);
        final NodeReferences result = NodeReferencesSerializer.deserialize(scratch, offset, resultLength);
        assertEquals(keys.length - 1, result.getNodeKeys().getLongCardinality());
        for (final long key : keys) {
          assertEquals(key != removed, result.contains(key), "unexpected membership for key " + key);
        }
        assertEquals((byte) 0x5A, scratch[offset - 1], "bytes before the output range must be untouched");
        assertEquals((byte) 0x5A, scratch[offset + resultLength], "bytes after the output range must be untouched");
      } finally {
        leaf.close();
      }
    }
  }

  @Test
  void packedSingleBitRemove_fromSlot_reportsAbsentAndEmptyUnambiguously() {
    final byte[] scratch = new byte[32];
    Arrays.fill(scratch, (byte) 0x33);
    final HOTLeafPage multiple = leafWithValue(packedOf(10L, 30L, 50L));
    final HOTLeafPage single = leafWithValue(packedOf(30L));
    try {
      assertEquals(NodeReferencesSerializer.PACKED_REMOVE_ABSENT,
          NodeReferencesSerializer.removePackedSingleBitFromSlot(multiple, multiple.valueRef(0), 20L, scratch, 4));
      assertEquals(NodeReferencesSerializer.PACKED_REMOVE_EMPTY,
          NodeReferencesSerializer.removePackedSingleBitFromSlot(single, single.valueRef(0), 30L, scratch, 4));
      for (final byte value : scratch) {
        assertEquals((byte) 0x33, value, "non-result statuses must not touch caller scratch");
      }
    } finally {
      multiple.close();
      single.close();
    }
  }

  @Test
  void packedSingleBitRemove_fromSlot_reportsNonPackedRepresentation() {
    final NodeReferences refs = new NodeReferences();
    for (int i = 0; i < 65; i++) {
      refs.addNodeKey(i);
    }
    final HOTLeafPage roaring = leafWithValue(NodeReferencesSerializer.serialize(refs));
    try {
      assertEquals(NodeReferencesSerializer.PACKED_REMOVE_NOT_APPLICABLE,
          NodeReferencesSerializer.removePackedSingleBitFromSlot(roaring, roaring.valueRef(0), 7L, new byte[1], 0));
    } finally {
      roaring.close();
    }
  }

  @Test
  void packedSingleBitRemove_fromSlot_rejectsMalformedPackedCountAndLength() {
    final byte[] truncated = packedOf(10L, 30L);
    truncated[1] = 3;
    final HOTLeafPage truncatedLeaf = leafWithValue(truncated);
    final byte[] trailing = Arrays.copyOf(packedOf(10L, 30L), 2 + 3 * Long.BYTES);
    final HOTLeafPage trailingLeaf = leafWithValue(trailing);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.removePackedSingleBitFromSlot(truncatedLeaf, truncatedLeaf.valueRef(0), 10L,
              new byte[32], 0));
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.removePackedSingleBitFromSlot(trailingLeaf, trailingLeaf.valueRef(0), 10L,
              new byte[32], 0));
    } finally {
      truncatedLeaf.close();
      trailingLeaf.close();
    }
  }

  @Test
  void postingFormatsRejectNonCanonicalTombstonesAndPackedOrdering() {
    assertThrows(IllegalArgumentException.class,
        () -> NodeReferencesSerializer.deserialize(new byte[] {(byte) 0xFE, 0x01}));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserialize(new byte[] {0x00, 0x00}));

    final byte[] unsorted = packedOf(10L, 30L);
    for (int i = 0; i < Long.BYTES; i++) {
      final byte first = unsorted[2 + i];
      unsorted[2 + i] = unsorted[2 + Long.BYTES + i];
      unsorted[2 + Long.BYTES + i] = first;
    }
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserialize(unsorted));

    final HOTLeafPage leaf = leafWithValue(unsorted);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.removePackedSingleBitFromSlot(leaf, leaf.valueRef(0), 10L, new byte[32], 0));
      assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(leaf,
          leaf.valueRef(0), singleBit(20L), 0, singleBit(20L).length));
    } finally {
      leaf.close();
    }
  }

  @Test
  void packedSlotMutationsRejectValuesOutsideTheUnsigned16BitChunkDomain() {
    final byte[] outOfRange = singleBit(1L << 16);
    final byte[] inRange = singleBit(7L);
    final HOTLeafPage malformedExisting = leafWithValue(outOfRange);
    final HOTLeafPage validExisting = leafWithValue(inRange);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(malformedExisting, malformedExisting.valueRef(0),
              inRange, 0, inRange.length));
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(validExisting, validExisting.valueRef(0),
              outOfRange, 0, outOfRange.length));
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.removePackedSingleBitFromSlot(malformedExisting, malformedExisting.valueRef(0),
              7L, new byte[16], 0));
    } finally {
      malformedExisting.close();
      validExisting.close();
    }

    final NodeReferences fullMalformed = new NodeReferences();
    for (long bit16 = 0; bit16 < 63; bit16++) {
      fullMalformed.addNodeKey(bit16);
    }
    fullMalformed.addNodeKey(1L << 16);
    final byte[] fullMalformedPayload = NodeReferencesSerializer.serialize(fullMalformed);
    assertEquals(64, fullMalformedPayload[1] & 0xFF);
    final HOTLeafPage fullMalformedLeaf = leafWithValue(fullMalformedPayload);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.mergePackedSingleBitFromSlot(fullMalformedLeaf, fullMalformedLeaf.valueRef(0),
              inRange, 0, inRange.length));
      assertThrows(IllegalArgumentException.class,
          () -> NodeReferencesSerializer.removePackedSingleBitFromSlot(fullMalformedLeaf, fullMalformedLeaf.valueRef(0),
              7L, new byte[512], 0));
    } finally {
      fullMalformedLeaf.close();
    }
  }

  @Test
  void chunkDeserializerRejectsPackedAndRoaringValuesOutsideUnsigned16BitDomain() {
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeChunk(new byte[0]));
    assertTrue(NodeReferencesSerializer.deserializeChunk(singleBit(0xFFFFL)).contains(0xFFFFL));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeChunk(singleBit(1L << 16)));
    assertThrows(IllegalArgumentException.class, () -> NodeReferencesSerializer.deserializeChunk(roaringChunk(true)));
  }

  @Test
  void coldBitmapMergeFallbacksRejectOutOfRangeValuesWithoutMutatingTheLeaf() {
    final byte[] malformedRoaring = roaringChunk(true);
    final byte[] validRoaring = roaringChunk(false);
    final byte[] incoming = singleBit(7L);
    final HOTLeafPage malformedExisting = leafWithValue(malformedRoaring);
    final HOTLeafPage validExisting = leafWithValue(validRoaring);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> malformedExisting.mergeWithNodeRefs("k".getBytes(StandardCharsets.UTF_8), 1, incoming,
              incoming.length));
      assertArrayEquals(malformedRoaring, malformedExisting.getValue(0));

      assertThrows(IllegalArgumentException.class,
          () -> validExisting.mergeWithNodeRefs("k".getBytes(StandardCharsets.UTF_8), 1, malformedRoaring,
              malformedRoaring.length));
      assertArrayEquals(validRoaring, validExisting.getValue(0));

      assertThrows(IllegalArgumentException.class,
          () -> HOTIncrementalInsert.mergeIndexValues(malformedRoaring, incoming));
      assertThrows(IllegalArgumentException.class,
          () -> HOTIncrementalInsert.mergeIndexValues(validRoaring, malformedRoaring));
    } finally {
      malformedExisting.close();
      validExisting.close();
    }
  }

  @Test
  void tombstonePassThroughMergesValidateIncomingChunksWithoutPublishingCorruption() {
    final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
    final byte[] malformedPacked = singleBit(1L << 16);
    final HOTLeafPage leaf = leafWithValue(tombstone);
    try {
      assertThrows(IllegalArgumentException.class, () -> leaf.mergeWithNodeRefs("k".getBytes(StandardCharsets.UTF_8), 1,
          malformedPacked, malformedPacked.length));
      assertArrayEquals(tombstone, leaf.getValue(0));
    } finally {
      leaf.close();
    }

    assertThrows(IllegalArgumentException.class,
        () -> HOTIncrementalInsert.mergeIndexValues(tombstone, malformedPacked));
  }

  @Test
  void chunkAccumulatorRejectsNonCanonicalTombstoneAndPackedSlots() {
    assertAccumulatorRejects(new byte[0]);
    assertAccumulatorRejects(new byte[] {(byte) 0xFE, 0x01});
    assertAccumulatorRejects(new byte[] {0x00});
    assertAccumulatorRejects(new byte[] {0x00, 0x00});
    assertAccumulatorRejects(new byte[] {0x00, 65});

    final byte[] truncated = packedOf(10L, 30L);
    truncated[1] = 3;
    assertAccumulatorRejects(truncated);

    final byte[] trailing = Arrays.copyOf(packedOf(10L, 30L), 2 + 3 * Long.BYTES);
    assertAccumulatorRejects(trailing);

    final byte[] descending = packedOf(10L, 30L);
    for (int i = 0; i < Long.BYTES; i++) {
      final byte first = descending[2 + i];
      descending[2 + i] = descending[2 + Long.BYTES + i];
      descending[2 + Long.BYTES + i] = first;
    }
    assertAccumulatorRejects(descending);

    final byte[] duplicate = packedOf(10L, 30L);
    System.arraycopy(duplicate, 2, duplicate, 2 + Long.BYTES, Long.BYTES);
    assertAccumulatorRejects(duplicate);

    // Chunk payload keys are bit16 values, not arbitrary node keys. Masking this value would alias
    // 65,536 to bit zero and manufacture a false posting.
    assertAccumulatorRejects(packedOf(1L << 16));

    assertAccumulatorRejects(roaringChunk(true));
  }

  @Test
  void chunkAccumulatorRejectsAnUnreadableSlotReference() {
    final HOTLeafPage leaf = leafWithValue(singleBit(7L));
    try {
      final NodeReferencesSerializer.ChunkAccumulator accumulator = new NodeReferencesSerializer.ChunkAccumulator();
      assertThrows(IllegalStateException.class, () -> accumulator.addChunk(leaf, HOTLeafPage.NO_VALUE_REF, 0L));
    } finally {
      leaf.close();
    }
  }

  @Test
  void rangeMergeRejectsChunkValuesOutsideTheUnsigned16BitDomain() {
    assertRangeMergeRejects(packedOf(1L << 16));

    assertRangeMergeRejects(roaringChunk(true));
  }

  @Test
  void rangeMergeRejectsNonCanonicalTombstoneAndPackedChunks() {
    assertRangeMergeRejects(new byte[0]);
    assertRangeMergeRejects(new byte[] {(byte) 0xFE, 0x01});
    assertRangeMergeRejects(new byte[] {0x00});
    assertRangeMergeRejects(new byte[] {0x00, 0x00});

    final byte[] trailing = Arrays.copyOf(packedOf(10L, 30L), 2 + 3 * Long.BYTES);
    assertRangeMergeRejects(trailing);

    final byte[] descending = packedOf(10L, 30L);
    for (int i = 0; i < Long.BYTES; i++) {
      final byte first = descending[2 + i];
      descending[2 + i] = descending[2 + Long.BYTES + i];
      descending[2 + Long.BYTES + i] = first;
    }
    assertRangeMergeRejects(descending);
  }
}
