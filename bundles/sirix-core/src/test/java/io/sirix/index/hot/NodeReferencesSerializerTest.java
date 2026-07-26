/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

  // ==================== mergePackedSingleBit fast path ====================

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

  /** Reference (slow) merge: deserialize both, OR, re-serialize. */
  private static byte[] slowMerge(byte[] existing, byte[] newValue, int off, int len) {
    final NodeReferences a = NodeReferencesSerializer.deserialize(existing);
    final NodeReferences b = NodeReferencesSerializer.deserialize(newValue, off, len);
    NodeReferencesSerializer.merge(a, b);
    return NodeReferencesSerializer.serialize(a);
  }

  @Test
  void mergePackedSingleBit_insertsAbsentKey_byteIdenticalToSlowPath() {
    final byte[] existing = packedOf(10L, 30L, 50L);
    // Insert before-all, middle, and after-all positions.
    for (final long k : new long[] {5L, 20L, 40L, 60L}) {
      final byte[] nv = singleBit(k);
      final byte[] fast = NodeReferencesSerializer.mergePackedSingleBit(existing, nv, 0, nv.length);
      assertArrayEquals(slowMerge(existing, nv, 0, nv.length), fast,
          "fast path must be byte-identical to slow path for key " + k);
    }
  }

  @Test
  void mergePackedSingleBit_presentKey_returnsSameReferenceNoOp() {
    final byte[] existing = packedOf(10L, 30L, 50L);
    final byte[] nv = singleBit(30L);
    final byte[] fast = NodeReferencesSerializer.mergePackedSingleBit(existing, nv, 0, nv.length);
    assertSame(existing, fast, "present key must be a no-op (same reference)");
    // Slow path leaves the set unchanged, so existing is already its own serialization.
    assertArrayEquals(slowMerge(existing, nv, 0, nv.length), existing);
  }

  @Test
  void mergePackedSingleBit_honorsOffsetIntoNewBuffer() {
    final byte[] existing = packedOf(100L, 200L);
    final byte[] one = singleBit(150L);
    final byte[] buf = new byte[3 + one.length + 4];
    System.arraycopy(one, 0, buf, 3, one.length);
    final byte[] fast = NodeReferencesSerializer.mergePackedSingleBit(existing, buf, 3, one.length);
    assertArrayEquals(slowMerge(existing, buf, 3, one.length), fast);
  }

  @Test
  void mergePackedSingleBit_bailsWhenBucketWouldOverflowToRoaring() {
    final long[] keys = new long[64];
    for (int i = 0; i < 64; i++) {
      keys[i] = i;
    }
    final byte[] existing = packedOf(keys); // exactly PACKED_THRESHOLD entries
    assertEquals(0x00, existing[0]);
    final byte[] nv = singleBit(1000L);
    assertNull(NodeReferencesSerializer.mergePackedSingleBit(existing, nv, 0, nv.length));
  }

  @Test
  void mergePackedSingleBit_bailsOnRoaringExisting() {
    final long[] keys = new long[65];
    for (int i = 0; i < 65; i++) {
      keys[i] = i;
    }
    final byte[] existing = packedOf(keys); // 65 entries -> Roaring format
    assertEquals((byte) 0xFF, existing[0]);
    final byte[] nv = singleBit(1000L);
    assertNull(NodeReferencesSerializer.mergePackedSingleBit(existing, nv, 0, nv.length));
  }

  @Test
  void mergePackedSingleBit_bailsWhenNewValueIsNotASinglePackedKey() {
    final byte[] existing = packedOf(10L, 20L);
    final byte[] twoKeys = packedOf(5L, 6L);
    assertNull(NodeReferencesSerializer.mergePackedSingleBit(existing, twoKeys, 0, twoKeys.length));
    final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
    assertNull(
        NodeReferencesSerializer.mergePackedSingleBit(existing, tombstone, 0, tombstone.length));
  }

  @Test
  void mergePackedSingleBit_bailsOnTombstoneExisting() {
    // Precondition: callers handle tombstone-existing before the fast path. The method still
    // defensively bails (a tombstone is not PACKED_FORMAT), deferring to the slow path.
    final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
    final byte[] nv = singleBit(7L);
    assertNull(NodeReferencesSerializer.mergePackedSingleBit(tombstone, nv, 0, nv.length));
  }

  @Test
  void mergePackedSingleBit_differentialRandom() {
    final Random rnd = new Random(0xC0FFEE);
    for (int trial = 0; trial < 5000; trial++) {
      // 1..63 entries: existing is genuinely packed (n==0 would be a tombstone, out of contract)
      // and stays packed after one insert (<= PACKED_THRESHOLD).
      final int n = 1 + rnd.nextInt(63);
      final java.util.TreeSet<Long> set = new java.util.TreeSet<>();
      while (set.size() < n) {
        set.add((long) rnd.nextInt(1 << 16)); // chunk-local 16-bit values, as on the live path
      }
      final byte[] existing = packedOf(set.stream().mapToLong(Long::longValue).toArray());
      final long newKey = rnd.nextInt(1 << 16);
      final byte[] nv = singleBit(newKey);
      final byte[] fast = NodeReferencesSerializer.mergePackedSingleBit(existing, nv, 0, nv.length);
      final byte[] slow = slowMerge(existing, nv, 0, nv.length);
      if (set.contains(newKey)) {
        assertSame(existing, fast, "present key must be a no-op at trial " + trial);
        assertArrayEquals(slow, existing);
      } else {
        assertArrayEquals(slow, fast, "absent key must match slow path at trial " + trial);
      }
    }
  }
}

