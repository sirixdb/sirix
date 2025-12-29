/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link NodeReferencesSerializer}.
 * 
 * <p>Verifies packed format for small sets and Roaring format for large sets.</p>
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
}

