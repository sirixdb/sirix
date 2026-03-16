/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.index.vector;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Roundtrip serialization tests for {@link VectorNode} and {@link VectorIndexMetadataNode}
 * through {@link NodeKind} serialize/deserialize.
 */
public final class VectorNodeSerializationTest {

  /** Lightweight ResourceConfiguration (never accessed by vector serialization). */
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("test").hashKind(HashType.NONE).build();

  @Test
  public void testVectorNodeRoundtripSimple() {
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final VectorNode original = new VectorNode(42L, 10L, vector, 0, 5);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(42L, deserialized.getNodeKey());
    assertEquals(10L, deserialized.getDocumentNodeKey());
    assertArrayEquals(vector, deserialized.getVector(), 0.0f);
    assertEquals(0, deserialized.getMaxLayer());
    assertEquals(0, deserialized.getNeighborCount(0));
    assertEquals(5, deserialized.getPreviousRevisionNumber());
    assertEquals(5, deserialized.getLastModifiedRevisionNumber());
    assertEquals(NodeKind.VECTOR_NODE, deserialized.getKind());
    assertNull(deserialized.getDeweyID());
    assertNull(deserialized.getDeweyIDAsBytes());
  }

  @Test
  public void testVectorNodeRoundtripWithNeighbors() {
    final float[] vector = {0.5f, -1.5f, 3.14f, 2.71f};
    final long[][] neighbors = new long[1][];
    final int[] neighborCounts = new int[1];

    // Layer 0 has sorted neighbor keys.
    neighbors[0] = new long[]{5L, 10L, 20L, 50L};
    neighborCounts[0] = 4;

    final VectorNode original = new VectorNode(100L, 1L, vector, 0,
        neighbors, neighborCounts, 3);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(100L, deserialized.getNodeKey());
    assertEquals(1L, deserialized.getDocumentNodeKey());
    assertArrayEquals(vector, deserialized.getVector(), 0.0f);
    assertEquals(0, deserialized.getMaxLayer());
    assertEquals(4, deserialized.getNeighborCount(0));

    final long[] deserializedNeighbors = deserialized.getNeighbors(0);
    assertNotNull(deserializedNeighbors);
    assertEquals(5L, deserializedNeighbors[0]);
    assertEquals(10L, deserializedNeighbors[1]);
    assertEquals(20L, deserializedNeighbors[2]);
    assertEquals(50L, deserializedNeighbors[3]);
  }

  @Test
  public void testVectorNodeRoundtripLargeDimension() {
    // Typical OpenAI embedding dimension.
    final int dim = 1536;
    final float[] vector = new float[dim];
    for (int i = 0; i < dim; i++) {
      vector[i] = (float) (Math.sin(i) * 0.1);
    }

    final VectorNode original = new VectorNode(999L, 500L, vector, 0, 1);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(999L, deserialized.getNodeKey());
    assertEquals(500L, deserialized.getDocumentNodeKey());
    assertEquals(dim, deserialized.getDimension());
    assertArrayEquals(vector, deserialized.getVector(), 0.0f);
    assertEquals(0, deserialized.getMaxLayer());
    assertEquals(1, deserialized.getPreviousRevisionNumber());
  }

  @Test
  public void testVectorNodeRoundtripMultiLayer() {
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final int maxLayer = 2;
    final long[][] neighbors = new long[maxLayer + 1][];
    final int[] neighborCounts = new int[maxLayer + 1];

    // Layer 0: 5 neighbors.
    neighbors[0] = new long[]{2L, 5L, 8L, 12L, 20L};
    neighborCounts[0] = 5;

    // Layer 1: 3 neighbors.
    neighbors[1] = new long[]{3L, 15L, 30L};
    neighborCounts[1] = 3;

    // Layer 2: 1 neighbor.
    neighbors[2] = new long[]{7L};
    neighborCounts[2] = 1;

    final VectorNode original = new VectorNode(50L, 10L, vector, maxLayer,
        neighbors, neighborCounts, 10);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(50L, deserialized.getNodeKey());
    assertEquals(10L, deserialized.getDocumentNodeKey());
    assertEquals(maxLayer, deserialized.getMaxLayer());

    // Layer 0.
    assertEquals(5, deserialized.getNeighborCount(0));
    final long[] layer0 = deserialized.getNeighbors(0);
    assertNotNull(layer0);
    assertEquals(2L, layer0[0]);
    assertEquals(5L, layer0[1]);
    assertEquals(8L, layer0[2]);
    assertEquals(12L, layer0[3]);
    assertEquals(20L, layer0[4]);

    // Layer 1.
    assertEquals(3, deserialized.getNeighborCount(1));
    final long[] layer1 = deserialized.getNeighbors(1);
    assertNotNull(layer1);
    assertEquals(3L, layer1[0]);
    assertEquals(15L, layer1[1]);
    assertEquals(30L, layer1[2]);

    // Layer 2.
    assertEquals(1, deserialized.getNeighborCount(2));
    final long[] layer2 = deserialized.getNeighbors(2);
    assertNotNull(layer2);
    assertEquals(7L, layer2[0]);
  }

  @Test
  public void testVectorNodeEmptyNeighborLayers() {
    final float[] vector = {1.0f};
    final int maxLayer = 2;
    final long[][] neighbors = new long[maxLayer + 1][];
    final int[] neighborCounts = new int[maxLayer + 1];

    // All layers empty.
    neighborCounts[0] = 0;
    neighborCounts[1] = 0;
    neighborCounts[2] = 0;

    final VectorNode original = new VectorNode(1L, 1L, vector, maxLayer,
        neighbors, neighborCounts, 0);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(1L, deserialized.getNodeKey());
    assertEquals(maxLayer, deserialized.getMaxLayer());
    for (int layer = 0; layer <= maxLayer; layer++) {
      assertEquals(0, deserialized.getNeighborCount(layer));
    }
  }

  @Test
  public void testVectorNodeNegativeDelta() {
    // documentNodeKey < nodeKey produces a negative delta.
    final float[] vector = {1.0f, 2.0f};
    final VectorNode original = new VectorNode(100L, 50L, vector, 0, 7);

    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(100L, deserialized.getNodeKey());
    assertEquals(50L, deserialized.getDocumentNodeKey());
  }

  @Test
  public void testVectorNodeEquality() {
    final float[] vector = {1.0f, 2.0f, 3.0f};
    final long[][] neighbors = new long[1][];
    final int[] neighborCounts = new int[1];
    neighbors[0] = new long[]{5L, 10L};
    neighborCounts[0] = 2;

    final VectorNode original = new VectorNode(42L, 10L, vector, 0,
        neighbors, neighborCounts, 5);
    final VectorNode deserialized = serializeAndDeserializeVectorNode(original);

    assertEquals(original, deserialized);
    assertEquals(original.hashCode(), deserialized.hashCode());
  }

  @Test
  public void testVectorIndexMetadataNodeRoundtrip() {
    final VectorIndexMetadataNode original = new VectorIndexMetadataNode(
        0L, 42L, 3, 128, "L2", 1000L, 5);

    final VectorIndexMetadataNode deserialized =
        serializeAndDeserializeMetadataNode(original);

    assertEquals(0L, deserialized.getNodeKey());
    assertEquals(42L, deserialized.getEntryPointKey());
    assertEquals(3, deserialized.getMaxLevel());
    assertEquals(128, deserialized.getDimension());
    assertEquals("L2", deserialized.getDistanceType());
    assertEquals(1000L, deserialized.getNodeCount());
    assertEquals(5, deserialized.getPreviousRevisionNumber());
    assertEquals(5, deserialized.getLastModifiedRevisionNumber());
    assertEquals(NodeKind.VECTOR_INDEX_METADATA, deserialized.getKind());
    assertNull(deserialized.getDeweyID());
    assertNull(deserialized.getDeweyIDAsBytes());
  }

  @Test
  public void testVectorIndexMetadataNodeEmptyGraph() {
    final VectorIndexMetadataNode original = new VectorIndexMetadataNode(
        0L, 256, "COSINE", 0);

    assertEquals(-1L, original.getEntryPointKey());
    assertEquals(-1, original.getMaxLevel());
    assertEquals(0L, original.getNodeCount());

    final VectorIndexMetadataNode deserialized =
        serializeAndDeserializeMetadataNode(original);

    assertEquals(0L, deserialized.getNodeKey());
    assertEquals(-1L, deserialized.getEntryPointKey());
    assertEquals(-1, deserialized.getMaxLevel());
    assertEquals(256, deserialized.getDimension());
    assertEquals("COSINE", deserialized.getDistanceType());
    assertEquals(0L, deserialized.getNodeCount());
    assertEquals(0, deserialized.getPreviousRevisionNumber());
  }

  @Test
  public void testVectorIndexMetadataNodeInnerProduct() {
    final VectorIndexMetadataNode original = new VectorIndexMetadataNode(
        0L, 99L, 5, 768, "INNER_PRODUCT", 50000L, 12);

    final VectorIndexMetadataNode deserialized =
        serializeAndDeserializeMetadataNode(original);

    assertEquals(99L, deserialized.getEntryPointKey());
    assertEquals(5, deserialized.getMaxLevel());
    assertEquals(768, deserialized.getDimension());
    assertEquals("INNER_PRODUCT", deserialized.getDistanceType());
    assertEquals(50000L, deserialized.getNodeCount());
    assertEquals(12, deserialized.getPreviousRevisionNumber());
  }

  @Test
  public void testVectorIndexMetadataNodeEquality() {
    final VectorIndexMetadataNode original = new VectorIndexMetadataNode(
        0L, 42L, 3, 128, "L2", 1000L, 5);
    final VectorIndexMetadataNode deserialized =
        serializeAndDeserializeMetadataNode(original);

    assertEquals(original, deserialized);
    assertEquals(original.hashCode(), deserialized.hashCode());
  }

  @Test
  public void testNodeKindLookupByByteId() {
    assertEquals(NodeKind.VECTOR_NODE, NodeKind.getKind((byte) 56));
    assertEquals(NodeKind.VECTOR_INDEX_METADATA, NodeKind.getKind((byte) 58));
  }

  @Test
  public void testVectorNodeGetId() {
    assertEquals((byte) 56, NodeKind.VECTOR_NODE.getId());
    assertEquals((byte) 58, NodeKind.VECTOR_INDEX_METADATA.getId());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVectorNodeDeweyIdDeserializeThrows() {
    NodeKind.VECTOR_NODE.deserializeDeweyID(null, null, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVectorNodeDeweyIdSerializeThrows() {
    NodeKind.VECTOR_NODE.serializeDeweyID(null, null, null, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVectorIndexMetadataDeweyIdDeserializeThrows() {
    NodeKind.VECTOR_INDEX_METADATA.deserializeDeweyID(null, null, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVectorIndexMetadataDeweyIdSerializeThrows() {
    NodeKind.VECTOR_INDEX_METADATA.serializeDeweyID(null, null, null, null);
  }

  @Test
  public void testVectorNodeSetNeighbors() {
    final float[] vector = {1.0f};
    final VectorNode node = new VectorNode(1L, 1L, vector, 1, 0);

    // Initially empty.
    assertEquals(0, node.getNeighborCount(0));
    assertEquals(0, node.getNeighborCount(1));

    // Set neighbors for layer 0.
    node.setNeighbors(0, new long[]{10L, 20L, 30L}, 3);
    assertEquals(3, node.getNeighborCount(0));
    assertEquals(10L, node.getNeighbors(0)[0]);
    assertEquals(20L, node.getNeighbors(0)[1]);
    assertEquals(30L, node.getNeighbors(0)[2]);

    // Set neighbors for layer 1.
    node.setNeighbors(1, new long[]{5L}, 1);
    assertEquals(1, node.getNeighborCount(1));
    assertEquals(5L, node.getNeighbors(1)[0]);
  }

  @Test
  public void testVectorIndexMetadataNodeMutators() {
    final VectorIndexMetadataNode node = new VectorIndexMetadataNode(0L, 128, "L2", 0);

    assertEquals(-1L, node.getEntryPointKey());
    assertEquals(-1, node.getMaxLevel());
    assertEquals(0L, node.getNodeCount());

    node.setEntryPointKey(42L);
    node.setMaxLevel(3);
    node.incrementNodeCount();
    node.incrementNodeCount();

    assertEquals(42L, node.getEntryPointKey());
    assertEquals(3, node.getMaxLevel());
    assertEquals(2L, node.getNodeCount());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVectorNodeNullVector() {
    new VectorNode(1L, 1L, null, 0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVectorNodeNegativeMaxLayer() {
    new VectorNode(1L, 1L, new float[]{1.0f}, -1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVectorIndexMetadataNodeZeroDimension() {
    new VectorIndexMetadataNode(0L, 0, "L2", 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testVectorIndexMetadataNodeNullDistanceType() {
    new VectorIndexMetadataNode(0L, 128, null, 0);
  }

  // ---- Helper methods ----

  private static VectorNode serializeAndDeserializeVectorNode(final VectorNode original) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.VECTOR_NODE.serialize(sink, original, CONFIG);

    final BytesIn<?> source = sink.asBytesIn();
    return (VectorNode) NodeKind.VECTOR_NODE.deserialize(
        source, original.getNodeKey(), null, CONFIG);
  }

  private static VectorIndexMetadataNode serializeAndDeserializeMetadataNode(
      final VectorIndexMetadataNode original) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.VECTOR_INDEX_METADATA.serialize(sink, original, CONFIG);

    final BytesIn<?> source = sink.asBytesIn();
    return (VectorIndexMetadataNode) NodeKind.VECTOR_INDEX_METADATA.deserialize(
        source, original.getNodeKey(), null, CONFIG);
  }
}
