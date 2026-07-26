/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * T1 round-trip test for {@link NodeKind#PROJECTION_INDEX_LEAF}. The
 * serialisation layer must be byte-exact across the size range we expect in
 * practice: empty leaves (placeholder), single byte, 20 KB (typical full
 * leaf) and 64 KB (conservative upper bound — a leaf with highly diverse
 * string dicts).
 */
final class ProjectionIndexRowGroupRecordTest {

  @Test
  void roundTrip_emptyPayload() {
    assertRoundTrip(17L, new byte[0]);
  }

  @Test
  void roundTrip_singleBytePayload() {
    assertRoundTrip(1L, new byte[] { (byte) 0xA7 });
  }

  @Test
  void roundTrip_typicalLeaf20KB() {
    assertRoundTrip(42L, deterministicPayload(20 * 1024, 0xBAADF00DL));
  }

  @Test
  void roundTrip_largeLeaf64KB() {
    assertRoundTrip(123L, deterministicPayload(64 * 1024, 0xDEADBEEFL));
  }

  @Test
  void kindIsProjectionIndexLeaf() {
    final ProjectionIndexRowGroupRecord rec = new ProjectionIndexRowGroupRecord(0L, new byte[] { 1, 2, 3 });
    assertSame(NodeKind.PROJECTION_INDEX_LEAF, rec.getKind());
    assertEquals(0L, rec.getNodeKey());
    assertArrayEquals(new byte[] { 1, 2, 3 }, rec.getPayload());
  }

  @Test
  void equalsAndHashCode_considerNodeKeyAndPayload() {
    final ProjectionIndexRowGroupRecord a = new ProjectionIndexRowGroupRecord(5L, new byte[] { 7, 8 });
    final ProjectionIndexRowGroupRecord b = new ProjectionIndexRowGroupRecord(5L, new byte[] { 7, 8 });
    final ProjectionIndexRowGroupRecord c = new ProjectionIndexRowGroupRecord(6L, new byte[] { 7, 8 });
    final ProjectionIndexRowGroupRecord d = new ProjectionIndexRowGroupRecord(5L, new byte[] { 7, 9 });
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotSame(a, b);
    assertEquals(false, a.equals(c));
    assertEquals(false, a.equals(d));
  }

  private static void assertRoundTrip(final long nodeKey, final byte[] payload) {
    final ProjectionIndexRowGroupRecord original = new ProjectionIndexRowGroupRecord(nodeKey, payload);
    final MemorySegmentBytesOut out = new MemorySegmentBytesOut(Math.max(16, payload.length + 8));
    NodeKind.PROJECTION_INDEX_LEAF.serialize(out, original, null);
    final byte[] encoded = out.toByteArray();

    // Wire format contract: int length prefix followed by exactly `length` bytes.
    // 4-byte int prefix + payload bytes = encoded length.
    assertEquals(4 + payload.length, encoded.length,
        "encoded frame must be 4-byte length prefix + payload bytes");

    final ByteArrayBytesIn in = new ByteArrayBytesIn(encoded);
    final DataRecord decoded = NodeKind.PROJECTION_INDEX_LEAF.deserialize(in, nodeKey, null, null);

    assertNotNull(decoded);
    assertSame(NodeKind.PROJECTION_INDEX_LEAF, decoded.getKind());
    assertEquals(nodeKey, decoded.getNodeKey());
    final ProjectionIndexRowGroupRecord asLeaf = (ProjectionIndexRowGroupRecord) decoded;
    assertArrayEquals(payload, asLeaf.getPayload(),
        "payload bytes must survive the serialise/deserialise round-trip byte-for-byte");
    assertEquals(original, asLeaf);
  }

  private static byte[] deterministicPayload(final int size, final long seed) {
    final byte[] out = new byte[size];
    new Random(seed).nextBytes(out);
    // Embed a marker at the head + tail so a size-mismatch bug is loud.
    if (size >= 4) {
      out[0] = (byte) 0xCA;
      out[1] = (byte) 0xFE;
      out[size - 2] = (byte) 0xBE;
      out[size - 1] = (byte) 0xEF;
    }
    return Arrays.copyOf(out, out.length);
  }
}
