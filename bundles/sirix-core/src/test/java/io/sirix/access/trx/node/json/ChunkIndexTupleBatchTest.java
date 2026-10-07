/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

final class ChunkIndexTupleBatchTest {

  @Test
  void casOnlyScalarFieldsDoNotAllocatePathObservations() {
    final ChunkIndexTupleBatch batch = new ChunkIndexTupleBatch(false, null, true, null, false, null);
    for (int i = 0; i < 1_000; i++) {
      batch.onPathEntry(1, i, NodeKind.OBJECT_NAMED_NUMBER);
      batch.onPathEntry(2, i, NodeKind.OBJECT_NAMED_STRING);
      batch.onPathEntry(3, i, NodeKind.OBJECT_NAMED_BOOLEAN);
      batch.onCasInt(1, i, i);
    }
    assertEquals(0, batch.pathEntryCount());
    assertEquals(1_000, batch.casEntryCount());
    batch.onPathEntry(4, 4, NodeKind.OBJECT_NAMED_NULL);
    batch.onPathEntry(5, 5, NodeKind.OBJECT_NAMED_OBJECT);
    batch.onPathEntry(6, 6, NodeKind.OBJECT_NAMED_ARRAY);
    batch.onPathEntry(7, 7, NodeKind.ARRAY);
    assertEquals(4, batch.pathEntryCount());
    assertEquals(NodeKind.OBJECT_NAMED_NULL, batch.pathKindAt(0));
    assertEquals(NodeKind.OBJECT_NAMED_OBJECT, batch.pathKindAt(1));
    assertEquals(NodeKind.OBJECT_NAMED_ARRAY, batch.pathKindAt(2));
    assertEquals(NodeKind.ARRAY, batch.pathKindAt(3));
    assertEquals(0, batch.mirrorCandidateCount());
  }

  @Test
  void mixedPathAndCasFeedsRetainFilteredScalarPathsAndArrayMirrors() {
    final LongOpenHashSet paths = new LongOpenHashSet(new long[] {2});
    final ChunkIndexTupleBatch batch = new ChunkIndexTupleBatch(true, paths, true, null, false, null);
    batch.onPathEntry(1, 11, NodeKind.OBJECT_NAMED_NUMBER);
    batch.onPathEntry(2, 12, NodeKind.OBJECT_NAMED_STRING);
    batch.onPathEntry(3, 13, NodeKind.OBJECT_NAMED_ARRAY);
    batch.onNamedArrayMirrorCandidate(3, 13);
    assertEquals(2, batch.pathEntryCount());
    assertEquals(2, batch.pathPcrAt(0));
    assertEquals(12, batch.pathNodeKeyAt(0));
    assertEquals(NodeKind.OBJECT_NAMED_STRING, batch.pathKindAt(0));
    assertEquals(3, batch.pathPcrAt(1));
    assertEquals(1, batch.mirrorCandidateCount());
    assertEquals(3, batch.mirrorArrayPcrAt(0));
    assertEquals(13, batch.mirrorNodeKeyAt(0));
  }

  @Test
  void mixedFallbackIntAndLongTuplesKeepIndependentDenseSideLaneOrder() {
    final ChunkIndexTupleBatch batch = new ChunkIndexTupleBatch(false, null, true, null, false, null);
    final BigInteger bigInteger = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    final BigDecimal decimal = new BigDecimal("1.25");

    batch.onCasNumber(11L, 101L, bigInteger);
    batch.onCasInt(12L, 102L, Integer.MIN_VALUE);
    batch.onCasLong(13L, 103L, Long.MIN_VALUE);
    batch.onCasNumber(14L, 104L, decimal);
    batch.onCasInt(15L, 105L, Integer.MAX_VALUE);
    batch.onCasLong(16L, 106L, Long.MAX_VALUE);

    assertEquals(6, batch.casEntryCount());
    assertTuple(batch, 0, ChunkIndexTupleBatch.CAS_KIND_NUMBER, 11L, 101L);
    assertTuple(batch, 1, ChunkIndexTupleBatch.CAS_KIND_INT, 12L, 102L);
    assertTuple(batch, 2, ChunkIndexTupleBatch.CAS_KIND_LONG, 13L, 103L);
    assertTuple(batch, 3, ChunkIndexTupleBatch.CAS_KIND_NUMBER, 14L, 104L);
    assertTuple(batch, 4, ChunkIndexTupleBatch.CAS_KIND_INT, 15L, 105L);
    assertTuple(batch, 5, ChunkIndexTupleBatch.CAS_KIND_LONG, 16L, 106L);

    assertSame(bigInteger, batch.casNumberAt(0));
    assertSame(decimal, batch.casNumberAt(1));
    assertEquals(Integer.MIN_VALUE, batch.casIntegralNumberAt(0));
    assertEquals(Long.MIN_VALUE, batch.casIntegralNumberAt(1));
    assertEquals(Integer.MAX_VALUE, batch.casIntegralNumberAt(2));
    assertEquals(Long.MAX_VALUE, batch.casIntegralNumberAt(3));
  }

  private static void assertTuple(final ChunkIndexTupleBatch batch, final int index, final byte expectedKind,
      final long expectedPcr, final long expectedNodeKey) {
    assertEquals(expectedKind, batch.casKindAt(index));
    assertEquals(expectedPcr, batch.casPcrAt(index));
    assertEquals(expectedNodeKey, batch.casNodeKeyAt(index));
  }
}
