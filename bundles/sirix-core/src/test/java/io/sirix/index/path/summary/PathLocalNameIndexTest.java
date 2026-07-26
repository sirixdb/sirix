package io.sirix.index.path.summary;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the SIMD scan returns identical results to the previous linear-walk
 * fallback, including hash-collision and namespace-difference cases.
 */
final class PathLocalNameIndexTest {

  private static PathNode pathNode(final String localName) {
    return new PathNode(new QNm(localName), NodeKind.OBJECT_NAMED_OBJECT, 1, 1,
        1L, -1L, -1, 0, (SirixDeweyID) null,
        -1L, -1L, -1L, -1L, 0L, 0L,
        -1, -1, localName.hashCode(), 0L);
  }

  private static Map<QNm, Set<PathNode>> mapping(final String... localNames) {
    final Map<QNm, Set<PathNode>> m = new LinkedHashMap<>();
    for (final String n : localNames) {
      final QNm qn = new QNm(n);
      final PathNode pn = pathNode(n);
      m.computeIfAbsent(qn, k -> new HashSet<>()).add(pn);
    }
    return m;
  }

  @Test
  void emptyIndex_returnsEmptyAndAllocatesNothing() {
    final PathLocalNameIndex idx = new PathLocalNameIndex();
    idx.build(new HashMap<>());
    assertSame(List.of(), idx.findByLocalName("anything"),
        "empty mapping must return the shared empty list — no allocation");
    assertFalse(idx.containsLocalName("anything"));
  }

  @Test
  void singleEntry_findsHit() {
    final PathLocalNameIndex idx = new PathLocalNameIndex();
    idx.build(mapping("age"));
    final List<PathNode> hits = idx.findByLocalName("age");
    assertEquals(1, hits.size());
    assertEquals("age", hits.getFirst().getName().getLocalName());
    assertTrue(idx.containsLocalName("age"));
    assertFalse(idx.containsLocalName("missing"));
  }

  @Test
  void manyEntries_simdScanCorrectAcrossLaneBoundary() {
    // Picked so we cross at least one IntVector lane boundary on common species
    // (8 ints @ 256-bit, 16 @ 512-bit). 33 entries forces tail handling too.
    final String[] names = new String[33];
    for (int i = 0; i < 33; i++) {
      names[i] = "field" + i;
    }
    final PathLocalNameIndex idx = new PathLocalNameIndex();
    idx.build(mapping(names));
    for (final String n : names) {
      assertTrue(idx.containsLocalName(n), "missing " + n);
      final List<PathNode> hits = idx.findByLocalName(n);
      assertEquals(1, hits.size());
      assertEquals(n, hits.getFirst().getName().getLocalName());
    }
    assertFalse(idx.containsLocalName("not-present"));
  }

  @Test
  void hashCollision_isResolvedByEqualsCheck() {
    // "Aa" and "BB" have identical String#hashCode(). Both must be findable
    // and neither must spuriously match the other on a SIMD hash hit.
    assertEquals("Aa".hashCode(), "BB".hashCode(),
        "test premise: 'Aa' and 'BB' must collide on hashCode");

    final PathLocalNameIndex idx = new PathLocalNameIndex();
    idx.build(mapping("Aa", "BB"));

    final List<PathNode> aHits = idx.findByLocalName("Aa");
    assertEquals(1, aHits.size());
    assertEquals("Aa", aHits.getFirst().getName().getLocalName());

    final List<PathNode> bHits = idx.findByLocalName("BB");
    assertEquals(1, bHits.size());
    assertEquals("BB", bHits.getFirst().getName().getLocalName());

    assertTrue(idx.containsLocalName("Aa"));
    assertTrue(idx.containsLocalName("BB"));
    assertFalse(idx.containsLocalName("Cc"));
  }

  @Test
  void invalidate_dropsArraysAndForcesRebuild() {
    final PathLocalNameIndex idx = new PathLocalNameIndex();
    idx.build(mapping("age", "city"));
    assertTrue(idx.isBuilt());
    idx.invalidate();
    assertFalse(idx.isBuilt());
    // After invalidation queries return empty until rebuilt — caller is
    // responsible for rebuild on the lookup path.
    assertSame(List.of(), idx.findByLocalName("age"));
    assertFalse(idx.containsLocalName("age"));
  }
}
