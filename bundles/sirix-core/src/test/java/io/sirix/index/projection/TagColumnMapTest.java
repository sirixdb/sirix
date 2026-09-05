/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one conflict rule the writer and the reader share. The property that matters is that both
 * sides, fed the same claims in ANY order and in any number of refreshes, build the same map — and
 * that a tag two columns claim is in neither.
 */
final class TagColumnMapTest {

  @Test
  @DisplayName("a tag claimed by one column maps to it; a tag claimed by two is absent")
  void singleClaimsResolveAndDoubleClaimsAreWithheld() {
    final TagColumnMap claims = new TagColumnMap(4);
    assertTrue(claims.claim(7L, 0));
    assertTrue(claims.claim(9L, 1));
    assertTrue(claims.claim(11L, 0));
    assertTrue(claims.claim(11L, 1)); // the same path class under a second field path
    final Int2IntMap tags = claims.build();
    assertEquals(2, tags.size());
    assertEquals(0, tags.get(7));
    assertEquals(1, tags.get(9));
    assertFalse(tags.containsKey(11), "a contested tag keeps its bytes on both sides");
    assertEquals(TagColumnMap.NO_COLUMN, tags.get(11), "even a get without containsKey reads no column");
    assertTrue(claims.isContested(11));
    assertFalse(claims.isContested(7));
    assertEquals(1, claims.contestedCount());
    assertEquals(2, claims.resolvedCount());
  }

  @Test
  @DisplayName("the same column claiming a tag twice is one claim, not a contest")
  void aRepeatedClaimBySameColumnIsNotAContest() {
    final TagColumnMap claims = new TagColumnMap(2);
    claims.claim(7L, 3);
    claims.claim(7L, 3);
    claims.claim(7L, 3);
    assertFalse(claims.isContested(7));
    assertEquals(0, claims.contestedCount());
    assertEquals(3, claims.build().get(7));
  }

  @Test
  @DisplayName("a contested tag stays contested: the original claimant cannot take it back")
  void contestIsSticky() {
    final TagColumnMap claims = new TagColumnMap(2);
    claims.claim(7L, 0);
    claims.claim(7L, 1);
    claims.claim(7L, 0);
    claims.claim(7L, 0);
    claims.claim(7L, 2);
    assertTrue(claims.isContested(7));
    assertEquals(1, claims.contestedCount(), "one contested tag, however many claims pile onto it");
    assertTrue(claims.build().isEmpty());
  }

  @Test
  @DisplayName("claim order does not matter: writer and reader feed the same claims in different orders")
  void theVerdictIsOrderIndependent() {
    final long[][] writerOrder = {{7L, 0}, {8L, 0}, {9L, 1}, {8L, 1}, {10L, 2}};
    final long[][] readerOrder = {{10L, 2}, {8L, 1}, {9L, 1}, {8L, 0}, {7L, 0}};
    final Int2IntMap writer = build(writerOrder);
    final Int2IntMap reader = build(readerOrder);
    assertEquals(writer, reader);
    assertEquals(3, writer.size());
    assertFalse(writer.containsKey(8));
  }

  @Test
  @DisplayName("a rebuilt map with the same claims plus new ones agrees with the earlier map on every old tag")
  void aRefreshOnlyAddsOrWithholds() {
    final TagColumnMap claims = new TagColumnMap(4);
    claims.claim(7L, 0);
    claims.claim(9L, 1);
    final Int2IntMap first = claims.build();
    claims.claim(12L, 2);
    claims.claim(9L, 2); // 9 becomes contested on refresh: it was a second field path's class
    final Int2IntMap second = claims.build();
    assertEquals(0, second.get(7), "an uncontested tag keeps its column across refreshes");
    assertEquals(2, second.get(12));
    assertFalse(second.containsKey(9));
    assertEquals(1, first.get(9), "the earlier snapshot is untouched: it is a published, immutable map");
  }

  @Test
  @DisplayName("keys outside the int tag range are ignored, never mapped, never counted")
  void outOfRangeKeysAreIgnored() {
    final TagColumnMap claims = new TagColumnMap(2);
    assertFalse(claims.claim(0L, 0));
    assertFalse(claims.claim(-5L, 0));
    assertFalse(claims.claim((long) Integer.MAX_VALUE + 1L, 0));
    assertTrue(claims.claim(Integer.MAX_VALUE, 0));
    assertEquals(1, claims.resolvedCount());
    final Int2IntMap tags = claims.build();
    assertEquals(1, tags.size());
    assertEquals(0, tags.get(Integer.MAX_VALUE));
    assertFalse(tags.containsKey(0));
  }

  @Test
  @DisplayName("the built map is unmodifiable and empty when nothing resolved")
  void theBuiltMapIsSafeToPublish() {
    final TagColumnMap claims = new TagColumnMap(0);
    assertTrue(claims.build().isEmpty());
    assertEquals(TagColumnMap.NO_COLUMN, claims.build().get(7), "an empty map answers no column either");
    claims.claim(7L, 0);
    claims.claim(7L, 1);
    assertTrue(claims.build().isEmpty(), "only contested tags: nothing to publish");
    claims.claim(8L, 1);
    final Int2IntMap tags = claims.build();
    assertThrows(UnsupportedOperationException.class, () -> tags.put(9, 0));
    assertThrows(UnsupportedOperationException.class, () -> tags.remove(8));
    assertEquals(1, tags.size());
  }

  @Test
  @DisplayName("negative columns and a negative expectation are refused")
  void contractViolations() {
    assertThrows(IllegalArgumentException.class, () -> new TagColumnMap(-1));
    final TagColumnMap claims = new TagColumnMap(1);
    assertThrows(IllegalArgumentException.class, () -> claims.claim(7L, -1));
    assertTrue(claims.build().isEmpty(), "the refused claim recorded nothing");
  }

  private static Int2IntMap build(final long[][] claimsInOrder) {
    final TagColumnMap claims = new TagColumnMap(claimsInOrder.length);
    for (final long[] claim : claimsInOrder) {
      claims.claim(claim[0], (int) claim[1]);
    }
    return claims.build();
  }
}
