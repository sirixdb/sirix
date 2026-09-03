/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The coupling that makes the whole scheme readable: the ids a page RECORDED and the ids the sealed
 * dictionary MINTS must be the same numbers.
 *
 * <p>
 * They are produced by two independently written pieces of code — {@link SegmentScopedDictionaries}'
 * counter and {@link GlobalValueDictionaryWriter#intern}'s {@code entryCount + 1} — and if they ever
 * disagree, every page of the segment resolves to a plausible WRONG value with no exception anywhere.
 * That is the failure this test exists for; the dictionary-writing half needs a storage writer and is
 * exercised where the load runs.
 * </p>
 */
final class SegmentDictionaryIdAlignmentTest {

  private static final int URL_TAG = 7;

  private static Int2IntMap tags() {
    final Int2IntOpenHashMap map = new Int2IntOpenHashMap();
    map.put(URL_TAG, 0);
    return map;
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("interning a segment's values in id order reproduces exactly the ids the pages recorded")
  void theTwoIdSpacesAgree() {
    final SegmentScopedDictionaries segments = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries page = segments.viewFor(0);
    // Values arrive in a realistic shape: repeats interleaved, several pages of one segment.
    final List<String> arriving =
        List.of("http://c", "http://a", "http://c", "http://b", "http://a", "http://d", "http://b");
    final List<Integer> recorded = new ArrayList<>();
    for (final String value : arriving) {
      final byte[] bytes = utf8(value);
      recorded.add(page.idOf(URL_TAG, bytes, 0, bytes.length));
    }
    assertEquals(List.of(1, 2, 1, 3, 2, 4, 3), recorded, "arrival order, one id per distinct value");

    // Seal: intern the segment's values in ID ORDER, exactly as SegmentDictionaryFlusher does, and
    // assert the dictionary writer hands back the same numbers.
    final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(0, 1L << 20);
    try {
      int expected = 0;
      final Iterator<byte[]> values = segments.valuesOf(0, 0);
      while (values.hasNext()) {
        final byte[] value = values.next();
        expected++;
        assertEquals(expected, generation.intern(value, 0, value.length),
            "the sealed dictionary must mint id " + expected + " for the value the segment gave that id");
      }
      assertEquals(4, expected, "four distinct values");
      assertEquals(4, generation.entryCount());
    } finally {
      generation.release();
    }
  }

  @Test
  @DisplayName("interning is idempotent per value, which is why one pass over the id-ordered values suffices")
  void internIsOnePerDistinctValue() {
    final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(0, 1L << 20);
    try {
      final byte[] a = utf8("a");
      final byte[] b = utf8("b");
      assertEquals(1, generation.intern(a, 0, a.length));
      assertEquals(2, generation.intern(b, 0, b.length));
      assertEquals(1, generation.intern(a, 0, a.length), "a repeat takes its existing id, never a new one");
      assertEquals(2, generation.entryCount(), "and does not grow the dictionary");
    } finally {
      generation.release();
    }
  }

  @Test
  @DisplayName("a duplicate in the id-ordered stream is caught: it would silently shift every later id")
  void aDuplicateInTheStreamIsCaught() {
    // SegmentScopedDictionaries cannot produce this, but the flusher must not assume it: a duplicate
    // makes intern return an EARLIER id, so every later value would be written one id low and every
    // page of the segment would resolve to its neighbour's value.
    final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(0, 1L << 20);
    try {
      final byte[] a = utf8("a");
      assertEquals(1, generation.intern(a, 0, a.length));
      final int second = generation.intern(a, 0, a.length);
      assertTrue(second != 2, "the duplicate does NOT take the next id — which is exactly the drift");
      assertEquals(1, second);
    } finally {
      generation.release();
    }
    // And the guard the flusher applies rejects it rather than writing a shifted dictionary.
    final List<byte[]> withDuplicate = List.of(utf8("a"), utf8("a"), utf8("b"));
    assertThrows(IllegalStateException.class, () -> internExpectingSequence(withDuplicate),
        "the flusher's per-value id check must fail the load");
    // The same stream without the duplicate is accepted.
    assertEquals(2, internExpectingSequence(List.of(utf8("a"), utf8("b"))));
  }

  /** Drives the FLUSHER'S OWN guard, not a copy of it. */
  private static int internExpectingSequence(final List<byte[]> values) {
    final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(0, 1L << 20);
    try {
      return SegmentDictionaryFlusher.internInIdOrder(generation, 0, values.iterator());
    } finally {
      generation.release();
    }
  }

  @Test
  @DisplayName("the guard also refuses a hole: a segment read before all its pages were encoded")
  void aHoleIsRefused() {
    final List<byte[]> withHole = new ArrayList<>();
    withHole.add(utf8("a"));
    withHole.add(null);
    assertThrows(IllegalStateException.class, () -> internExpectingSequence(withHole),
        "a null at an id means the segment was drained too early");
    assertEquals(2, internExpectingSequence(List.of(utf8("a"), utf8("b"))), "and a whole stream is accepted");
  }

  @Test
  @DisplayName("segments seal independently, so each dictionary's ids start at 1 again")
  void eachSegmentStartsAtOne() {
    final SegmentScopedDictionaries segments = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries first = segments.viewFor(0);
    final GlobalStringDictionaries second = segments.viewFor(1024);
    for (final String value : List.of("x", "y")) {
      final byte[] bytes = utf8(value);
      first.idOf(URL_TAG, bytes, 0, bytes.length);
    }
    final byte[] z = utf8("z");
    second.idOf(URL_TAG, z, 0, z.length);

    for (long segment = 0; segment <= 1; segment++) {
      final GlobalValueDictionaryWriter generation = new GlobalValueDictionaryWriter(0, 1L << 20);
      try {
        int expected = 0;
        final Iterator<byte[]> values = segments.valuesOf(segment, 0);
        while (values.hasNext()) {
          final byte[] value = values.next();
          expected++;
          assertEquals(expected, generation.intern(value, 0, value.length),
              "segment " + segment + " numbers its own dictionary from 1");
        }
      } finally {
        generation.release();
      }
    }
  }
}
