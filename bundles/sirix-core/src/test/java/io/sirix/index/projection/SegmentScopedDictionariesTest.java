/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Segment-scoped ENCODE dictionaries: minted during the load, no pre-pass, no closed corpus.
 *
 * <p>
 * The property that carries the design is not the minting — it is that a page's segment is bound to
 * the PAGE and not to a moment, because record pages are encoded on a pool and a page of segment N
 * can be encoded after the writer has moved to N + 1.
 * </p>
 */
final class SegmentScopedDictionariesTest {

  private static final int URL_TAG = 7;

  private static final int TITLE_TAG = 9;

  private static Int2IntMap tags() {
    final Int2IntOpenHashMap map = new Int2IntOpenHashMap();
    map.put(URL_TAG, 0);
    map.put(TITLE_TAG, 1);
    return map;
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static int id(final GlobalStringDictionaries view, final int tag, final String value) {
    final byte[] bytes = utf8(value);
    return view.idOf(tag, bytes, 0, bytes.length);
  }

  private static List<String> values(final SegmentScopedDictionaries dictionaries, final long segment,
      final int column) {
    final List<String> out = new ArrayList<>();
    final Iterator<byte[]> iterator = dictionaries.valuesOf(segment, column);
    while (iterator.hasNext()) {
      out.add(new String(iterator.next(), StandardCharsets.UTF_8));
    }
    return out;
  }

  @Test
  @DisplayName("a value is minted once per segment, and its id is stable for the rest of that segment")
  void mintsOncePerSegment() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries page0 = dictionaries.viewFor(0);
    final GlobalStringDictionaries page1 = dictionaries.viewFor(1); // same segment: 1/1024 == 0

    final int a = id(page0, URL_TAG, "http://a");
    assertEquals(1, a, "ids are 1-based: 0 is ID_ABSENT");
    assertEquals(a, id(page0, URL_TAG, "http://a"), "the same value on the same page keeps its id");
    assertEquals(a, id(page1, URL_TAG, "http://a"), "and on another page of the SAME segment");
    assertEquals(2, id(page1, URL_TAG, "http://b"), "a new value takes the next id");
    assertEquals(2, dictionaries.entryCount(0, 0));
    assertEquals(List.of("http://a", "http://b"), values(dictionaries, 0, 0), "values come back in ID order");
  }

  @Test
  @DisplayName("columns are independent within a segment: the same bytes in two columns are two dictionaries")
  void columnsAreIndependent() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries view = dictionaries.viewFor(0);
    // DISTINCT values per column: with one shared id space "title-only" would take id 2, so this is
    // what separates the two dictionaries. (The same value in both columns cannot: a shared space
    // would hand it id 1 as well, which is why an earlier version of this test proved nothing.)
    assertEquals(1, id(view, URL_TAG, "url-only"));
    assertEquals(1, id(view, TITLE_TAG, "title-only"), "each column mints its own id space, from 1");
    assertEquals(2, id(view, URL_TAG, "url-second"), "and advances only for its own values");
    assertEquals(2, id(view, TITLE_TAG, "title-second"));
    assertEquals(2, dictionaries.entryCount(0, 0));
    assertEquals(2, dictionaries.entryCount(0, 1));
    assertEquals(List.of("url-only", "url-second"), values(dictionaries, 0, 0));
    assertEquals(List.of("title-only", "title-second"), values(dictionaries, 0, 1));
    // The same bytes in two columns are two entries, one per column.
    assertEquals(3, id(view, URL_TAG, "shared"));
    assertEquals(3, id(view, TITLE_TAG, "shared"));
    assertEquals(3, dictionaries.entryCount(0, 0));
    assertEquals(3, dictionaries.entryCount(0, 1));
  }

  @Test
  @DisplayName("segments are independent: the same value in two segments is minted twice, which is the trade")
  void segmentsAreIndependent() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries first = dictionaries.viewFor(0);
    final GlobalStringDictionaries second = dictionaries.viewFor(1024); // the next segment

    assertEquals(1L, first.dictionaryKey(URL_TAG), "segment 0 anchors as 1: zero is the no-dictionary sentinel");
    assertEquals(2L, second.dictionaryKey(URL_TAG), "the anchor is the SEGMENT id plus one");
    assertEquals(1, id(first, URL_TAG, "http://a"));
    assertEquals(1, id(second, URL_TAG, "http://a"),
        "a repeated value in a later segment is minted again — the 11.2 % a global dictionary would save");
    assertEquals(1, dictionaries.entryCount(0, 0));
    assertEquals(1, dictionaries.entryCount(1, 0));
    assertEquals(2, dictionaries.liveDictionaryCount());
  }

  @Test
  @DisplayName("THE HAZARD: a page's view answers for ITS segment however late the flush pool encodes it")
  void aViewIsBoundToItsPageNotToTheMoment() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    // The writer hands out a view for a page of segment 0, then moves on and fills segments 1 and 2 —
    // exactly what happens while that page still sits in the async flush queue.
    final GlobalStringDictionaries latePage = dictionaries.viewFor(5);
    for (long page = 1024; page < 3 * 1024; page += 512) {
      id(dictionaries.viewFor(page), URL_TAG, "later-" + page);
    }
    // Only NOW is the stranded page encoded.
    assertEquals(1L, latePage.dictionaryKey(URL_TAG), "its anchor is still segment 0 (encoded as 1)");
    assertEquals(1, id(latePage, URL_TAG, "http://stranded"), "and it mints into segment 0's dictionary");
    assertEquals(1, dictionaries.entryCount(0, 0), "segment 0 has exactly the stranded page's value");
    assertEquals(List.of("http://stranded"), values(dictionaries, 0, 0));
    // A resolver keyed on "the segment being filled" would have put this value — and this anchor — in
    // segment 2, on a page whose neighbours point at 0. Both dictionaries are live and large enough,
    // so the reader's entry-count check could not have caught it. Assert the value is NOT there.
    assertTrue(!values(dictionaries, 2, 0).contains("http://stranded"),
        "the stranded page's value must not land in the segment the writer had moved on to");
    assertEquals(List.of("later-2048", "later-2560"), values(dictionaries, 2, 0),
        "segment 2 holds exactly what pages of segment 2 minted");
  }

  @Test
  @DisplayName("the recorded entry count never exceeds the live one, which is what the reader's check needs")
  void recordedCountsOnlyGrow() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries view = dictionaries.viewFor(0);
    final List<Integer> recorded = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      id(view, URL_TAG, "v" + i);
      recorded.add(view.dictionaryEntryCount(URL_TAG));
    }
    for (int i = 1; i < recorded.size(); i++) {
      assertTrue(recorded.get(i) >= recorded.get(i - 1), "a segment dictionary only ever appends");
    }
    assertEquals(50, recorded.get(recorded.size() - 1));
    assertTrue(view.dictionaryEntryCount(URL_TAG) >= recorded.get(recorded.size() - 1),
        "the live count is never below anything a page recorded");
  }

  @Test
  @DisplayName("an unprojected tag mints nothing: the page keeps its bytes")
  void anUnprojectedTagIsRefused() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries view = dictionaries.viewFor(0);
    assertTrue(view.hasDictionary(URL_TAG));
    assertTrue(!view.hasDictionary(4242));
    assertEquals(GlobalStringDictionaries.ID_ABSENT, id(view, 4242, "unprojected"));
    assertEquals(0, view.dictionaryEntryCount(4242));
    assertEquals(0, dictionaries.liveDictionaryCount(), "nothing was created for a tag with no column");
  }

  @Test
  @DisplayName("the decode direction always refuses: an encoder never turns an id back into bytes")
  void theDecodeDirectionRefuses() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final GlobalStringDictionaries view = dictionaries.viewFor(0);
    id(view, URL_TAG, "http://a");
    assertNull(view.valueOf(URL_TAG, 0L, 1, 1));
    assertTrue(!view.accepts(URL_TAG, 0L, 1));
  }

  @Test
  @DisplayName("concurrent encoders of one segment agree on every id: no value gets two, no id gets two values")
  void concurrentMintingIsConsistent() throws Exception {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    final int threads = 8;
    final int values = 500;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    // [thread][value] -> the id that thread saw, so disagreement is visible rather than averaged away.
    final AtomicReferenceArray<int[]> seen = new AtomicReferenceArray<>(threads);
    try {
      for (int t = 0; t < threads; t++) {
        final int worker = t;
        pool.submit(() -> {
          final int[] mine = new int[values];
          // Every worker encodes a DIFFERENT page of the SAME segment, which is the real shape.
          final GlobalStringDictionaries view = dictionaries.viewFor(worker);
          start.await();
          for (int v = 0; v < values; v++) {
            mine[v] = id(view, URL_TAG, "http://v" + v);
          }
          seen.set(worker, mine);
          return null;
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers finished");
    } finally {
      pool.shutdownNow();
    }
    final int[] reference = seen.get(0);
    assertNotNull(reference, "worker 0 recorded its ids");
    for (int t = 1; t < threads; t++) {
      assertArrayEqualsWithLabel(reference, seen.get(t), t);
    }
    final Set<Integer> distinct = new HashSet<>();
    for (final int assigned : reference) {
      assertTrue(assigned >= 1, "every value was minted");
      distinct.add(assigned);
    }
    assertEquals(values, distinct.size(), "no id was handed to two different values");
    assertEquals(values, dictionaries.entryCount(0, 0));
    assertEquals(values, values(dictionaries, 0, 0).size(), "and every one comes back in id order");
    assertEquals(1, dictionaries.liveDictionaryCount(), "all eight pages shared one segment dictionary");
  }

  private static void assertArrayEqualsWithLabel(final int[] expected, final int[] actual, final int worker) {
    assertEquals(expected.length, actual.length, "worker " + worker);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i], "worker " + worker + " disagrees about value " + i);
    }
  }

  @Test
  @DisplayName("the segment boundary is a power of two and a page's segment is derived from its key")
  void segmentBoundaries() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(1024, tags());
    assertEquals(0L, dictionaries.segmentOf(0));
    assertEquals(0L, dictionaries.segmentOf(1023));
    assertEquals(1L, dictionaries.segmentOf(1024));
    assertEquals(9L, dictionaries.segmentOf(1024L * 9 + 7));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.segmentOf(-1));
    assertThrows(IllegalArgumentException.class, () -> new SegmentScopedDictionaries(1000, tags()));
    assertThrows(IllegalArgumentException.class, () -> new SegmentScopedDictionaries(0, tags()));
    assertNotEquals(dictionaries.viewFor(0).dictionaryKey(URL_TAG), dictionaries.viewFor(1024).dictionaryKey(URL_TAG));
  }

  @Test
  @DisplayName("a tag map published mid-load is seen by later encodes; a tag absent when a page encoded is not fatal")
  void tagsMayBePublishedMidLoad() {
    final SegmentScopedDictionaries dictionaries =
        new SegmentScopedDictionaries(1024, SegmentScopedDictionaries.noTags());
    final GlobalStringDictionaries view = dictionaries.viewFor(0);
    assertEquals(GlobalStringDictionaries.ID_ABSENT, id(view, URL_TAG, "early"),
        "a path class the load has not resolved yet keeps its bytes");
    dictionaries.publishTags(tags());
    assertEquals(1, id(view, URL_TAG, "late"), "the same view sees the republished map");
    assertEquals(List.of("late"), values(dictionaries, 0, 0));
  }
}
