/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.SegmentScopedDictionaries.ColumnDictionary;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

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

  /**
   * A segment per 1024 adopted keys and no byte budget: the shape every test but the budget one
   * wants, so a page's segment is its key's high bits and the assertions read as arithmetic.
   */
  private static final long LEAVES_PER_SEGMENT = 1024;

  private static SegmentScopedDictionaries leafCapped() {
    return new SegmentScopedDictionaries(new SegmentBoundaries(Long.MAX_VALUE, LEAVES_PER_SEGMENT), tags());
  }

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

  private static List<String> values(final SegmentScopedDictionaries dictionaries, final int segment,
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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries page0 = dictionaries.adopt(0);
    final GlobalStringDictionaries page1 = dictionaries.adopt(1); // same segment: 1/1024 == 0

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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries first = dictionaries.adopt(0);
    final GlobalStringDictionaries second = dictionaries.adopt(1024); // the next segment

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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    // The writer adopts segment 0's first page, hands out a view for another page of segment 0, then
    // moves on and fills segments 1 and 2 — exactly what happens while that page still sits in the
    // async flush queue.
    dictionaries.adopt(0);
    final GlobalStringDictionaries latePage = dictionaries.adopt(5);
    for (long page = 1024; page < 3 * 1024; page += 512) {
      id(dictionaries.adopt(page), URL_TAG, "later-" + page);
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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
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
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    assertTrue(view.hasDictionary(URL_TAG));
    assertTrue(!view.hasDictionary(4242));
    assertEquals(GlobalStringDictionaries.ID_ABSENT, id(view, 4242, "unprojected"));
    assertEquals(0, view.dictionaryEntryCount(4242));
    assertEquals(0L, view.dictionaryKey(4242),
        "and it reports NO dictionary key: a non-zero anchor would send the reader to this segment's"
            + " dictionary for a tag that never minted into it");
    assertEquals(1L, view.dictionaryKey(URL_TAG), "while a projected tag anchors at its segment");
    assertEquals(0, dictionaries.liveDictionaryCount(), "nothing was created for a tag with no column");
  }

  @Test
  @DisplayName("the decode direction always refuses: an encoder never turns an id back into bytes")
  void theDecodeDirectionRefuses() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    id(view, URL_TAG, "http://a");
    assertNull(view.valueOf(URL_TAG, 0L, 1, 1));
    assertTrue(!view.accepts(URL_TAG, 0L, 1));
  }

  @Test
  @DisplayName("concurrent encoders of one segment agree on every id: no value gets two, no id gets two values")
  void concurrentMintingIsConsistent() throws Exception {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final int threads = 8;
    final int values = 500;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    // [thread][value] -> the id that thread saw, so disagreement is visible rather than averaged away.
    final AtomicReferenceArray<int[]> seen = new AtomicReferenceArray<>(threads);
    // Adoption is the writer thread's; the pool only ENCODES. Every worker encodes a DIFFERENT page
    // of the SAME segment, which is the real shape.
    final GlobalStringDictionaries[] views = new GlobalStringDictionaries[threads];
    for (int t = 0; t < threads; t++) {
      views[t] = dictionaries.adopt(t);
    }
    final List<Future<?>> outcomes = new ArrayList<>(threads);
    try {
      for (int t = 0; t < threads; t++) {
        final int worker = t;
        outcomes.add(pool.submit(() -> {
          final int[] mine = new int[values];
          final GlobalStringDictionaries view = views[worker];
          start.await();
          for (int v = 0; v < values; v++) {
            mine[v] = id(view, URL_TAG, "http://v" + v);
            // What the string region checks after encoding a run: the count it records covers every id.
            if (view.dictionaryEntryCount(URL_TAG) < mine[v]) {
              throw new IllegalStateException("id " + mine[v] + " above the entry count");
            }
          }
          seen.set(worker, mine);
          return null;
        }));
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers finished");
    } finally {
      pool.shutdownNow();
    }
    for (final Future<?> outcome : outcomes) {
      outcome.get(); // a worker's exception, with its message, rather than a null array later
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

  @Test
  @DisplayName("concurrent encoders across several REHASHES still agree: no duplicate id, no lost value, dense ids")
  void concurrentMintingSurvivesRehashes() throws Exception {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final int threads = 12;
    // 6000 distinct values through a 1024-slot table at load factor 1/2: three rehashes and one
    // value-array doubling while every thread probes; each thread walks the values in its own order
    // so that misses, hits and rehashes interleave.
    final int distinct = 6000;
    // Strides coprime to 6000 = 2^4 * 3 * 5^3, so each worker's walk is a permutation of the values.
    final int[] strides = {7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47};
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicReferenceArray<int[]> seen = new AtomicReferenceArray<>(threads);
    final GlobalStringDictionaries[] views = new GlobalStringDictionaries[threads];
    for (int t = 0; t < threads; t++) {
      views[t] = dictionaries.adopt(t);
    }
    final List<Future<?>> outcomes = new ArrayList<>(threads);
    try {
      for (int t = 0; t < threads; t++) {
        final int worker = t;
        outcomes.add(pool.submit(() -> {
          final int[] mine = new int[distinct];
          final GlobalStringDictionaries view = views[worker];
          start.await();
          for (int step = 0; step < distinct; step++) {
            // A different permutation per worker: (step * stride) mod distinct.
            final int v = (int) (((long) step * strides[worker]) % distinct);
            mine[v] = id(view, URL_TAG, "http://value-" + v);
            if (view.dictionaryEntryCount(URL_TAG) < mine[v]) {
              throw new IllegalStateException("id " + mine[v] + " above the entry count");
            }
          }
          seen.set(worker, mine);
          return null;
        }));
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(120, TimeUnit.SECONDS), "workers finished");
    } finally {
      pool.shutdownNow();
    }
    for (final Future<?> outcome : outcomes) {
      outcome.get();
    }
    final int[] reference = seen.get(0);
    assertNotNull(reference, "worker 0 recorded its ids");
    for (int t = 1; t < threads; t++) {
      assertArrayEqualsWithLabel(reference, seen.get(t), t);
    }
    final boolean[] taken = new boolean[distinct + 1];
    for (final int assigned : reference) {
      assertTrue(assigned >= 1 && assigned <= distinct, "ids are dense in 1.." + distinct + ", got " + assigned);
      assertTrue(!taken[assigned], "id " + assigned + " was handed to two values");
      taken[assigned] = true;
    }
    assertEquals(distinct, dictionaries.entryCount(0, 0), "exactly one id per distinct value");
    final byte[][] byId = dictionaries.valuesById(0, 0);
    assertEquals(distinct, byId.length);
    for (int v = 0; v < distinct; v++) {
      assertEquals("http://value-" + v, new String(byId[reference[v] - 1], StandardCharsets.UTF_8),
          "the value behind id " + reference[v] + " is the one every worker was given that id for");
    }
    assertEquals(1, dictionaries.liveDictionaryCount());
  }

  private static void assertArrayEqualsWithLabel(final int[] expected, final int[] actual, final int worker) {
    assertEquals(expected.length, actual.length, "worker " + worker);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i], "worker " + worker + " disagrees about value " + i);
    }
  }

  @Test
  @DisplayName("a segment closes by MINTED BYTES: the first page adopted after the budget opens the next one")
  void aSegmentClosesByMintedBytes() {
    final SegmentScopedDictionaries dictionaries =
        new SegmentScopedDictionaries(new SegmentBoundaries(16, Long.MAX_VALUE), tags());
    final SegmentScopedDictionaries.SegmentView page0 = dictionaries.adopt(0);
    assertEquals(0, page0.segment());
    assertEquals(0L, dictionaries.mintedBytes(0));
    assertEquals(1, id(page0, URL_TAG, "0123456"));
    assertEquals(1, id(page0, URL_TAG, "0123456"));
    assertEquals(7L, dictionaries.mintedBytes(0), "only a MISS is charged, by its length; a hit mints nothing");
    assertEquals(0, dictionaries.adopt(1).segment(), "7 < 16: the segment is still open");
    assertEquals(1, id(page0, TITLE_TAG, "abcdefghi"));
    assertEquals(16L, dictionaries.mintedBytes(0), "the budget is per SEGMENT, summed over its columns");
    final SegmentScopedDictionaries.SegmentView page2 = dictionaries.adopt(2);
    assertEquals(1, page2.segment(), "the budget was reached, so the next adopted page opens segment 1");
    assertEquals(2L, page2.dictionaryKey(URL_TAG), "and anchors as segment + 1");
    assertEquals(1, id(page2, URL_TAG, "0123456"), "the same value is minted afresh in the new segment");
    assertEquals(0, dictionaries.segmentOf(0));
    assertEquals(0, dictionaries.segmentOf(1), "a page adopted before the close stays in segment 0");
    assertEquals(1, dictionaries.segmentOf(2));
    assertEquals(1, dictionaries.segmentOf(4242), "a key past every adopted page reads as the open segment");
    assertEquals(7L, dictionaries.mintedBytes(1), "segment 1 holds exactly the bytes ITS pages minted");
    assertEquals(16L, dictionaries.mintedBytes(0), "and segment 0 keeps its own");
    assertEquals(2, dictionaries.liveSegmentCount());
    assertThrows(IllegalArgumentException.class, () -> dictionaries.segmentOf(-1));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.adopt(-1));
  }

  @Test
  @DisplayName("a segment closes by LEAVES: the page a leaf cap past the segment's first page opens the next one")
  void aSegmentClosesByLeaves() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    assertEquals(0, dictionaries.adopt(0).segment());
    assertEquals(0, dictionaries.adopt(1023).segment(), "1023 - 0 < 1024: still segment 0");
    assertEquals(1, dictionaries.adopt(1024).segment(), "1024 - 0 == 1024: the cap is reached");
    assertEquals(1, dictionaries.adopt(1024L * 2 - 1).segment(), "the cap counts from the segment's FIRST page");
    assertEquals(2, dictionaries.adopt(1024L * 9 + 7).segment(),
        "a jump in keys closes at most ONE segment: segments are made of adopted pages, not of key ranges");
    assertEquals(2, dictionaries.segmentOf(1024L * 9 + 7));
    assertEquals(0, dictionaries.segmentOf(7), "an unadopted key below the first boundary reads as segment 0");
    assertNotEquals(dictionaries.adopt(3).dictionaryKey(URL_TAG),
        dictionaries.adopt(1024L * 9 + 8).dictionaryKey(URL_TAG),
        "a late page of segment 0 and a page of the open segment anchor differently");
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(0, 1024));
    assertThrows(IllegalArgumentException.class, () -> new SegmentBoundaries(1024, 0));
  }

  @Test
  @DisplayName("a released segment refuses a late mint loudly: its dictionary is sealed and gone")
  void aReleasedSegmentRefusesLateMints() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries page = dictionaries.adopt(0);
    assertEquals(1, id(page, URL_TAG, "http://a"));
    assertEquals(1, dictionaries.adopt(1024).segment());
    assertEquals(List.of("http://a"), values(dictionaries, 0, 0), "the seal reads the values BEFORE releasing");
    dictionaries.release(0);
    // The page's view had already resolved its column, exactly as a page mid-encode has: the refusal
    // must hold on that cached path, not only on the segment's column lookup.
    assertThrows(IllegalStateException.class, () -> id(page, URL_TAG, "http://late"),
        "a mint into a released segment would start a second dictionary nobody seals");
    assertThrows(IllegalStateException.class, () -> id(page, URL_TAG, "http://a"),
        "even a value the segment HAD is refused: the mint map is gone, so a hit cannot be told from a miss");
    assertThrows(IllegalStateException.class, () -> id(page, TITLE_TAG, "t"),
        "and so is a column the segment never had");
    assertThrows(IllegalStateException.class, () -> dictionaries.valuesById(0, 0), "the values are gone with it");
    assertThrows(IllegalStateException.class, () -> dictionaries.adopt(7),
        "and a late page cannot be adopted into a released segment");
    assertEquals(1, dictionaries.entryCount(0, 0), "the sealed COUNT survives the release: the directory records it");
    assertEquals(1, page.dictionaryEntryCount(URL_TAG));
    assertEquals(1, id(dictionaries.adopt(1025), URL_TAG, "http://a"), "the open segment is unaffected");
  }

  @Test
  @DisplayName("the mint table survives growth: ids stay dense and every value reads back past the initial capacities")
  void theMintTableGrows() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    // Past INITIAL_VALUES (512) values and INITIAL_SLOTS (1024) slots at load factor 1/2: several
    // rehashes.
    final int distinct = 5000;
    for (int i = 0; i < distinct; i++) {
      assertEquals(i + 1, id(view, URL_TAG, "http://value-" + i), "ids are dense in arrival order");
    }
    for (int i = 0; i < distinct; i++) {
      assertEquals(i + 1, id(view, URL_TAG, "http://value-" + i), "every value keeps its id across every rehash");
    }
    assertEquals(distinct, dictionaries.entryCount(0, 0));
    final byte[][] byId = dictionaries.valuesById(0, 0);
    assertEquals(distinct, byId.length);
    for (int i = 0; i < distinct; i++) {
      assertEquals("http://value-" + i, new String(byId[i], StandardCharsets.UTF_8), "index i holds id i + 1");
    }
    assertEquals(1, id(view, URL_TAG, "http://value-0"), "and the first value is still id 1");
    assertEquals(distinct + 1, id(view, URL_TAG, ""), "the empty string is a value like any other");
  }

  @Test
  @DisplayName("a tag map published mid-load is seen by later encodes; a tag absent when a page encoded is not fatal")
  void tagsMayBePublishedMidLoad() {
    final SegmentScopedDictionaries dictionaries = new SegmentScopedDictionaries(
        new SegmentBoundaries(Long.MAX_VALUE, LEAVES_PER_SEGMENT), SegmentScopedDictionaries.noTags());
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    assertEquals(GlobalStringDictionaries.ID_ABSENT, id(view, URL_TAG, "early"),
        "a path class the load has not resolved yet keeps its bytes");
    dictionaries.publishTags(tags());
    assertEquals(1, id(view, URL_TAG, "late"), "the same view sees the republished map");
    assertEquals(List.of("late"), values(dictionaries, 0, 0));
  }

  @Test
  @DisplayName("a value is probed at its OFFSET: the same bytes at any offset are the same entry")
  void theSliceOffsetIsHonoured() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    final byte[] framed = utf8("XXXXhttp://aYYYY");
    final int id = view.idOf(URL_TAG, framed, 4, 8);
    assertEquals(1, id);
    assertEquals(id, id(view, URL_TAG, "http://a"), "the same bytes at offset 0 are the same entry");
    assertEquals(id, view.idOf(URL_TAG, utf8("zzhttp://a"), 2, 8), "and at a third offset");
    // A hash or a comparison that ignored the offset would fold the frame in and mint a second id.
    assertEquals(1, dictionaries.entryCount(0, 0));
    assertEquals(List.of("http://a"), values(dictionaries, 0, 0), "the stored bytes are the SLICE, not the frame");
    // A prefix and an extension of it are different values, however the slice was cut.
    assertEquals(2, view.idOf(URL_TAG, framed, 4, 7), "http:// is not http://a");
    assertEquals(3, view.idOf(URL_TAG, framed, 4, 9), "http://aY is not either");
    assertEquals(3, dictionaries.entryCount(0, 0));
  }

  @Test
  @DisplayName("a malformed slice is refused before anything is minted")
  void malformedSlicesAreRefused() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    final byte[] value = utf8("http://a");
    assertThrows(NullPointerException.class, () -> view.idOf(URL_TAG, null, 0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> view.idOf(URL_TAG, value, -1, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> view.idOf(URL_TAG, value, 0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> view.idOf(URL_TAG, value, 0, value.length + 1));
    assertThrows(IndexOutOfBoundsException.class, () -> view.idOf(URL_TAG, value, value.length, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> view.idOf(URL_TAG, value, 1, value.length));
    assertEquals(0, dictionaries.entryCount(0, 0), "a refused call mints nothing");
    assertEquals(1, view.idOf(URL_TAG, value, value.length, 0), "an EMPTY slice at the end is a legal value");
  }

  @Test
  @DisplayName("a value longer than the dictionary can persist is left as bytes, not minted")
  void anOverlongValueIsNotMinted() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    final byte[] overlong = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1];
    assertEquals(GlobalStringDictionaries.ID_ABSENT, view.idOf(URL_TAG, overlong, 0, overlong.length),
        "the seal could not write it, so the page must keep its bytes");
    assertEquals(0, dictionaries.entryCount(0, 0), "and nothing was minted for it");
    assertNotEquals(GlobalStringDictionaries.ID_ABSENT,
        view.idOf(URL_TAG, overlong, 0, GlobalValueDictionaryWriter.MAX_VALUE_BYTES),
        "exactly at the limit is still minted");
    assertEquals(1, dictionaries.entryCount(0, 0));
  }

  @Test
  @DisplayName("two values whose FOLDED hashes collide get distinct ids and read back as themselves")
  void aHashCollisionIsResolvedByTheValue() {
    final String[] collision = findFoldedHashCollision();
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    assertEquals(ColumnDictionary.hash(utf8(collision[0]), 0, collision[0].length()),
        ColumnDictionary.hash(utf8(collision[1]), 0, collision[1].length()),
        "the fixture must actually collide, or this test proves nothing");
    assertEquals(collision[0].length(), collision[1].length(),
        "and the two must be the same length, or a length check alone would separate them");
    assertNotEquals(collision[0], collision[1]);
    assertEquals(1, id(view, URL_TAG, collision[0]));
    assertEquals(2, id(view, URL_TAG, collision[1]), "a colliding value takes the NEXT id, not the first one's");
    assertEquals(1, id(view, URL_TAG, collision[0]), "and each keeps its own id on re-probe");
    assertEquals(2, id(view, URL_TAG, collision[1]));
    assertEquals(List.of(collision[0], collision[1]), values(dictionaries, 0, 0));
  }

  @Test
  @DisplayName("release frees the tables, not just the flag: the segment stops holding its values")
  void releaseFreesTheTables() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries page = dictionaries.adopt(0);
    for (int i = 0; i < 4096; i++) {
      id(page, URL_TAG, "http://" + i); // past both initial capacities: the tables have grown
    }
    assertTrue(dictionaries.slotCount(0, 0) > 4096, "the mint table grew with the values");
    assertTrue(dictionaries.retainedValueSlots(0, 0) >= 4096);
    assertEquals(1, dictionaries.adopt(1024).segment());

    dictionaries.release(0);
    assertEquals(1, dictionaries.slotCount(0, 0), "the mint table is gone, not merely marked");
    assertEquals(0, dictionaries.retainedValueSlots(0, 0), "and so are the values it pointed at");
    assertEquals(4096, dictionaries.entryCount(0, 0), "while the sealed count survives");
  }

  @Test
  @DisplayName("a released dictionary's probe misses instead of reading a freed value slot")
  void aReleasedDictionaryMatchesNothing() {
    final ColumnDictionary dictionary = new ColumnDictionary(new LongAdder());
    final byte[] value = utf8("http://a");
    assertEquals(1, dictionary.idOf(value, 0, value.length));
    assertTrue(dictionary.matches(1, value, 0, value.length));

    dictionary.release();
    assertTrue(!dictionary.matches(1, value, 0, value.length),
        "a value slot freed under a racing probe is a MISS: the miss path refuses under the lock");
    assertTrue(!dictionary.matches(7, value, 0, value.length), "and so is an id the array never held");
    assertEquals(1, dictionary.size(), "the sealed count is what the directory records");
    assertThrows(IllegalStateException.class, () -> dictionary.idOf(value, 0, value.length));
  }

  @Test
  @DisplayName("a tag republished to another column mid-run keeps ONE dictionary behind a page's ids")
  void aRunIsPinnedToOneColumn() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    // Column 1 is far ahead: if the run switched columns mid-page, the count read at the end would
    // come from column 1 while the ids came from column 0 — a bound that does not cover them, or a
    // bound that silently covers the WRONG values.
    for (int i = 0; i < 50; i++) {
      id(view, TITLE_TAG, "title-" + i);
    }
    assertTrue(view.hasDictionary(URL_TAG), "the run opens against the map as published now");
    final int first = id(view, URL_TAG, "http://a");
    final long key = view.dictionaryKey(URL_TAG);

    final Int2IntOpenHashMap moved = new Int2IntOpenHashMap();
    moved.put(URL_TAG, 1); // the builder resolves a second field path onto the URL column's tag
    moved.put(TITLE_TAG, 1);
    dictionaries.publishTags(moved);

    final int second = id(view, URL_TAG, "http://b");
    final int count = view.dictionaryEntryCount(URL_TAG);
    assertEquals(1, first);
    assertEquals(2, second, "the run keeps minting into the column it opened against");
    assertEquals(key, view.dictionaryKey(URL_TAG));
    assertTrue(count >= second, "the count read at the end of the run covers every id the run handed out");
    assertEquals(2, count, "and it is column 0's count, not the 50 of column 1");
    assertEquals(2, dictionaries.entryCount(0, 0));
    assertEquals(50, dictionaries.entryCount(0, 1), "the other column was not touched by the run");

    // A NEW run on the same page — the next tag the encoder opens — does see the republished map.
    final GlobalStringDictionaries next = dictionaries.adopt(1);
    assertTrue(next.hasDictionary(URL_TAG));
    assertEquals(51, id(next, URL_TAG, "http://a"), "which is column 1's id space now");
  }

  @Test
  @DisplayName("the entry count a run reports always covers every id it handed out, under concurrent minting")
  void theReportedCountCoversEveryIdUnderConcurrency() throws Exception {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final int threads = 4;
    // The window this witness has to hit is the gap between publishing an id's slot and publishing
    // the count that covers it — a few nanoseconds per mint. Millions of probes are what turn a
    // nanosecond-wide window into a near-certain observation; it still costs well under a second.
    final int perThread = 1_500_000;
    // Adoption is the WRITER's, one page at a time — never a pool thread's. Each worker gets the
    // view of its own page of segment 0, exactly as the flush pool gets one view per page it encodes.
    final GlobalStringDictionaries[] views = new GlobalStringDictionaries[threads];
    for (int t = 0; t < threads; t++) {
      views[t] = dictionaries.adopt(t);
    }
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<?>> futures = new ArrayList<>(threads);
      for (int t = 0; t < threads; t++) {
        final GlobalStringDictionaries view = views[t];
        futures.add(pool.submit(() -> {
          start.await();
          final byte[] scratch = new byte[32];
          for (int i = 0; i < perThread; i++) {
            // Overlapping value spaces, so a thread mostly HITS ids another thread minted: that is
            // the interleaving in which a count published before its id would be observed.
            final int length = writeDecimal(scratch, i % (perThread / 2));
            final int id = view.idOf(URL_TAG, scratch, 0, length);
            final int count = view.dictionaryEntryCount(URL_TAG);
            if (id > count) {
              throw new AssertionError("id " + id + " was handed out with a recorded count of " + count
                  + "; the page would be written with an id its own bound does not cover");
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> future : futures) {
        future.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
    assertEquals(perThread / 2, dictionaries.entryCount(0, 0), "every distinct value was minted exactly once");
  }

  @Test
  @DisplayName("negative segments and columns are refused, and releasing a segment nobody adopted into is loud")
  void contractViolations() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    assertThrows(IllegalArgumentException.class, () -> dictionaries.entryCount(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.entryCount(0, -1));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.mintedBytes(-1));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.valuesById(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.valuesById(0, -1));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.valuesOf(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.release(-1));
    assertThrows(NullPointerException.class, () -> dictionaries.publishTags(null));
    assertThrows(NullPointerException.class, () -> new SegmentScopedDictionaries(null, tags()));
    assertThrows(NullPointerException.class, () -> new SegmentScopedDictionaries(new SegmentBoundaries(), null));
    // A segment that adopted no page has no dictionary to seal, so a release of it is a bookkeeping
    // error: a silent no-op would let a later page open exactly the unsealed dictionary release exists
    // to prevent.
    assertThrows(IllegalStateException.class, () -> dictionaries.release(3));
    dictionaries.adopt(0);
    dictionaries.release(0);
    assertEquals(0, dictionaries.entryCount(9, 0), "a segment nobody adopted into simply has no entries");
    assertEquals(0, dictionaries.mintedBytes(9));
    assertEquals(0, dictionaries.valuesById(9, 0).length);
  }

  /**
   * Two distinct strings of the SAME LENGTH whose folded 32-bit hashes are equal — the collision the
   * probe must resolve. Equal lengths on purpose: with different lengths a comparison that checked
   * only the length would still separate them, and the test would prove nothing about the bytes.
   */
  private static String[] findFoldedHashCollision() {
    final HashMap<Integer, String> seen = new HashMap<>(1 << 18);
    for (int i = 0; i < (1 << 22); i++) {
      final String candidate = String.format("http://collide/%08d", i);
      final byte[] bytes = utf8(candidate);
      final int hash = SegmentScopedDictionaries.ColumnDictionary.hash(bytes, 0, bytes.length);
      final String previous = seen.putIfAbsent(hash, candidate);
      if (previous != null) {
        return new String[] {previous, candidate};
      }
    }
    throw new AssertionError("no folded-hash collision found in 4M candidates; the fixture cannot be built");
  }

  /**
   * {@code value} as ASCII digits in {@code scratch}, returning its length — no allocation per probe.
   */
  private static int writeDecimal(final byte[] scratch, final int value) {
    int remaining = value;
    int length = 0;
    do {
      scratch[length++] = (byte) ('0' + remaining % 10);
      remaining /= 10;
    } while (remaining != 0);
    for (int i = 0, j = length - 1; i < j; i++, j--) {
      final byte swap = scratch[i];
      scratch[i] = scratch[j];
      scratch[j] = swap;
    }
    return length;
  }

  @Test
  @DisplayName("two threads adopting at once are refused: the cursors they would race are updated without atomics")
  void concurrentAdoptionIsRefused() throws Exception {
    final int threads = 8;
    final int perThread = 5_000;
    final int rounds = 20;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    int refusals = 0;
    try {
      // Adoption updates the boundary cursors and grows the state array with plain reads and writes,
      // on the contract that one thread adopts at a time. Hammering it from eight threads collides
      // within a round or two on any real machine; the loop only exists so a scheduler that happens
      // to serialise one round does not fail the build.
      for (int round = 0; round < rounds && refusals == 0; round++) {
        final SegmentScopedDictionaries dictionaries = leafCapped();
        final CountDownLatch ready = new CountDownLatch(threads);
        final AtomicInteger refused = new AtomicInteger();
        final AtomicInteger adopted = new AtomicInteger();
        final List<Future<?>> futures = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
          final int worker = t;
          futures.add(pool.submit(() -> {
            ready.countDown();
            ready.await();
            for (int i = 0; i < perThread; i++) {
              try {
                dictionaries.adopt((long) worker * perThread + i);
                adopted.incrementAndGet();
              } catch (final IllegalStateException refusal) {
                assertTrue(refusal.getMessage().contains("sequential"), refusal.getMessage());
                refused.incrementAndGet();
              }
            }
            return null;
          }));
        }
        for (final Future<?> future : futures) {
          future.get(120, TimeUnit.SECONDS);
        }
        assertEquals(threads * perThread, refused.get() + adopted.get(),
            "every adoption either happened or was refused");
        refusals = refused.get();
      }
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
    assertTrue(refusals > 0,
        "concurrent adoption must be refused rather than silently losing a segment state to a lost update");
  }

  @Test
  @DisplayName("sequential adoption from DIFFERENT threads is fine: the writer hands the load over, never shares it")
  void anOrderedHandOverBetweenThreadsIsAllowed() throws Exception {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    dictionaries.adopt(0);
    // The bulk importer's coordinator adopts built leaves; the transaction's own thread creates
    // fresh pages. Never at the same time, but genuinely different threads.
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      pool.submit(() -> dictionaries.adopt(1)).get(30, TimeUnit.SECONDS);
      pool.submit(() -> dictionaries.adopt(2)).get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
    }
    assertEquals(2L, dictionaries.boundaries().highestAdoptedPageKey());
    assertEquals(1, id(dictionaries.adopt(3), URL_TAG, "back on the first thread"), "and the load goes on");
  }

  @Test
  @DisplayName("the resolver publishes the longest value it can persist, so a caller need not probe to find out")
  void theValueLimitIsPartOfTheContract() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    assertEquals(GlobalValueDictionaryWriter.MAX_VALUE_BYTES, view.maxValueBytes(),
        "what the seal can write is what the encoder must not offer");
    // The encoder leaves the whole tag as bytes when any value exceeds it, WITHOUT probing — which
    // is the point: a probe would mint, and a tag that keeps its bytes references nothing it minted.
    final byte[] overlong = new byte[view.maxValueBytes() + 1];
    assertEquals(GlobalStringDictionaries.ID_ABSENT, view.idOf(URL_TAG, overlong, 0, overlong.length));
    assertEquals(0, dictionaries.entryCount(0, 0));
  }

  @Test
  @DisplayName("a segment reports the dictionaries it minted, so a seal can prove it wrote them all")
  void aSegmentReportsItsDictionaryCount() {
    final SegmentScopedDictionaries dictionaries = leafCapped();
    final GlobalStringDictionaries view = dictionaries.adopt(0);
    assertEquals(0, dictionaries.dictionaryCount(0), "nothing minted yet");
    id(view, URL_TAG, "http://a");
    assertEquals(1, dictionaries.dictionaryCount(0));
    id(view, URL_TAG, "http://b");
    assertEquals(1, dictionaries.dictionaryCount(0), "a second value in the same column is the same dictionary");
    id(view, TITLE_TAG, "title");
    assertEquals(2, dictionaries.dictionaryCount(0), "a second column is a second dictionary");
    assertEquals(0, dictionaries.dictionaryCount(1), "another segment has its own");
    assertEquals(0, dictionaries.dictionaryCount(7), "and a segment nobody adopted into has none");
    assertThrows(IllegalArgumentException.class, () -> dictionaries.dictionaryCount(-1));
    dictionaries.adopt(1024);
    dictionaries.release(0);
    assertEquals(2, dictionaries.dictionaryCount(0), "the count survives the release, like the entry counts");
  }
}
