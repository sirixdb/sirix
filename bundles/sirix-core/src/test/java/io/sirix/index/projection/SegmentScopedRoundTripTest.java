/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The segment-dictionary scheme end to end WITHOUT a pre-pass: values are minted as pages encode,
 * each segment is sealed once its pages are done, and a page's ids resolve back to the exact bytes it
 * wrote — through its own segment, never a neighbour's.
 *
 * <p>
 * The dictionary store is faked (a header key to a value list), so what is under test is the part
 * this design adds: minting per segment, the anchor translation, and the refusals. Turning a value
 * list into committed dictionary pages is {@link PrePassDictionaryBuilder}'s job and is tested where
 * it lives.
 * </p>
 */
final class SegmentScopedRoundTripTest {

  private static final int URL_TAG = 7;

  private static final int TITLE_TAG = 9;

  private static final long LEAVES_PER_SEGMENT = 1024;

  private static Int2IntMap tags() {
    final Int2IntOpenHashMap map = new Int2IntOpenHashMap();
    map.put(URL_TAG, 0);
    map.put(TITLE_TAG, 1);
    return map;
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  /** Stands in for the committed dictionaries: header key to the values it holds, id 1-based. */
  private static final class FakeStore {
    private final Map<Long, List<byte[]>> byHeaderKey = new HashMap<>();

    private long nextHeaderKey = 100;

    /** "Commit" a segment's values, exactly as PrePassDictionaryBuilder would, and return its key. */
    long commit(final Iterator<byte[]> values) {
      final List<byte[]> stored = new ArrayList<>();
      while (values.hasNext()) {
        stored.add(values.next());
      }
      final long headerKey = nextHeaderKey++;
      byHeaderKey.put(headerKey, stored);
      return headerKey;
    }

    byte[] read(final long headerKey, final int id) {
      final List<byte[]> stored = byHeaderKey.get(headerKey);
      return stored == null || id < 1 || id > stored.size()
          ? null
          : stored.get(id - 1);
    }
  }

  /** Encode a page's values, returning the (anchor, entryCount, ids) it would record. */
  private record Encoded(long anchor, int entryCount, int[] ids) {
  }

  private static Encoded encode(final SegmentScopedDictionaries dictionaries, final long recordPageKey, final int tag,
      final String... valuesOnPage) {
    final GlobalStringDictionaries view = dictionaries.viewFor(recordPageKey);
    final int[] ids = new int[valuesOnPage.length];
    for (int i = 0; i < valuesOnPage.length; i++) {
      final byte[] bytes = utf8(valuesOnPage[i]);
      ids[i] = view.idOf(tag, bytes, 0, bytes.length);
    }
    // A page records its anchor and the count it saw AFTER encoding its values, which is what the
    // string region does.
    return new Encoded(view.dictionaryKey(tag), view.dictionaryEntryCount(tag), ids);
  }

  @Test
  @DisplayName("mint, seal, resolve: every page's ids come back as the exact bytes it wrote")
  void roundTripThroughSegments() {
    final SegmentScopedDictionaries writer = new SegmentScopedDictionaries(LEAVES_PER_SEGMENT, tags());
    // Two pages of segment 0, one of segment 1 — the same value in both segments on purpose.
    final Encoded page0 = encode(writer, 0, URL_TAG, "http://a", "http://b");
    final Encoded page1 = encode(writer, 900, URL_TAG, "http://b", "http://c");
    final Encoded page2 = encode(writer, 1024, URL_TAG, "http://a", "http://z");

    assertEquals(1L, page0.anchor(), "segment 0 anchors as 1: zero is the no-dictionary sentinel");
    assertEquals(1L, page1.anchor(), "page 900 is still segment 0");
    assertEquals(2L, page2.anchor());
    assertEquals(2, page0.ids()[1] - page0.ids()[0] + 1, "b follows a in segment 0");
    assertEquals(page0.ids()[1], page1.ids()[0], "the repeat of b on another page of segment 0 reuses its id");
    assertEquals(1, page2.ids()[0], "a in segment 1 is minted afresh — that is the trade");

    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    for (long segment = 0; segment <= 1; segment++) {
      final int count = writer.entryCount(segment, 0);
      anchors.seal(segment, 0, store.commit(writer.valuesOf(segment, 0)), count);
    }
    assertEquals(2, anchors.sealedCount());

    final SegmentScopedReadDictionaries reader =
        new SegmentScopedReadDictionaries(null, tags(), anchors, (key, id, ignored) -> store.read(key, id));

    assertResolves(reader, page0, URL_TAG, "http://a", "http://b");
    assertResolves(reader, page1, URL_TAG, "http://b", "http://c");
    assertResolves(reader, page2, URL_TAG, "http://a", "http://z");
  }

  private static void assertResolves(final SegmentScopedReadDictionaries reader, final Encoded page, final int tag,
      final String... expected) {
    for (int i = 0; i < expected.length; i++) {
      final byte[] resolved = reader.valueOf(tag, page.anchor(), page.entryCount(), page.ids()[i]);
      assertNotNull(resolved, "id " + page.ids()[i] + " of segment " + page.anchor() + " resolves");
      assertArrayEquals(utf8(expected[i]), resolved,
          "segment " + page.anchor() + " id " + page.ids()[i] + " must be " + expected[i]);
    }
  }

  @Test
  @DisplayName("a page never resolves against a NEIGHBOUR's dictionary, even when the id exists there")
  void aPageNeverResolvesAgainstAnotherSegment() {
    final SegmentScopedDictionaries writer = new SegmentScopedDictionaries(LEAVES_PER_SEGMENT, tags());
    final Encoded inZero = encode(writer, 0, URL_TAG, "zero-only");
    final Encoded inOne = encode(writer, 1024, URL_TAG, "one-only");
    assertEquals(inZero.ids()[0], inOne.ids()[0], "both are id 1 in their own segment — the collision that matters");

    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    anchors.seal(1, 0, store.commit(writer.valuesOf(1, 0)), writer.entryCount(1, 0));
    final SegmentScopedReadDictionaries reader =
        new SegmentScopedReadDictionaries(null, tags(), anchors, (key, id, ignored) -> store.read(key, id));

    assertArrayEquals(utf8("zero-only"), reader.valueOf(URL_TAG, inZero.anchor(), inZero.entryCount(), 1));
    assertArrayEquals(utf8("one-only"), reader.valueOf(URL_TAG, inOne.anchor(), inOne.entryCount(), 1),
        "the SAME id in the next segment is a different value, and the anchor is what separates them");
  }

  @Test
  @DisplayName("an UNSEALED segment refuses: its pages are durable, its dictionary is not")
  void anUnsealedSegmentRefuses() {
    final SegmentScopedDictionaries writer = new SegmentScopedDictionaries(LEAVES_PER_SEGMENT, tags());
    final Encoded page = encode(writer, 0, URL_TAG, "http://a");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    final SegmentScopedReadDictionaries reader =
        new SegmentScopedReadDictionaries(null, tags(), anchors, (key, id, ignored) -> store.read(key, id));

    assertTrue(!reader.accepts(URL_TAG, page.anchor(), page.entryCount()));
    assertNull(reader.valueOf(URL_TAG, page.anchor(), page.entryCount(), page.ids()[0]),
        "a crash between writing the pages and sealing the dictionary must refuse, never guess");
    // And once sealed, the same page resolves.
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    assertArrayEquals(utf8("http://a"), reader.valueOf(URL_TAG, page.anchor(), page.entryCount(), page.ids()[0]));
  }

  @Test
  @DisplayName("an id past what the page recorded is refused, and so is a segment holding fewer entries than it saw")
  void idsAndCountsAreBounded() {
    final SegmentScopedDictionaries writer = new SegmentScopedDictionaries(LEAVES_PER_SEGMENT, tags());
    final Encoded page = encode(writer, 0, URL_TAG, "http://a", "http://b");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries reader =
        new SegmentScopedReadDictionaries(null, tags(), anchors, (key, id, ignored) -> store.read(key, id));

    assertNotNull(reader.valueOf(URL_TAG, 1L, page.entryCount(), 2));
    assertNull(reader.valueOf(URL_TAG, 1L, page.entryCount(), 3), "an id above the page's own count");
    assertNull(reader.valueOf(URL_TAG, 1L, page.entryCount(), 0), "id 0 is ID_ABSENT, never a value");
    assertNull(reader.valueOf(URL_TAG, 1L, page.entryCount(), -1));
    assertNull(reader.valueOf(URL_TAG, 0L, page.entryCount(), 1), "anchor 0 is the no-dictionary sentinel");
    // A page that saw MORE than the segment was sealed with cannot be resolved: that is a reused key.
    assertTrue(!reader.accepts(URL_TAG, 1L, page.entryCount() + 1));
    assertNull(reader.valueOf(URL_TAG, 1L, page.entryCount() + 1, 1));
    assertTrue(!reader.accepts(4242, 1L, 1), "an unprojected tag has no dictionary to accept");
  }

  @Test
  @DisplayName("resealing a segment at a different key is refused: pages already carry the first key's ids")
  void resealingIsRefused() {
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(3, 0, 42L, 7);
    anchors.seal(3, 0, 42L, 7); // idempotent
    assertEquals(1, anchors.sealedCount());
    assertThrows(IllegalStateException.class, () -> anchors.seal(3, 0, 43L, 7));
    assertThrows(IllegalStateException.class, () -> anchors.seal(3, 0, 42L, 9));
    assertThrows(IllegalArgumentException.class,
        () -> anchors.seal(3, 1, SegmentDictionaryAnchors.NO_HEADER_KEY, 1));
    assertEquals(42L, anchors.headerKeyOf(3, 0));
    assertEquals(7, anchors.sealedEntryCountOf(3, 0));
    assertEquals(SegmentDictionaryAnchors.NO_HEADER_KEY, anchors.headerKeyOf(3, 1), "another column is separate");
    assertEquals(SegmentDictionaryAnchors.NO_HEADER_KEY, anchors.headerKeyOf(4, 0), "and so is another segment");
  }

  @Test
  @DisplayName("two columns of one segment seal separately and resolve separately")
  void columnsSealSeparately() {
    final SegmentScopedDictionaries writer = new SegmentScopedDictionaries(LEAVES_PER_SEGMENT, tags());
    final Encoded urls = encode(writer, 0, URL_TAG, "u1", "u2");
    final Encoded titles = encode(writer, 0, TITLE_TAG, "t1");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries reader =
        new SegmentScopedReadDictionaries(null, tags(), anchors, (key, id, ignored) -> store.read(key, id));

    assertArrayEquals(utf8("u2"), reader.valueOf(URL_TAG, urls.anchor(), urls.entryCount(), 2));
    assertNull(reader.valueOf(TITLE_TAG, titles.anchor(), titles.entryCount(), 1),
        "the title column of the same segment is not sealed yet, so it refuses");
    anchors.seal(0, 1, store.commit(writer.valuesOf(0, 1)), writer.entryCount(0, 1));
    assertArrayEquals(utf8("t1"), reader.valueOf(TITLE_TAG, titles.anchor(), titles.entryCount(), 1));
  }
}
