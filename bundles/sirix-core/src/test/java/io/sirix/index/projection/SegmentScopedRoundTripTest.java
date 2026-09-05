/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.SegmentDictionaryDirectoryNode;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;

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
 * each segment is sealed once its pages are done, and a page's ids resolve back to the exact bytes
 * it wrote — through its own segment, never a neighbour's.
 *
 * <p>
 * The dictionary store is faked (a header key to a value list), so what is under test is the part
 * this design adds: minting per segment, the anchor translation, and the refusals. Turning a value
 * list into committed dictionary pages is {@link PrePassDictionaryBuilder}'s job and is tested
 * where it lives.
 * </p>
 */
final class SegmentScopedRoundTripTest {

  private static final int URL_TAG = 7;

  private static final int TITLE_TAG = 9;

  /** A segment per 1024 adopted keys and no byte budget, so a page's segment is arithmetic here. */
  private static final long LEAVES_PER_SEGMENT = 1024;

  private static SegmentScopedDictionaries writer() {
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
    final GlobalStringDictionaries view = dictionaries.adopt(recordPageKey);
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
    final SegmentScopedDictionaries writer = writer();
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
    for (int segment = 0; segment <= 1; segment++) {
      final int count = writer.entryCount(segment, 0);
      anchors.seal(segment, 0, store.commit(writer.valuesOf(segment, 0)), count);
    }
    assertEquals(2, anchors.sealedCount());

    final SegmentScopedReadDictionaries reader = readerOf(anchors, 2, store);

    assertResolves(reader, page0, URL_TAG, "http://a", "http://b");
    assertResolves(reader, page1, URL_TAG, "http://b", "http://c");
    assertResolves(reader, page2, URL_TAG, "http://a", "http://z");
  }

  /**
   * The directory a seal would have written: one slot per column that sealed a dictionary, carrying
   * the tags that resolve to it. Built from the same anchors the lane records, so the test drives the
   * reader through the record the reader really reads.
   */
  private static SegmentDictionaryDirectoryNode directoryOf(final SegmentDictionaryAnchors anchors,
      final int segments) {
    final long[] starts = new long[segments];
    for (int segment = 0; segment < segments; segment++) {
      starts[segment] = (long) segment * LEAVES_PER_SEGMENT;
    }
    final SegmentDictionaryDirectoryNode.SlotTable[] tables = new SegmentDictionaryDirectoryNode.SlotTable[segments];
    for (int segment = 0; segment < segments; segment++) {
      final IntArrayList columns = new IntArrayList();
      for (int column = 0; column <= 1; column++) {
        if (anchors.headerKeyOf(segment, column) != SegmentDictionaryAnchors.NO_HEADER_KEY) {
          columns.add(column);
        }
      }
      if (columns.isEmpty()) {
        tables[segment] = SegmentDictionaryDirectoryNode.SlotTable.EMPTY;
        continue;
      }
      final int[][] tagsBySlot = new int[columns.size()][];
      final long[] headerKeys = new long[columns.size()];
      final int[] entryCounts = new int[columns.size()];
      for (int slot = 0; slot < columns.size(); slot++) {
        final int column = columns.getInt(slot);
        tagsBySlot[slot] = column == 0
            ? new int[] {URL_TAG}
            : new int[] {TITLE_TAG};
        headerKeys[slot] = anchors.headerKeyOf(segment, column);
        entryCounts[slot] = anchors.sealedEntryCountOf(segment, column);
      }
      tables[segment] = SegmentDictionaryDirectoryNode.SlotTable.takeOwnership(tagsBySlot, headerKeys, entryCounts);
    }
    return SegmentDictionaryDirectoryNode.takeOwnership(SegmentDictionaryDirectoryNode.DIRECTORY_KEY, starts, tables);
  }

  private static SegmentScopedReadDictionaries readerOf(final SegmentDictionaryAnchors anchors, final int segments,
      final FakeStore store) {
    return new SegmentScopedReadDictionaries(null, directoryOf(anchors, segments),
        (key, id, ignored) -> store.read(key, id));
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
    final SegmentScopedDictionaries writer = writer();
    final Encoded inZero = encode(writer, 0, URL_TAG, "zero-only");
    final Encoded inOne = encode(writer, 1024, URL_TAG, "one-only");
    assertEquals(inZero.ids()[0], inOne.ids()[0], "both are id 1 in their own segment — the collision that matters");

    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    anchors.seal(1, 0, store.commit(writer.valuesOf(1, 0)), writer.entryCount(1, 0));
    final SegmentScopedReadDictionaries reader = readerOf(anchors, 2, store);

    assertArrayEquals(utf8("zero-only"), reader.valueOf(URL_TAG, inZero.anchor(), inZero.entryCount(), 1));
    assertArrayEquals(utf8("one-only"), reader.valueOf(URL_TAG, inOne.anchor(), inOne.entryCount(), 1),
        "the SAME id in the next segment is a different value, and the anchor is what separates them");
  }

  @Test
  @DisplayName("a tag's SLOT is looked up in the page's own segment, which is not the same slot everywhere")
  void aTagsSlotIsPerSegment() {
    final SegmentScopedDictionaries writer = writer();
    // Segment 0 has a title and no url; segment 1 has both. So the title tag sits at slot 0 in one
    // segment and slot 1 in the other, and a resolver that looked its slot up in the WRONG segment's
    // table would hand back the url dictionary's key for a title id — plausible bytes, wrong column.
    final Encoded zeroTitle = encode(writer, 0, TITLE_TAG, "t-zero");
    final Encoded oneUrl = encode(writer, 1024, URL_TAG, "u-one");
    final Encoded oneTitle = encode(writer, 1024, TITLE_TAG, "t-one");

    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 1, store.commit(writer.valuesOf(0, 1)), writer.entryCount(0, 1));
    anchors.seal(1, 0, store.commit(writer.valuesOf(1, 0)), writer.entryCount(1, 0));
    anchors.seal(1, 1, store.commit(writer.valuesOf(1, 1)), writer.entryCount(1, 1));
    final SegmentScopedReadDictionaries reader = readerOf(anchors, 2, store);

    assertEquals(1, reader.slotCountOf(0), "segment 0 sealed one column");
    assertEquals(2, reader.slotCountOf(1), "segment 1 sealed two");
    assertArrayEquals(utf8("t-zero"), reader.valueOf(TITLE_TAG, zeroTitle.anchor(), zeroTitle.entryCount(), 1));
    assertArrayEquals(utf8("u-one"), reader.valueOf(URL_TAG, oneUrl.anchor(), oneUrl.entryCount(), 1));
    assertArrayEquals(utf8("t-one"), reader.valueOf(TITLE_TAG, oneTitle.anchor(), oneTitle.entryCount(), 1),
        "the title tag resolves through segment 1's OWN slot, not segment 0's");
    assertNull(reader.valueOf(URL_TAG, zeroTitle.anchor(), zeroTitle.entryCount(), 1),
        "and segment 0 covers no url tag at all");
  }

  @Test
  @DisplayName("an id above what the PAGE recorded is refused even when the dictionary holds it")
  void anIdAboveThePagesOwnCountIsRefused() {
    final SegmentScopedDictionaries writer = writer();
    final Encoded page = encode(writer, 0, URL_TAG, "u1", "u2");
    // The segment goes on to mint a third value AFTER this page was encoded — the ordinary shape,
    // since other pages of the segment keep minting. The page's own recorded count is 2.
    final GlobalStringDictionaries later = writer.adopt(1);
    final byte[] third = utf8("u3");
    assertEquals(3, later.idOf(URL_TAG, third, 0, third.length));

    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries reader = readerOf(anchors, 2, store);

    assertEquals(2, page.entryCount(), "the page recorded the count it saw");
    assertArrayEquals(utf8("u2"), reader.valueOf(URL_TAG, page.anchor(), page.entryCount(), 2));
    // id 3 IS in the sealed dictionary. It is still refused: the page cannot have written it, so an
    // id above its own bound means the page and the dictionary disagree about which one this is.
    assertNotNull(store.read(anchors.headerKeyOf(0, 0), 3), "the fixture must really hold id 3");
    assertNull(reader.valueOf(URL_TAG, page.anchor(), page.entryCount(), 3),
        "an id above the page's own recorded count is refused, dictionary or no dictionary");
  }

  @Test
  @DisplayName("an UNSEALED segment refuses: its pages are durable, its dictionary is not")
  void anUnsealedSegmentRefuses() {
    final SegmentScopedDictionaries writer = writer();
    final Encoded page = encode(writer, 0, URL_TAG, "http://a");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    final SegmentScopedReadDictionaries unsealed = readerOf(anchors, 2, store);

    assertTrue(!unsealed.accepts(URL_TAG, page.anchor(), page.entryCount()));
    assertNull(unsealed.valueOf(URL_TAG, page.anchor(), page.entryCount(), page.ids()[0]),
        "a crash between writing the pages and sealing the dictionary must refuse, never guess");
    // And once sealed, the same page resolves — through the directory of the revision that HAS the
    // seal. A reader holds the directory record of its own revision, so a later seal is a later
    // revision's directory, never a mutation under a reader that has already answered.
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries sealed = readerOf(anchors, 2, store);
    assertArrayEquals(utf8("http://a"), sealed.valueOf(URL_TAG, page.anchor(), page.entryCount(), page.ids()[0]));
  }

  @Test
  @DisplayName("an id past what the page recorded is refused, and so is a segment holding fewer entries than it saw")
  void idsAndCountsAreBounded() {
    final SegmentScopedDictionaries writer = writer();
    final Encoded page = encode(writer, 0, URL_TAG, "http://a", "http://b");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries reader = readerOf(anchors, 2, store);

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
    assertThrows(IllegalArgumentException.class, () -> anchors.seal(3, 1, SegmentDictionaryAnchors.NO_HEADER_KEY, 1));
    assertEquals(42L, anchors.headerKeyOf(3, 0));
    assertEquals(7, anchors.sealedEntryCountOf(3, 0));
    assertEquals(SegmentDictionaryAnchors.NO_HEADER_KEY, anchors.headerKeyOf(3, 1), "another column is separate");
    assertEquals(SegmentDictionaryAnchors.NO_HEADER_KEY, anchors.headerKeyOf(4, 0), "and so is another segment");
  }

  @Test
  @DisplayName("two columns of one segment seal separately and resolve separately")
  void columnsSealSeparately() {
    final SegmentScopedDictionaries writer = writer();
    final Encoded urls = encode(writer, 0, URL_TAG, "u1", "u2");
    final Encoded titles = encode(writer, 0, TITLE_TAG, "t1");
    final FakeStore store = new FakeStore();
    final SegmentDictionaryAnchors anchors = new SegmentDictionaryAnchors();
    anchors.seal(0, 0, store.commit(writer.valuesOf(0, 0)), writer.entryCount(0, 0));
    final SegmentScopedReadDictionaries urlsOnly = readerOf(anchors, 2, store);

    assertArrayEquals(utf8("u2"), urlsOnly.valueOf(URL_TAG, urls.anchor(), urls.entryCount(), 2));
    assertNull(urlsOnly.valueOf(TITLE_TAG, titles.anchor(), titles.entryCount(), 1),
        "the title column of the same segment is not sealed yet, so it refuses");
    assertTrue(!urlsOnly.hasDictionary(TITLE_TAG), "and the directory does not claim to cover its tag");

    anchors.seal(0, 1, store.commit(writer.valuesOf(0, 1)), writer.entryCount(0, 1));
    final SegmentScopedReadDictionaries both = readerOf(anchors, 2, store);
    assertArrayEquals(utf8("t1"), both.valueOf(TITLE_TAG, titles.anchor(), titles.entryCount(), 1));
    assertArrayEquals(utf8("u2"), both.valueOf(URL_TAG, urls.anchor(), urls.entryCount(), 2),
        "and the column that was already sealed still resolves from its own slot");
  }
}
