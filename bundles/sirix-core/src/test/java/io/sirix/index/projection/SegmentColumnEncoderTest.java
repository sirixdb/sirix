/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The projection build's route into a segment dictionary.
 *
 * <p>
 * The premise the whole projection-side lane rests on is that a document page and a projection leaf
 * interning the same bytes get the SAME id, because they are interning into the same dictionary. If
 * that failed, the projection would need a dictionary of its own — which is exactly the duplication
 * the design exists to remove.
 * </p>
 */
final class SegmentColumnEncoderTest {

  private static final int URL_TAG = 7;

  private static final int TITLE_TAG = 9;

  private static final long LEAVES_PER_SEGMENT = 1024;

  private static SegmentScopedDictionaries dictionaries() {
    final Int2IntOpenHashMap tags = new Int2IntOpenHashMap();
    tags.put(URL_TAG, 0);
    tags.put(TITLE_TAG, 1);
    return new SegmentScopedDictionaries(new SegmentBoundaries(Long.MAX_VALUE, LEAVES_PER_SEGMENT), tags);
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a page and a projection leaf interning the same value get the SAME id: one dictionary, not two")
  void bothSidesShareOneIdSpace() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    final GlobalStringDictionaries page = dictionaries.adopt(0);
    final SegmentColumnEncoder projection = new SegmentColumnEncoder(dictionaries, 0, 0);

    final int fromPage = page.idOf(URL_TAG, utf8("http://a"), 0, 8);
    final int fromProjection = projection.intern("http://a");
    assertEquals(fromPage, fromProjection, "the projection leaf must reuse the id the page minted");
    assertEquals(1, dictionaries.entryCount(0, 0), "and mint nothing new for it");

    // The other direction too: a value the projection sees first is the one the page then reuses.
    final int projectionFirst = projection.intern("http://b");
    assertEquals(2, projectionFirst);
    assertEquals(projectionFirst, page.idOf(URL_TAG, utf8("http://b"), 0, 8));
    assertEquals(2, dictionaries.entryCount(0, 0));
  }

  @Test
  @DisplayName("a column's ids are its own, and a segment's are its own")
  void idsAreScopedToSegmentAndColumn() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    dictionaries.adopt(0);
    dictionaries.adopt(1024);
    final SegmentColumnEncoder zeroUrl = new SegmentColumnEncoder(dictionaries, 0, 0);
    final SegmentColumnEncoder zeroTitle = new SegmentColumnEncoder(dictionaries, 0, 1);
    final SegmentColumnEncoder oneUrl = new SegmentColumnEncoder(dictionaries, 1, 0);

    assertEquals(1, zeroUrl.intern("first"));
    assertEquals(1, zeroTitle.intern("other"), "a second column of the same segment starts at 1 again");
    assertEquals(1, oneUrl.intern("elsewhere"), "and so does the same column of the next segment");
    // The same VALUE in two segments is two entries — the trade a segment dictionary makes.
    assertEquals(2, zeroUrl.intern("shared"));
    assertEquals(2, oneUrl.intern("shared"));
    assertEquals(0, zeroUrl.segment());
    assertEquals(1, oneUrl.segment());
  }

  @Test
  @DisplayName("a value the seal could not persist is refused, not silently written as an absent cell")
  void anOverlongValueIsRefused() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    dictionaries.adopt(0);
    final SegmentColumnEncoder encoder = new SegmentColumnEncoder(dictionaries, 0, 0);
    final byte[] overlong = new byte[GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1];
    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> encoder.intern(overlong, 0, overlong.length));
    assertTrue(failure.getMessage().contains("above what a dictionary entry can hold"), failure.getMessage());
    assertEquals(0, dictionaries.entryCount(0, 0), "and nothing was minted for it");
    assertNotEquals(GlobalStringDictionaries.ID_ABSENT,
        encoder.intern(overlong, 0, GlobalValueDictionaryWriter.MAX_VALUE_BYTES), "exactly at the limit is fine");
  }

  @Test
  @DisplayName("the direct mint route validates its arguments and refuses a segment nothing adopted")
  void contractViolations() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    dictionaries.adopt(0);
    assertThrows(IllegalArgumentException.class, () -> new SegmentColumnEncoder(dictionaries, -1, 0));
    assertThrows(IllegalArgumentException.class, () -> new SegmentColumnEncoder(dictionaries, 0, -1));
    assertThrows(NullPointerException.class, () -> new SegmentColumnEncoder(null, 0, 0));
    final byte[] value = utf8("v");
    assertThrows(IllegalArgumentException.class, () -> dictionaries.idIn(-1, 0, value, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> dictionaries.idIn(0, -1, value, 0, 1));
    assertThrows(NullPointerException.class, () -> dictionaries.idIn(0, 0, null, 0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> dictionaries.idIn(0, 0, value, 0, 2));
    // A segment nothing was adopted into has no dictionary to mint into, and creating one here would
    // start a dictionary the seal never sees.
    assertThrows(IllegalStateException.class, () -> dictionaries.idIn(7, 0, value, 0, 1));
  }

  @Test
  @DisplayName("a sealed and released segment refuses a late projection mint, like a late page mint")
  void aReleasedSegmentRefusesTheProjectionToo() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    dictionaries.adopt(0);
    final SegmentColumnEncoder encoder = new SegmentColumnEncoder(dictionaries, 0, 0);
    assertEquals(1, encoder.intern("before the seal"));
    dictionaries.adopt(1024);
    dictionaries.release(0);
    assertThrows(IllegalStateException.class, () -> encoder.intern("after the seal"));
  }

  @Test
  @DisplayName("the tag map is not consulted: the projection knows its own column")
  void theProjectionRouteNeedsNoTag() {
    final SegmentScopedDictionaries dictionaries = dictionaries();
    dictionaries.adopt(0);
    // Column 4 is beyond anything the tag map mentions. The page route would refuse it (no tag maps
    // there); the projection route mints, because a projection column IS the column number.
    final SegmentColumnEncoder beyondTheTags = new SegmentColumnEncoder(dictionaries, 0, 4);
    assertEquals(1, beyondTheTags.intern("a column no tag names"));
    assertEquals(1, dictionaries.entryCount(0, 4));
    final Int2IntMap tags = dictionaries.tags();
    assertTrue(!tags.containsValue(4), "the fixture's tag map really does not mention column 4");
  }
}
