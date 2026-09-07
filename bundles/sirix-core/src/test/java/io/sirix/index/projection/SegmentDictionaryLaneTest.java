/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.projection.ProjectionIndexMetadata.SegmentAnchor;
import io.sirix.node.SegmentDictionaryDirectoryNode;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.pax.GlobalStringDictionaries;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * The lane's own bookkeeping: which segment a page's two notifications land in, and what happens to
 * a segment once it is sealed.
 *
 * <p>
 * Writing a sealed segment's values needs a real storage engine writer and is covered end to end;
 * what is under test here is the part that decides WHEN that write may happen and that a page which
 * arrives after it cannot mint silently.
 * </p>
 */
final class SegmentDictionaryLaneTest {

  /** A segment per 1024 adopted keys and no byte budget, so a page's segment is arithmetic here. */
  private static final long LEAVES_PER_SEGMENT = 1024;

  private @Nullable String savedEnabled;

  private boolean savedChunked;

  @BeforeEach
  void armTheLane() {
    savedEnabled = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    // The lane refuses to arm without chunk-framed bodies, because a converted page is readable only
    // on the lazy route; see theLaneRefusesAMonolithicBody for that guard's own witness.
    savedChunked = ChunkedBodyConfig.setEnabledForTesting(true);
  }

  @AfterEach
  void restoreTheProperty() {
    ChunkedBodyConfig.setEnabledForTesting(savedChunked);
    if (savedEnabled == null) {
      System.clearProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    } else {
      System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, savedEnabled);
    }
  }

  @Test
  @DisplayName("the lane refuses to arm over monolithic bodies: it would write pages nothing can read")
  void theLaneRefusesAMonolithicBody() {
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    ChunkedBodyConfig.setEnabledForTesting(false);
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> SegmentDictionaryLane.bind(writer, 1));
    assertTrue(failure.getMessage().contains("chunk-framed"), failure.getMessage());
    // Refused before anything was written or installed: the load can still run, converting nothing.
    verifyNoMoreInteractions(writer);
  }

  private static SegmentDictionaryLane lane(final int columns) {
    return new SegmentDictionaryLane(columns, new SegmentBoundaries(Long.MAX_VALUE, LEAVES_PER_SEGMENT));
  }

  @Test
  @DisplayName("the lane is off unless the property arms it, and installs nothing while it is off")
  void theKillSwitchGatesTheWiring() {
    System.clearProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    assertFalse(SegmentDictionaryLane.enabled());
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    assertNull(SegmentDictionaryLane.bind(writer, 2), "a load that did not ask for the lane gets none");
    // Not one call: an unarmed lane neither reserves the directory's keys nor installs a seam, so a
    // load that did not ask for it cannot tell the lane exists.
    verifyNoMoreInteractions(writer);

    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    assertTrue(SegmentDictionaryLane.enabled());
  }

  @Test
  @DisplayName("bind installs the resolver factory and the encode listener; release takes both away")
  void bindInstallsBothSeamsAndReleaseRemovesThem() {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final SegmentDictionaryLane lane = SegmentDictionaryLane.install(writer, 2, new SegmentBoundaries());
    assertNotNull(lane);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<LongFunction<GlobalStringDictionaries>> factory = ArgumentCaptor.forClass(LongFunction.class);
    verify(writer).installDocumentStringDictionaryFactory(factory.capture());
    final ArgumentCaptor<LongConsumer> listener = ArgumentCaptor.forClass(LongConsumer.class);
    verify(writer).installDocumentPageEncodedListener(listener.capture());
    assertNotNull(factory.getValue());
    assertNotNull(listener.getValue());

    // The installed factory is the lane's own adoption: a page it serves belongs to the lane's
    // dictionaries, and the lane knows the page is outstanding.
    final GlobalStringDictionaries view = factory.getValue().apply(7L);
    assertNotNull(view);
    assertEquals(1L, view.dictionaryKey(publishedTag(lane)), "segment 0, encoded as 1");
    assertThrows(IllegalStateException.class, () -> lane.sealAll(writer), "page 7 has not been encoded");
    listener.getValue().accept(7L);
    assertEquals(0, lane.sealAll(writer).length, "and now the segment seals, with nothing to write");

    lane.release(writer);
    verify(writer).installDocumentStringDictionaryFactory(null);
    verify(writer).installDocumentPageEncodedListener(null);
    lane.release(null); // a load that never bound must be able to release
  }

  @Test
  @DisplayName("a page's encode retires it in the segment it was ADOPTED into, not the one being filled")
  void anEncodeRetiresTheAdoptingSegment() {
    final SegmentDictionaryLane lane = lane(1);
    lane.adoptPage(0L);
    lane.adoptPage(5L); // segment 0, and it will sit in the flush queue
    lane.adoptPage(1024L); // the writer has moved on: segment 1 is now the open one
    lane.adoptPage(1025L);

    lane.pageEncoded(1024L);
    lane.pageEncoded(1025L);
    lane.pageEncoded(0L);
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    // Page 5 is still outstanding IN SEGMENT 0. A listener that retired the page in "the segment
    // being filled" would have taken this one off segment 1 and sealed segment 0 with a page of it
    // still to be encoded.
    final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> lane.sealAll(writer));
    assertTrue(failure.getMessage().contains("segment 0"), failure.getMessage());

    lane.pageEncoded(5L);
    assertEquals(0, lane.sealAll(writer).length);
    verifyNoMoreInteractions(writer);
  }

  @Test
  @DisplayName("a page adopted twice — a copy-on-write copy — is one outstanding page, not two")
  void aReAdoptedPageDoesNotDeadlockTheSeal() {
    final SegmentDictionaryLane lane = lane(1);
    lane.adoptPage(0L);
    lane.adoptPage(0L);
    lane.adoptPage(0L);
    lane.pageEncoded(0L);
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    // A counter would sit at two here and refuse forever; the seal must go through.
    assertEquals(0, lane.sealAll(writer).length);
  }

  @Test
  @DisplayName("a sealed segment is RELEASED at once: a page that arrives afterwards cannot mint into it")
  void sealingReleasesTheSegment() {
    final SegmentDictionaryLane lane = lane(1);
    final SegmentScopedDictionaries.SegmentView view = lane.adoptPage(0L);
    lane.pageEncoded(0L);
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final SegmentAnchor[] anchors = lane.sealAll(writer);
    assertEquals(0, anchors.length, "the page minted nothing, so there is no dictionary to anchor");

    // The mint maps are gone with the seal. A late page — one encoded after the drain claimed the
    // pool was fenced — must be refused rather than starting a dictionary nobody will persist.
    final byte[] value = "http://late".getBytes(StandardCharsets.UTF_8);
    assertThrows(IllegalStateException.class, () -> view.idOf(publishedTag(lane), value, 0, value.length));
    assertThrows(IllegalStateException.class, () -> lane.adoptPage(1L), "and so is a late adoption");
    assertEquals(0, lane.sealedCount(), "nothing was written, so nothing is anchored");
  }

  @Test
  @DisplayName("a dictionary minted at a column the seal does not iterate is refused, not released unwritten")
  void aDictionaryOutsideTheSealsColumnsIsRefused() {
    final SegmentDictionaryLane lane = lane(1); // the index declares ONE column
    final SegmentScopedDictionaries.SegmentView view = lane.adoptPage(0L);
    // ... but a tag resolves to column 1. However that happens — an extractor whose field list is
    // not the index def's — the pages of this segment stamp ids against a dictionary sealAll's loop
    // never reaches. Releasing it silently would leave every one of those pages unreadable.
    final TagColumnMap claims = new TagColumnMap(1);
    claims.claim(9L, 1);
    lane.dictionaries().publishTags(claims.build());
    final byte[] value = "title".getBytes(StandardCharsets.UTF_8);
    assertEquals(1, view.idOf(9, value, 0, value.length));
    lane.pageEncoded(0L);

    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> lane.sealAll(writer));
    assertTrue(failure.getMessage().contains("minted 1 dictionaries"), failure.getMessage());
    assertTrue(failure.getMessage().contains("wrote 0"), failure.getMessage());
    verifyNoMoreInteractions(writer);
  }

  @Test
  @DisplayName("the lane refuses a negative column count")
  void contractViolations() {
    assertThrows(IllegalArgumentException.class, () -> new SegmentDictionaryLane(-1, new SegmentBoundaries()));
    assertThrows(NullPointerException.class, () -> lane(1).sealAll(null));
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    assertThrows(NullPointerException.class, () -> SegmentDictionaryLane.bind(null, 1));
  }

  /** A tag the lane's dictionaries resolve to column 0, published the way the builder publishes. */
  private static int publishedTag(final SegmentDictionaryLane lane) {
    final TagColumnMap claims = new TagColumnMap(1);
    claims.claim(7L, 0);
    lane.dictionaries().publishTags(claims.build());
    return 7;
  }

  @Test
  @DisplayName("a column fed by several path classes files ALL of them, ascending, whatever order the map yields")
  void aColumnsTagsAreFiledAscending() {
    final SegmentDictionaryLane lane = lane(1);
    lane.adoptPage(0L);
    // Two field paths resolving onto one column — a descendant step, a repeated field — is exactly
    // the shape TagColumnMap keeps (one column, so no contest), and BOTH tags must reach the
    // directory: a tag it does not list is a tag whose pages cannot be resolved. The pair is chosen
    // so the map yields them DESCENDING, because a lane that files them in map order would then hand
    // the directory a slot it refuses, and any other pair would pass by luck.
    final Int2IntMap tags = tagPairYieldingDescendingOrder();
    final IntIterator keys = tags.keySet().iterator();
    final int firstYielded = keys.nextInt();
    final int secondYielded = keys.nextInt();
    assertTrue(firstYielded > secondYielded, "the fixture must yield the tags descending: " + tags.keySet());
    lane.dictionaries().publishTags(tags);
    lane.anchors().seal(0, 0, 4242L, 7);

    final SegmentDictionaryDirectoryNode.SlotTable table = lane.slotTableFor(0, tags);
    assertEquals(1, table.slotCount(), "one column with a sealed dictionary is one slot");
    assertArrayEquals(new int[] {secondYielded, firstYielded}, table.tags(0), "the slot's tags are ascending");
    assertEquals(4242L, table.headerKey(0));
    assertEquals(7, table.entryCount(0));
  }

  @Test
  @DisplayName("a segment with nothing sealed files no slot at all")
  void anUnsealedSegmentFilesNoSlot() {
    final SegmentDictionaryLane lane = lane(1);
    lane.adoptPage(0L);
    final TagColumnMap claims = new TagColumnMap(1);
    claims.claim(7L, 0);
    assertEquals(0, lane.slotTableFor(0, claims.build()).slotCount());
  }

  /** A one-column tag map whose iteration yields its two tags in DESCENDING order. */
  private static Int2IntMap tagPairYieldingDescendingOrder() {
    for (int low = 1; low < 512; low++) {
      for (int high = low + 1; high < 512; high++) {
        final TagColumnMap claims = new TagColumnMap(2);
        claims.claim(low, 0);
        claims.claim(high, 0);
        final Int2IntMap candidate = claims.build();
        final IntIterator keys = candidate.keySet().iterator();
        if (keys.nextInt() == high && keys.nextInt() == low) {
          return candidate;
        }
      }
    }
    throw new AssertionError("no tag pair iterates descending; the fixture cannot be built");
  }
}
