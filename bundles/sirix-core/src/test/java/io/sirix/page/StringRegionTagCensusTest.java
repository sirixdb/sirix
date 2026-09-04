/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.page.pax.StringRegion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witnesses the per-tag string-region census — the instrument that says which COLUMN owns which of
 * the trie's string-region bytes.
 *
 * <p>
 * The load-bearing assertion is the census IDENTITY: framing plus every tag's attributed bytes must
 * equal the region's own encoded length, exactly, on both encoder lanes. A share that merely looks
 * plausible is worthless for pricing a lever; a residual of zero is the only evidence that no byte
 * is missed and none is counted twice, and it is what fails the day the encoder's layout moves
 * without this instrument following it.
 * </p>
 *
 * <p>
 * The counters are process-wide and cumulative, so every assertion here is on a DELTA, and each
 * test uses tag values of its own so one test cannot read another's bytes.
 * </p>
 */
final class StringRegionTagCensusTest {

  private static final byte PATH_TAG = StringRegion.TAG_KIND_PATH_NODE;

  @BeforeEach
  void theInstrumentMustActuallyBeOn() {
    // The gates are static final reads, so this suite cannot switch them on for itself — it asserts
    // they are on and lets the build supply them. With them off every counter reads zero, and a zero
    // from a disabled instrument is indistinguishable from a zero from a healthy one, which is
    // exactly the hollow witness this campaign has caught five times.
    assertTrue(PageSectionDiag.ENABLED,
        "run with -Dsirix.pageSectionDiag=true -Dsirix.pageSectionDiag.stringTags=true");
    assertTrue(PageSectionDiag.STRING_TAG_DIAG,
        "run with -Dsirix.pageSectionDiag=true -Dsirix.pageSectionDiag.stringTags=true");
  }

  @AfterEach
  void restoreTheLane() {
    StringRegion.clearPlainLaneOverride();
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  /** Encode a fixed shape and return its length, so a test can compare it against the census. */
  private static int encodeTwoTagShape(final int tagA, final int tagB) {
    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    // Tag A repeats a value, so it needs a dictionary AND an id lane; tag B is all-distinct.
    encoder.addValue(tagA, utf8("https://example.com/alpha"));
    encoder.addValue(tagB, utf8("first-title"));
    encoder.addValue(tagA, utf8("https://example.com/alpha"));
    encoder.addValue(tagB, utf8("second-title"));
    encoder.addValue(tagA, utf8("https://example.com/beta"));
    encoder.addValue(tagB, utf8("third-title"));
    return encoder.finish(PATH_TAG, false).length;
  }

  @Test
  void theCensusAttributesEveryByteOnTheBitPackedLane() {
    StringRegion.setPlainLaneEnabled(false);
    assertCensusIsExact(1101, 1102);
  }

  @Test
  void theCensusAttributesEveryByteOnTheVarintFramedLane() {
    StringRegion.setPlainLaneEnabled(true);
    assertCensusIsExact(1201, 1202);
  }

  private static void assertCensusIsExact(final int tagA, final int tagB) {
    final long censusBefore = PageSectionDiag.stringRegionCensusBytes();
    final long framingBefore = PageSectionDiag.stringRegionFramingBytes();
    final long dictBefore = PageSectionDiag.stringTagDictBytesTotal();
    final long laneBefore = PageSectionDiag.stringRegionLaneBytes();

    final int encodedLength = encodeTwoTagShape(tagA, tagB);
    assertTrue(encodedLength > 0, "the fixture must produce a region");

    final long census = PageSectionDiag.stringRegionCensusBytes() - censusBefore;
    final long framing = PageSectionDiag.stringRegionFramingBytes() - framingBefore;
    // Dictionaries per tag, plus the lane as the ENCODER rounded it for this region. Summing the
    // per-tag lane BITS instead loses up to seven bits per region — the first two versions of this
    // instrument got that wrong in both directions, and this identity is what caught each of them.
    final long tagged = (PageSectionDiag.stringTagDictBytesTotal() - dictBefore)
        + (PageSectionDiag.stringRegionLaneBytes() - laneBefore);

    assertEquals(encodedLength, census, "the census must see the region's real encoded length");
    // THE assertion. A residual means the instrument does not follow the layout it is measuring.
    assertEquals(encodedLength, framing + tagged,
        "framing + per-tag bytes must reconstruct the region exactly, with no residual");
    assertTrue(framing > 0, "the tag directory is not free and must be attributed to framing");
    assertTrue(tagged > 0, "the tags must own bytes");
  }

  @Test
  void bytesLandOnTheTagThatOwnsThemAndNotOnItsNeighbour() {
    StringRegion.setPlainLaneEnabled(false);
    final int fat = 1301;
    final int thin = 1302;
    final String longValue = "https://example.com/" + "q".repeat(400);

    final long fatBefore = PageSectionDiag.stringTagDictValueBytes(PATH_TAG, fat);
    final long thinBefore = PageSectionDiag.stringTagDictValueBytes(PATH_TAG, thin);

    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    encoder.addValue(fat, utf8(longValue));
    encoder.addValue(thin, utf8("x"));
    encoder.finish(PATH_TAG, false);

    final long fatDelta = PageSectionDiag.stringTagDictValueBytes(PATH_TAG, fat) - fatBefore;
    final long thinDelta = PageSectionDiag.stringTagDictValueBytes(PATH_TAG, thin) - thinBefore;

    // Attribution to the right column is the entire purpose: a census that summed correctly but
    // charged the bytes to the wrong tag would price the wrong lever and still reconcile.
    assertEquals(longValue.length(), fatDelta, "the long value must be charged to its own tag");
    assertEquals(1L, thinDelta, "the neighbour must be charged only its own byte");
  }

  @Test
  void anAllDistinctPlainTagContributesNoIdLaneAtAll() {
    StringRegion.setPlainLaneEnabled(true);
    final int plain = 1401;
    final int repeating = 1402;

    final long plainBefore = PageSectionDiag.stringTagIdLaneBits(PATH_TAG, plain);
    final long repeatingBefore = PageSectionDiag.stringTagIdLaneBits(PATH_TAG, repeating);

    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    encoder.addValue(plain, utf8("a"));
    encoder.addValue(plain, utf8("b"));
    encoder.addValue(plain, utf8("c"));
    encoder.addValue(repeating, utf8("r"));
    encoder.addValue(repeating, utf8("r"));
    encoder.addValue(repeating, utf8("s"));
    encoder.finish(PATH_TAG, false);

    // On the plain lane an all-distinct tag stores no ids — its values ARE its dictionary. Counting
    // a lane there would invent bytes the file does not contain and overstate what a FOR-packed lane
    // could save.
    assertEquals(0L, PageSectionDiag.stringTagIdLaneBits(PATH_TAG, plain) - plainBefore,
        "an all-distinct plain tag has no id lane");
    assertTrue(PageSectionDiag.stringTagIdLaneBits(PATH_TAG, repeating) - repeatingBefore > 0L,
        "a tag with repeats does have one");
  }

  @Test
  void theForWidthIsNarrowerThanThePageWideWidthWhenTagsDifferInCardinality() {
    StringRegion.setPlainLaneEnabled(false);
    final int wide = 1501;
    final int narrow = 1502;

    final long narrowLaneBefore = PageSectionDiag.stringTagIdLaneBits(PATH_TAG, narrow);
    final long narrowForBefore = PageSectionDiag.stringTagForIdLaneBits(PATH_TAG, narrow);

    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    // 300 distinct values under `wide` force a 9-bit page-wide width...
    for (int i = 0; i < 300; i++) {
      encoder.addValue(wide, utf8("w" + i));
    }
    // ...which `narrow`, holding two distinct values, is then forced to pay for every id.
    for (int i = 0; i < 64; i++) {
      encoder.addValue(narrow, utf8(i % 2 == 0
          ? "yes"
          : "no"));
    }
    encoder.finish(PATH_TAG, false);

    final long narrowLane = PageSectionDiag.stringTagIdLaneBits(PATH_TAG, narrow) - narrowLaneBefore;
    final long narrowFor = PageSectionDiag.stringTagForIdLaneBits(PATH_TAG, narrow) - narrowForBefore;

    // This gap IS the lever the table exists to price. If the two were equal the report's "saving"
    // column would be structurally zero and could never say anything.
    assertEquals(64L * 9L, narrowLane, "the narrow tag pays the page-wide width today");
    assertEquals(64L * 1L, narrowFor, "at its own cardinality one bit per id is enough");
    assertTrue(narrowFor < narrowLane, "the FOR lane must be the narrower of the two");
  }
}
