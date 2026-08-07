package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A tag the encoder cannot carry exactly must cost ONE FIELD its column, not the whole page.
 *
 * <h2>What makes a tag unsound</h2>
 * Every encoding except {@code ENC_DEC} stores a value's DOUBLE IMAGE. That is the truth for a
 * genuinely double-typed value, and for a decimal only when the image is the decimal — two decimals
 * can share one ({@code 1000.25} and {@code 1000.25000000000001} do), so storing an image for an
 * inexact decimal is the miscount {@code DecimalDoubleCollisionTest} exists to prevent. When such a
 * tag also falls out of the exact-decimal domain there is nowhere left to put it, and it must be
 * left out of the column entirely rather than encoded as PLAIN or ALP.
 *
 * <h2>The trigger is reachable without mixed typing</h2>
 * An ALL-decimal tag is exact-encoded at the maximum scale it holds, so one scale-14 value forces a
 * {@code 10^12} lift on its scale-2 neighbours; any unscaled value past {@code Long.MAX_VALUE /
 * 10^12} (about {@code 9.2e6}, i.e. a scale-2 price over {@code 92233.72}) overflows the lift and
 * drops the tag out of the exact domain. It then fails the image check and cannot be encoded at all.
 *
 * <h2>The scope</h2>
 * Omitting the tag is unambiguous in the wire format because it disappears WHOLE — dict, per-tag
 * counts and total are rebuilt from the surviving values, so a reader sees a region that never held
 * the field. Its scan finds no tag, the {@code longCount + doubleCount == anchorSlots} oracle fails,
 * and that field alone keeps the record path. Every other field keeps its column, which is what the
 * assertions below pin: before this, one bad field returned a null payload for the entire region.
 */
@DisplayName("DoubleRegion unsound-tag scope")
final class DoubleRegionUnsoundTagScopeTest {

  private static final int UNSOUND_TAG = 7;
  private static final int SOUND_TAG = 9;

  /** Scale-2 values whose unscaled form overflows a lift to scale 14, plus one scale-14 value. */
  private static final long[] UNSOUND_UNSCALED = { 10_000_000L, 10_000_100L, 10_000_200L,
                                                   100_025_000_000_000_001L };
  private static final int[] UNSOUND_SCALES = { 2, 2, 2, 14 };

  /** Ordinary scale-2 prices: all-decimal, one common scale, no lift at all. */
  private static final long[] SOUND_UNSCALED = { 1_999L, 10_010L, 3_333L, 4_242L };
  private static final int[] SOUND_SCALES = { 2, 2, 2, 2 };

  private static byte[] encodeBoth(final boolean withSoundTag) {
    final int nu = UNSOUND_UNSCALED.length;
    final int ns = withSoundTag ? SOUND_UNSCALED.length : 0;
    final int n = nu + ns;
    final double[] values = new double[n];
    final long[] unscaled = new long[n];
    final int[] scales = new int[n];
    final int[] tags = new int[n];
    final int[] ordinals = new int[n];
    for (int i = 0; i < nu; i++) {
      unscaled[i] = UNSOUND_UNSCALED[i];
      scales[i] = UNSOUND_SCALES[i];
      values[i] = UNSOUND_UNSCALED[i] / DoubleRegion.exp10(UNSOUND_SCALES[i]);
      tags[i] = UNSOUND_TAG;
      ordinals[i] = i;
    }
    for (int i = 0; i < ns; i++) {
      unscaled[nu + i] = SOUND_UNSCALED[i];
      scales[nu + i] = SOUND_SCALES[i];
      values[nu + i] = SOUND_UNSCALED[i] / DoubleRegion.exp10(SOUND_SCALES[i]);
      tags[nu + i] = SOUND_TAG;
      ordinals[nu + i] = i;
    }
    return DoubleRegion.encode(values, unscaled, scales, tags, ordinals, n,
                               NumberRegion.TAG_KIND_NAME);
  }

  @Test
  @DisplayName("an unsound tag is dropped; every other field keeps its column")
  void unsoundTagIsDroppedAndTheRestSurvives() {
    final byte[] wire = encodeBoth(true);
    assertNotNull(wire,
                  "one unsound field must not cost the page its whole double region — the other "
                      + "field's values are perfectly encodable");
    final MemorySegment seg = PaxTestSegments.of(wire);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h, "the surviving region must still parse");

    assertTrue(DoubleRegion.lookupTag(h, UNSOUND_TAG) < 0,
               "the unsound tag must be absent, so the completeness oracle refuses THAT field "
                   + "rather than the column serving an approximated value");
    final int sound = DoubleRegion.lookupTag(h, SOUND_TAG);
    assertTrue(sound >= 0, "the sound field must still have its column");
    assertEquals(DoubleRegion.ENC_DEC, h.tagEnc[sound],
                 "an all-decimal tag at one scale is still carried exactly");
    assertEquals(SOUND_UNSCALED.length, h.tagCount[sound], "every surviving value must be present");
    assertEquals(1, h.dictSize, "only the surviving tag may remain in the dict");
    assertEquals(SOUND_UNSCALED.length, h.count, "the total must be rebuilt, not inherited");
  }

  @Test
  @DisplayName("dropping the only tag leaves no region at all")
  void droppingTheOnlyTagYieldsNoRegion() {
    assertNull(encodeBoth(false),
               "with nothing left to carry, the region is absent — which every caller already "
                   + "handles as 'no double column'");
  }

  @Test
  @DisplayName("the surviving tag encodes bit-for-bit as if the unsound one had never been there")
  void survivingTagIsUnaffectedByTheDrop() {
    final int n = SOUND_UNSCALED.length;
    final double[] values = new double[n];
    final int[] tags = new int[n];
    final int[] ordinals = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = SOUND_UNSCALED[i] / DoubleRegion.exp10(SOUND_SCALES[i]);
      tags[i] = SOUND_TAG;
      ordinals[i] = i;
    }
    final byte[] alone = DoubleRegion.encode(values, SOUND_UNSCALED.clone(), SOUND_SCALES.clone(),
                                             tags, ordinals, n, NumberRegion.TAG_KIND_NAME);
    assertNotNull(alone);
    assertEquals(alone.length, encodeBoth(true).length,
                 "the drop must rebuild the region as though the unsound field never existed");
  }
}
