package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RecordOrdinalRegion}: the round-trip, and — more importantly — the alignment
 * certificate, which is the only thing standing between a two-column predicate and values paired
 * across record boundaries.
 */
@DisplayName("RecordOrdinalRegion")
final class RecordOrdinalRegionTest {

  @Test
  @DisplayName("round-trip: ordinals are dense and in first-appearance order")
  void roundTrip() {
    // Three records of two fields each, laid out as the shredder lays them out: the object node at
    // slot 0/3/6 and its two fields immediately after.
    final int[] parentSlots = { 0, 0, 3, 3, 6, 6 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, parentSlots.length);
    assertNotNull(wire);
    assertEquals(RecordOrdinalRegion.encodedSize(6, 3), wire.length);

    final var h = new RecordOrdinalRegion.Header().parseInto(PaxTestSegments.of(wire));
    assertNotNull(h);
    assertEquals(6, h.okCount);
    assertEquals(3, h.recordCount);
    assertEquals(2, h.bitWidth);  // ceil(log2(3)) = 2

    final var seg = PaxTestSegments.of(wire);
    final int[] expected = { 0, 0, 1, 1, 2, 2 };
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], RecordOrdinalRegion.ordinalAt(seg, h, i), "slot " + i);
    }
    assertEquals(-1, RecordOrdinalRegion.ordinalAt(seg, h, 6), "past the end");
    assertEquals(-1, RecordOrdinalRegion.ordinalAt(seg, h, -1), "before the start");
  }

  @Test
  @DisplayName("a single record needs no bits at all")
  void singleRecordIsZeroWidth() {
    final int[] parentSlots = { 4, 4, 4 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 3);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertNotNull(h);
    assertEquals(1, h.recordCount);
    assertEquals(0, h.bitWidth);
    for (int i = 0; i < 3; i++) {
      assertEquals(0, RecordOrdinalRegion.ordinalAt(seg, h, i));
    }
  }

  /**
   * A parent on another page — anywhere past the leading run — makes the whole region unwritable.
   *
   * <p>Not "write the entries we can": a reader cannot tell which entry is the unusable one, so a
   * column that is right for most slots would link the rest to whichever record sat at that ordinal.
   */
  @Test
  @DisplayName("an off-page parent past the leading run refuses the whole region")
  void offPageParentRefusesTheRegion() {
    assertNull(RecordOrdinalRegion.encode(new int[] { 0, 0, -1, 3 }, 4),
               "a parent outside the page must refuse the region, not write a partial one");
    assertNull(RecordOrdinalRegion.encode(new int[] { 0, 1024 }, 2),
               "a parent slot past the page must refuse the region");
    assertNull(RecordOrdinalRegion.encode(new int[] { 0, 0 }, 0), "nothing to link");
    assertNull(RecordOrdinalRegion.encode(null, 4));
    assertNull(RecordOrdinalRegion.encode(new int[] { -1, -1 }, 2),
               "a page holding nothing but the spanning tail has no record to link");
  }

  // ─────────────────────────────────────────────────────────────── the skip prefix

  /**
   * The shape MOST pages of a multi-field corpus have: the first record's object node sits at the
   * tail of the previous page, so the head of this page is that record's remaining field nodes.
   * Refusing it meant refusing the linkage almost everywhere; instead the leading run is recorded
   * as a skip prefix, carries no ordinals, and every field's column window starts past its share.
   */
  @Test
  @DisplayName("a leading off-page run becomes a skip prefix, not a refusal")
  void leadingOffPageRunBecomesSkipPrefix() {
    // Two tail slots of the spanning record, then records at object slots 2 and 5, two fields each.
    final int[] parentSlots = { -1, -1, 2, 2, 5, 5 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 6);
    assertNotNull(wire, "a leading off-page run must not refuse the region");
    assertEquals(RecordOrdinalRegion.encodedSize(6, 2, 2), wire.length);

    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertNotNull(h);
    assertEquals(6, h.okCount);
    assertEquals(2, h.skipCount);
    assertEquals(2, h.recordCount);

    assertEquals(-1, RecordOrdinalRegion.ordinalAt(seg, h, 0), "prefix slots have no ordinal");
    assertEquals(-1, RecordOrdinalRegion.ordinalAt(seg, h, 1), "prefix slots have no ordinal");
    assertEquals(0, RecordOrdinalRegion.ordinalAt(seg, h, 2));
    assertEquals(0, RecordOrdinalRegion.ordinalAt(seg, h, 3));
    assertEquals(1, RecordOrdinalRegion.ordinalAt(seg, h, 4));
    assertEquals(1, RecordOrdinalRegion.ordinalAt(seg, h, 5));
  }

  @Test
  @DisplayName("alignedLead reports each field's share of the prefix")
  void alignedLeadReportsTheFieldsShareOfThePrefix() {
    // The spanning record's tail carries field a (bitmap 0) and field b (bitmap 1); then two
    // records at object slots 2 and 5 carry both fields: a at 2,4 and b at 3,5.
    final int[] parentSlots = { -1, -1, 2, 2, 5, 5 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 6);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertNotNull(h);

    assertEquals(1, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 0, 2, 4 }, 3, 2),
                 "field a: one prefix occurrence, then records 0..1 in order");
    assertEquals(1, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 1, 3, 5 }, 3, 2),
                 "field b: one prefix occurrence, then records 0..1 in order");
    assertEquals(0, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 2, 4 }, 2, 2),
                 "a field absent from the spanning tail has no lead");
    assertEquals(-1, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 0, 2 }, 2, 2),
                 "prefix occurrence plus HALF the records must decline");
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 2, 4 }, 3),
                "isRecordAligned means lead == 0 over the whole page, exactly");
    // A narrower window: the field enumerates record 0 and then a trailing record — the shape a
    // tail-partial record produces. Window m=1 accepts it; a trailing occurrence that maps INSIDE
    // the window is a duplicate and declines.
    assertEquals(1, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 0, 2, 4 }, 3, 1),
                 "entries past the window may map to records at or past m");
    assertEquals(-1, RecordOrdinalRegion.alignedLead(seg, h, new int[] { 0, 2, 3 }, 3, 1),
                 "an entry past the window mapping inside it is a duplicate");
  }

  @Test
  @DisplayName("a truncated or future-version payload parses to null, never to wrong bounds")
  void unreadablePayloadDeclines() {
    final byte[] wire = RecordOrdinalRegion.encode(new int[] { 0, 0, 3, 3 }, 4);
    final var h = new RecordOrdinalRegion.Header();

    assertNull(h.parseInto(null));
    assertNull(h.parseInto(PaxTestSegments.of(new byte[3])), "shorter than the fixed header");

    final byte[] future = wire.clone();
    future[0] = 99;
    assertNull(h.parseInto(PaxTestSegments.of(future)), "a future version must decline");

    final byte[] truncated = new byte[wire.length - 1];
    System.arraycopy(wire, 0, truncated, 0, truncated.length);
    assertNull(h.parseInto(PaxTestSegments.of(truncated)), "ordinals cut short must decline");

    // A bitWidth inconsistent with the declared record count is corruption, not a variant encoding:
    // reading it would decode every ordinal at the wrong stride.
    final byte[] badWidth = wire.clone();
    badWidth[7] = 7;
    assertNull(h.parseInto(PaxTestSegments.of(badWidth)),
               "a bitWidth that does not match recordCount must decline");

    // A skip prefix swallowing the whole region is a shape the encoder never writes.
    final byte[] badSkip = wire.clone();
    badSkip[3] = 4;
    assertNull(h.parseInto(PaxTestSegments.of(badSkip)),
               "skipCount >= okCount must decline");
  }

  /**
   * A declined parse must not leave the reusable header holding a previous page's numbers.
   *
   * <p>The scratch is shared across every page of a scan. If {@code parseInto} committed fields as
   * it went, a page whose payload turned out to be truncated would leave this page's okCount beside
   * the previous page's offsets — a mixture that reads as valid and links against another page.
   */
  @Test
  @DisplayName("a declined parse leaves the scratch untouched")
  void declinedParseDoesNotCorruptTheScratch() {
    final var h = new RecordOrdinalRegion.Header();
    final byte[] good = RecordOrdinalRegion.encode(new int[] { 0, 0, 3, 3, 6, 6 }, 6);
    assertNotNull(h.parseInto(PaxTestSegments.of(good)));
    final int okCount = h.okCount;
    final int recordCount = h.recordCount;
    final int bitWidth = h.bitWidth;

    final byte[] otherShape = RecordOrdinalRegion.encode(new int[] { 0, 0, 0, 0, 5, 5, 5, 5 }, 8);
    final byte[] truncated = new byte[otherShape.length - 1];
    System.arraycopy(otherShape, 0, truncated, 0, truncated.length);
    assertNull(h.parseInto(PaxTestSegments.of(truncated)));

    assertEquals(okCount, h.okCount, "okCount survived a declined parse");
    assertEquals(recordCount, h.recordCount, "recordCount survived a declined parse");
    assertEquals(bitWidth, h.bitWidth, "bitWidth survived a declined parse");
  }

  // ─────────────────────────────────────────────────────── the alignment certificate

  @Test
  @DisplayName("a field on every record, in record order, is aligned")
  void denseFieldIsAligned() {
    // Records (a,b) × 4: a at bitmap positions 0,2,4,6 and b at 1,3,5,7.
    final int[] parentSlots = { 0, 0, 3, 3, 6, 6, 9, 9 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 8);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);

    assertTrue(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 2, 4, 6 }, 4), "field a");
    assertTrue(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 1, 3, 5, 7 }, 4), "field b");
  }

  /**
   * The shape that defeats every inference drawn from the field-name sequence alone.
   *
   * <p>Records {@code (a,c)} and {@code (b,d)} produce the name sequence {@code a,c,b,d,a,c,b,d} —
   * perfectly periodic, four distinct names per period, every field on exactly half the records. Yet
   * the k-th {@code a} and the k-th {@code b} are in DIFFERENT records, so pairing them positionally
   * pairs across a record boundary. The certificate is what catches it: neither field maps to
   * {@code 0..R-1}.
   */
  @Test
  @DisplayName("a periodic-but-misaligned page is refused")
  void periodicButMisalignedIsRefused() {
    // Four records of two fields: (a,c) (b,d) (a,c) (b,d) at object slots 0,3,6,9.
    final int[] parentSlots = { 0, 0, 3, 3, 6, 6, 9, 9 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 8);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertEquals(4, h.recordCount);

    // `a` sits at bitmap positions 0 and 4 — records 0 and 2, not 0 and 1.
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 4 }, 2),
                "a field on half the records must not pass as covering all of them");
    // `b` sits at 2 and 6 — records 1 and 3.
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 2, 6 }, 2));
  }

  @Test
  @DisplayName("a sparse field is refused")
  void sparseFieldIsRefused() {
    // Three records, but the field only appears on the first and third.
    final int[] parentSlots = { 0, 0, 3, 6, 6 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 5);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertEquals(3, h.recordCount);
    // Positions 0 and 3 are records 0 and 2 — a gap the caller must not paper over.
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 3 }, 2));
  }

  @Test
  @DisplayName("a field appearing twice in one record is refused")
  void duplicateFieldInOneRecordIsRefused() {
    // Record 0 carries the field twice, record 1 once: two entries, two records, but the first two
    // entries are both record 0.
    final int[] parentSlots = { 0, 0, 4 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 3);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertEquals(2, h.recordCount);
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 1 }, 2),
                "two occurrences within one record must not pass as one per record");
  }

  @Test
  @DisplayName("interleaved fields, so slots do not follow record order, are refused")
  void interleavedFieldsAreRefused() {
    // Parent slots out of order: entry 0 belongs to the object at slot 5, entry 1 to slot 2. First
    // appearance gives slot 5 ordinal 0 and slot 2 ordinal 1, so the field's positions map to
    // 0,1 — but the SECOND field, appearing in the other order, maps to 1,0 and is refused.
    final int[] parentSlots = { 5, 2, 2, 5 };
    final byte[] wire = RecordOrdinalRegion.encode(parentSlots, 4);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertEquals(2, h.recordCount);
    assertTrue(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0, 1 }, 2));
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 2, 3 }, 2),
                "a field whose slots run counter to record order must be refused");
  }

  @Test
  @DisplayName("a null header or over-long count is refused rather than read")
  void defensiveInputsAreRefused() {
    assertFalse(RecordOrdinalRegion.isRecordAligned(null, null, new int[] { 0 }, 1));
    final byte[] wire = RecordOrdinalRegion.encode(new int[] { 0, 0 }, 2);
    final var seg = PaxTestSegments.of(wire);
    final var h = new RecordOrdinalRegion.Header().parseInto(seg);
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, null, 1));
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0 }, 2), "n past the array");
    assertFalse(RecordOrdinalRegion.isRecordAligned(seg, h, new int[] { 0 }, -1));
  }

  /**
   * Randomized round-trip across every bit width a page can produce.
   *
   * <p>Bit widths 1..10 are all reachable within one page, and the packing straddles byte boundaries
   * at every width that is not a power of two — the case a hand-written fixture is least likely to
   * cover and the bit-packer most likely to get wrong.
   */
  @Test
  @DisplayName("randomized round-trip over every reachable bit width")
  void randomizedRoundTrip() {
    final Random rng = new Random(0xC0FFEEL);
    for (int records = 1; records <= 512; records = records < 8 ? records + 1 : records * 2) {
      for (int trial = 0; trial < 8; trial++) {
        final int fieldsPerRecord = 1 + rng.nextInt(3);
        final int okCount = records * fieldsPerRecord;
        if (okCount > 1024) {
          continue;
        }
        // Distinct object slots, ascending, one per record; each owns fieldsPerRecord entries.
        final int[] parentSlots = new int[okCount];
        final int[] expected = new int[okCount];
        int w = 0;
        for (int r = 0; r < records; r++) {
          final int objectSlot = r;  // any injective map works; the ordinals are what matter
          for (int f = 0; f < fieldsPerRecord; f++) {
            parentSlots[w] = objectSlot;
            expected[w] = r;
            w++;
          }
        }
        final byte[] wire = RecordOrdinalRegion.encode(parentSlots, okCount);
        assertNotNull(wire, "records=" + records + " fields=" + fieldsPerRecord);
        final var seg = PaxTestSegments.of(wire);
        final var h = new RecordOrdinalRegion.Header().parseInto(seg);
        assertNotNull(h);
        assertEquals(records, h.recordCount);
        assertEquals(RecordOrdinalRegion.bitWidthFor(records), h.bitWidth);
        for (int i = 0; i < okCount; i++) {
          assertEquals(expected[i], RecordOrdinalRegion.ordinalAt(seg, h, i),
                       "records=" + records + " fields=" + fieldsPerRecord + " entry " + i);
        }
        // Each field position within the record is a dense, in-order enumeration of all records.
        for (int f = 0; f < fieldsPerRecord; f++) {
          final int[] positions = new int[records];
          for (int r = 0; r < records; r++) {
            positions[r] = r * fieldsPerRecord + f;
          }
          assertTrue(RecordOrdinalRegion.isRecordAligned(seg, h, positions, records),
                     "field " + f + " of " + fieldsPerRecord + " at records=" + records);
        }
      }
    }
  }
}
