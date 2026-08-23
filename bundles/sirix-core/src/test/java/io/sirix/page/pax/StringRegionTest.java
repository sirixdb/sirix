package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the BtrBlocks/Umbra-style dictionary-encoded {@link StringRegion}: encode → parse →
 * decode round-trip, plus a compression-ratio sanity check on the reference workload to prove the
 * page-size motivation from the compression study.
 */
@DisplayName("StringRegion")
final class StringRegionTest {

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("empty encoder → zero-length payload")
  void emptyRoundTrip() {
    final byte[] wire = new StringRegion.Encoder().finish();
    assertEquals(0, wire.length);
  }

  @Test
  @DisplayName("single tag, three distinct values — dict + bit-pack round-trip")
  void singleTagRoundTrip() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int deptKey = 7;
    enc.addValue(deptKey, bytes("Eng"));
    enc.addValue(deptKey, bytes("Sales"));
    enc.addValue(deptKey, bytes("Eng"));
    enc.addValue(deptKey, bytes("Mkt"));
    enc.addValue(deptKey, bytes("Eng"));

    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(PaxTestSegments.of(wire));
    assertEquals(5, h.count);
    assertEquals(1, h.parentDictSize);
    assertEquals(deptKey, h.parentDict[0]);
    assertEquals(0, h.tagStart[0]);
    assertEquals(5, h.tagCount[0]);
    assertEquals(3, h.tagStringDictSize[0]);
    assertEquals(2, h.valueBitWidthEff); // ceil(log2(3)) = 2 bits per dict id

    // Round-trip: decode each record's dict-id and look up its string.
    final String[] expected = {"Eng", "Sales", "Eng", "Mkt", "Eng"};
    for (int i = 0; i < 5; i++) {
      final int dictId = StringRegion.decodeDictIdAt(PaxTestSegments.of(wire), h, i);
      final int off = StringRegion.decodeStringOffset(PaxTestSegments.of(wire), h, 0, dictId);
      final int len = StringRegion.decodeStringLength(PaxTestSegments.of(wire), h, 0, dictId);
      final String actual = new String(wire, off, len, StandardCharsets.UTF_8);
      assertEquals(expected[i], actual, "record " + i);
    }
  }

  @Test
  @DisplayName("multiple tags (dept + city) — each gets its own local dict")
  void multipleTagsRoundTrip() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int deptKey = 7, cityKey = 9;
    enc.addValue(deptKey, bytes("Eng"));
    enc.addValue(cityKey, bytes("NYC"));
    enc.addValue(deptKey, bytes("Sales"));
    enc.addValue(cityKey, bytes("NYC"));
    enc.addValue(deptKey, bytes("Eng"));
    enc.addValue(cityKey, bytes("LA"));

    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(PaxTestSegments.of(wire));
    assertEquals(6, h.count);
    assertEquals(2, h.parentDictSize);

    final int deptTag = StringRegion.lookupTag(h, deptKey);
    final int cityTag = StringRegion.lookupTag(h, cityKey);
    assertTrue(deptTag >= 0 && cityTag >= 0);

    assertEquals(3, h.tagCount[deptTag]);
    assertEquals(3, h.tagCount[cityTag]);
    assertEquals(2, h.tagStringDictSize[deptTag]); // Eng, Sales
    assertEquals(2, h.tagStringDictSize[cityTag]); // NYC, LA

    // Record-order within each tag is preserved.
    final String[] deptExpected = {"Eng", "Sales", "Eng"};
    for (int i = 0; i < 3; i++) {
      final int idx = h.tagStart[deptTag] + i;
      final int dictId = StringRegion.decodeDictIdAt(PaxTestSegments.of(wire), h, idx);
      final int off = StringRegion.decodeStringOffset(PaxTestSegments.of(wire), h, deptTag, dictId);
      final int len = StringRegion.decodeStringLength(PaxTestSegments.of(wire), h, deptTag, dictId);
      assertEquals(deptExpected[i], new String(wire, off, len, StandardCharsets.UTF_8));
    }
    final String[] cityExpected = {"NYC", "NYC", "LA"};
    for (int i = 0; i < 3; i++) {
      final int idx = h.tagStart[cityTag] + i;
      final int dictId = StringRegion.decodeDictIdAt(PaxTestSegments.of(wire), h, idx);
      final int off = StringRegion.decodeStringOffset(PaxTestSegments.of(wire), h, cityTag, dictId);
      final int len = StringRegion.decodeStringLength(PaxTestSegments.of(wire), h, cityTag, dictId);
      assertEquals(cityExpected[i], new String(wire, off, len, StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("reference workload — 90 records × 2 string fields, 8 unique each")
  void compressionRatioOnReferenceWorkload() {
    final String[] depts = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};
    final String[] cities = {"NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL"};
    final int deptKey = 7, cityKey = 9;
    final int records = 90;

    final StringRegion.Encoder enc = new StringRegion.Encoder();
    int rawBytes = 0;
    for (int i = 0; i < records; i++) {
      final byte[] d = bytes(depts[i % depts.length]);
      final byte[] c = bytes(cities[(i * 3) % cities.length]);
      enc.addValue(deptKey, d);
      enc.addValue(cityKey, c);
      // Raw in-record cost: per value = 1 varint length (~1B) + UTF-8 bytes.
      rawBytes += 1 + d.length + 1 + c.length;
    }
    final byte[] wire = enc.finish();

    // Expected ratio: raw ~850 B, StringRegion well under 250 B — 3× or better
    // on this small sample; the larger the page + lower the cardinality, the
    // better the ratio grows.
    assertTrue(wire.length < rawBytes, "StringRegion payload should be smaller than raw in-record strings: " + "raw="
        + rawBytes + " encoded=" + wire.length);
    final double ratio = (double) rawBytes / wire.length;
    // Guard against regressions — we expect well above 2× on this workload.
    assertTrue(ratio > 2.0, "Expected >2× ratio on reference workload, got " + ratio + "× (raw=" + rawBytes
        + ", encoded=" + wire.length + ")");
  }

  @Test
  @DisplayName("bit-packed dict ids — width scales with max local dict size")
  void bitWidthScalesWithDictSize() {
    // 3 unique → 2 bits
    final StringRegion.Encoder e3 = new StringRegion.Encoder();
    for (int i = 0; i < 10; i++)
      e3.addValue(1, bytes("A" + (i % 3)));
    final byte[] w3 = e3.finish();
    assertEquals(2, new StringRegion.Header().parseInto(PaxTestSegments.of(w3)).valueBitWidthEff);

    // 9 unique → 4 bits
    final StringRegion.Encoder e9 = new StringRegion.Encoder();
    for (int i = 0; i < 20; i++)
      e9.addValue(1, bytes("B" + (i % 9)));
    final byte[] w9 = e9.finish();
    assertEquals(4, new StringRegion.Header().parseInto(PaxTestSegments.of(w9)).valueBitWidthEff);
  }

  @Test
  @DisplayName("randomized round-trip (100 pages × 90 records, 2 fields)")
  void randomizedRoundTrip() {
    final String[] depts = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};
    final String[] cities = {"NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL"};
    final Random rng = new Random(42);

    for (int page = 0; page < 100; page++) {
      final StringRegion.Encoder enc = new StringRegion.Encoder();
      final String[] expectedDept = new String[90];
      final String[] expectedCity = new String[90];
      for (int i = 0; i < 90; i++) {
        expectedDept[i] = depts[rng.nextInt(depts.length)];
        expectedCity[i] = cities[rng.nextInt(cities.length)];
        enc.addValue(7, bytes(expectedDept[i]));
        enc.addValue(9, bytes(expectedCity[i]));
      }
      final byte[] wire = enc.finish();
      final StringRegion.Header h = new StringRegion.Header().parseInto(PaxTestSegments.of(wire));
      final int deptTag = StringRegion.lookupTag(h, 7);
      final int cityTag = StringRegion.lookupTag(h, 9);
      for (int i = 0; i < 90; i++) {
        // Dept
        final int deptIdx = h.tagStart[deptTag] + i;
        final int deptDictId = StringRegion.decodeDictIdAt(PaxTestSegments.of(wire), h, deptIdx);
        final int deptOff = StringRegion.decodeStringOffset(PaxTestSegments.of(wire), h, deptTag, deptDictId);
        final int deptLen = StringRegion.decodeStringLength(PaxTestSegments.of(wire), h, deptTag, deptDictId);
        assertArrayEquals(bytes(expectedDept[i]), Arrays.copyOfRange(wire, deptOff, deptOff + deptLen),
            "page " + page + " record " + i + " dept");
        // City
        final int cityIdx = h.tagStart[cityTag] + i;
        final int cityDictId = StringRegion.decodeDictIdAt(PaxTestSegments.of(wire), h, cityIdx);
        final int cityOff = StringRegion.decodeStringOffset(PaxTestSegments.of(wire), h, cityTag, cityDictId);
        final int cityLen = StringRegion.decodeStringLength(PaxTestSegments.of(wire), h, cityTag, cityDictId);
        assertArrayEquals(bytes(expectedCity[i]), Arrays.copyOfRange(wire, cityOff, cityOff + cityLen),
            "page " + page + " record " + i + " city");
      }
    }
  }

  @Test
  @DisplayName("reusable output remains wire-exact across large, small, large, and empty pages")
  void reusableOutputAcrossPageSizes() {
    final StringRegion.Encoder reused = new StringRegion.Encoder();

    addDistinctValues(reused, 11, "large-a-", 96);
    assertReusableWireEqualsDetached(reused, StringRegion.TAG_KIND_NAME, false);

    reused.reset();
    reused.addValue(3, bytes("x"));
    reused.addValue(3, bytes("y"));
    reused.addValue(3, bytes("x"));
    assertReusableWireEqualsDetached(reused, StringRegion.TAG_KIND_PATH_NODE, false);

    reused.reset();
    addDistinctValues(reused, 17, "large-b-", 144);
    assertReusableWireEqualsDetached(reused, StringRegion.TAG_KIND_PATH_NODE, true);

    reused.reset();
    assertEquals(0, reused.encodeInto(StringRegion.TAG_KIND_NAME, false));
    assertEquals(0, reused.encodedLength());
  }

  @Test
  @DisplayName("finish is detached while RegionTable synchronously owns an encoded scratch prefix")
  void detachedFinishAndRegionTableOwnership() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.addValue(5, bytes("first"));
    enc.addValue(5, bytes("second"));

    final byte[] first = enc.finish();
    final byte[] second = enc.finish();
    assertNotSame(first, second);
    assertArrayEquals(first, second);
    first[0] ^= 0x7f;
    assertArrayEquals(second, enc.finish(), "mutating a finished payload must not mutate encoder scratch");

    final int length = enc.encodeInto(StringRegion.TAG_KIND_NAME, false);
    final byte[] expected = Arrays.copyOf(enc.output(), length);
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_STRING, enc.output(), length);
    Arrays.fill(enc.output(), (byte) 0x5a);
    assertArrayEquals(expected, PaxTestSegments.bytes(table.payload(RegionTable.KIND_STRING)));
  }

  @Test
  @DisplayName("reusable encoding preserves tag kind, element guarantee, and signed FSST lengths")
  void reusableHeaderAndFsstFlagsAreExact() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.addValue(23, bytes("raw"), false);
    enc.addValue(23, bytes("encoded"), true);

    final int length = enc.encodeInto(StringRegion.TAG_KIND_PATH_NODE, true);
    final byte[] wire = Arrays.copyOf(enc.output(), length);
    final StringRegion.Header header = new StringRegion.Header().parseInto(PaxTestSegments.of(wire));
    final int tag = StringRegion.lookupTag(header, 23);

    assertEquals(StringRegion.ENC_DICT_BITPACKED_ZM_ELEMENTS, header.encodingKind);
    assertEquals(StringRegion.TAG_KIND_PATH_NODE, header.tagKind);
    assertFalse(StringRegion.isEntryCompressed(PaxTestSegments.of(wire), header, tag, 0));
    assertTrue(StringRegion.isEntryCompressed(PaxTestSegments.of(wire), header, tag, 1));
    assertEquals(bytes("raw").length, StringRegion.decodeStringLength(PaxTestSegments.of(wire), header, tag, 0));
    assertEquals(bytes("encoded").length, StringRegion.decodeStringLength(PaxTestSegments.of(wire), header, tag, 1));
    assertArrayEquals(wire, enc.finish(StringRegion.TAG_KIND_PATH_NODE, true));
  }

  @Test
  @DisplayName("scratch slices are copied only into owned dictionary entries and remain wire-exact")
  void scratchSliceOwnershipAndWireParity() {
    final byte[] raw = bytes("raw-value");
    final byte[] encoded = bytes("encoded-value");
    final byte[] scratch = new byte[64];
    final StringRegion.Encoder sliced = new StringRegion.Encoder();
    final StringRegion.Encoder pathCandidate = new StringRegion.Encoder();

    System.arraycopy(raw, 0, scratch, 7, raw.length);
    sliced.addValueCopiedAndShareWith(23, scratch, 7, raw.length, false, pathCandidate, 101);
    Arrays.fill(scratch, (byte) 0x55);

    // Replaying the same value from a different slice is a dictionary hit. It must neither retain
    // the borrowed array nor create a second dictionary entry.
    System.arraycopy(raw, 0, scratch, 19, raw.length);
    sliced.addValueCopiedAndShareWith(23, scratch, 19, raw.length, false, pathCandidate, 101);
    Arrays.fill(scratch, (byte) 0x33);

    System.arraycopy(encoded, 0, scratch, 3, encoded.length);
    sliced.addValueCopiedAndShareWith(23, scratch, 3, encoded.length, true, pathCandidate, 101);
    Arrays.fill(scratch, (byte) 0x11);

    final StringRegion.Encoder detached = new StringRegion.Encoder();
    detached.addValue(23, raw, false);
    detached.addValue(23, raw, false);
    detached.addValue(23, encoded, true);
    final byte[] expected = detached.finish(StringRegion.TAG_KIND_PATH_NODE);
    final byte[] actual = sliced.finish(StringRegion.TAG_KIND_PATH_NODE);
    assertArrayEquals(expected, actual);

    // PageKind builds name- and path-tagged candidates together. The bridge keeps the canonical
    // private while giving the alternate encoder the exact same stored representation.
    final StringRegion.Encoder expectedPathCandidate = new StringRegion.Encoder();
    expectedPathCandidate.addValue(101, raw, false);
    expectedPathCandidate.addValue(101, raw, false);
    expectedPathCandidate.addValue(101, encoded, true);
    assertArrayEquals(expectedPathCandidate.finish(StringRegion.TAG_KIND_PATH_NODE),
        pathCandidate.finish(StringRegion.TAG_KIND_PATH_NODE));
    assertTrue(sliced.sharesCanonicalValueWith(23, 0, pathCandidate, 101, 0),
        "raw entry must be one store range shared by the name/path candidates");
    assertTrue(sliced.sharesCanonicalValueWith(23, 1, pathCandidate, 101, 1),
        "FSST entry must be one store range shared by the name/path candidates");

    final StringRegion.Header header = new StringRegion.Header().parseInto(PaxTestSegments.of(actual));
    assertEquals(2, header.tagStringDictSize[0]);
    assertFalse(StringRegion.isEntryCompressed(PaxTestSegments.of(actual), header, 0, 0));
    assertTrue(StringRegion.isEntryCompressed(PaxTestSegments.of(actual), header, 0, 1));

    assertThrows(IndexOutOfBoundsException.class, () -> sliced.addValue(23, scratch, -1, 1, false));
    assertThrows(IndexOutOfBoundsException.class, () -> sliced.addValue(23, scratch, scratch.length - 1, 2, false));
    assertThrows(NullPointerException.class, () -> sliced.addValue(23, null, 0, 0, false));
  }

  @Test
  @DisplayName("grow-only store stays bounded across true/false/false/true path-summary reuse")
  void reusableValueStoreAcrossCandidateResetAndFailure() {
    final StringRegion.Encoder name = new StringRegion.Encoder();
    final StringRegion.Encoder path = new StringRegion.Encoder();
    final byte[] scratch = new byte[192];

    addSharedScratchValue(name, path, scratch, "same-value", false);
    addSharedScratchValue(name, path, scratch, "same-value", true);
    addSharedScratchValue(name, path, scratch, "same-value", false);
    final byte[] nameA = name.finish(StringRegion.TAG_KIND_NAME);
    final byte[] pathA = path.finish(StringRegion.TAG_KIND_PATH_NODE);
    final StringRegion.Header aHeader = new StringRegion.Header().parseInto(PaxTestSegments.of(nameA));
    assertEquals(2, aHeader.tagStringDictSize[0], "stored representation and FSST flag define identity");
    assertEquals(0, StringRegion.decodeDictIdAt(PaxTestSegments.of(nameA), aHeader, 0));
    assertEquals(1, StringRegion.decodeDictIdAt(PaxTestSegments.of(nameA), aHeader, 1));
    assertEquals(0, StringRegion.decodeDictIdAt(PaxTestSegments.of(nameA), aHeader, 2));
    assertFalse(StringRegion.isEntryCompressed(PaxTestSegments.of(nameA), aHeader, 0, 0));
    assertTrue(StringRegion.isEntryCompressed(PaxTestSegments.of(nameA), aHeader, 0, 1));
    assertTrue(name.sharesCanonicalValueWith(23, 0, path, 101, 0));
    assertTrue(name.sharesCanonicalValueWith(23, 1, path, 101, 1));
    assertEquals(2 * bytes("same-value").length, name.valueStoreLength());
    assertEquals(0, path.valueStoreLength(), "the alternative must not copy shared dictionary misses");

    // Resetting the owner first cannot invalidate ranges retained by the alternative candidate.
    name.reset();
    assertTrue(name.valueStoreLength() > 0);
    assertArrayEquals(pathA, path.finish(StringRegion.TAG_KIND_PATH_NODE));
    path.reset();
    assertEquals(0, name.valueStoreLength());

    // true -> false: B is name-only and exceeds the initial store size, establishing a retained
    // high-water capacity. The path candidate was reset above even though this page does not use it.
    int bLogicalLength = 0;
    for (int i = 0; i < 48; i++) {
      final String value = "large-page-value-" + i + "-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
      bLogicalLength += bytes(value).length;
      addScratchValue(name, scratch, value, (i & 1) != 0);
    }
    final byte[] nameB = name.finish(StringRegion.TAG_KIND_NAME);
    final int highWaterCapacity = name.valueStoreCapacity();
    assertTrue(highWaterCapacity > 1024);
    assertEquals(bLogicalLength, name.valueStoreLength());
    assertThrows(IllegalArgumentException.class, () -> name.encodeInto((byte) 99, false));
    assertEquals(0, name.encodedLength());
    assertArrayEquals(nameB, name.finish(StringRegion.TAG_KIND_NAME),
        "a failed encode attempt must not change dictionary state or store ranges");

    // false -> false: PageKind resets both thread-local candidates when the name encoder is acquired,
    // regardless of the current resource mode. A second name-only page must reuse offset zero rather
    // than append after B forever.
    name.reset();
    path.reset();
    assertEquals(0, name.valueStoreLength());
    assertEquals(highWaterCapacity, name.valueStoreCapacity());
    for (int i = 0; i < 48; i++) {
      final String value = "large-page-value-" + i + "-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
      addScratchValue(name, scratch, value, (i & 1) != 0);
    }
    assertEquals(bLogicalLength, name.valueStoreLength());
    assertEquals(highWaterCapacity, name.valueStoreCapacity());
    assertArrayEquals(nameB, name.finish(StringRegion.TAG_KIND_NAME));

    // false -> true: capacity stays at B's high-water mark and both candidate wires reproduce A.
    name.reset();
    path.reset();
    assertEquals(0, name.valueStoreLength());
    assertEquals(highWaterCapacity, name.valueStoreCapacity());

    addSharedScratchValue(name, path, scratch, "same-value", false);
    addSharedScratchValue(name, path, scratch, "same-value", true);
    addSharedScratchValue(name, path, scratch, "same-value", false);
    assertArrayEquals(nameA, name.finish(StringRegion.TAG_KIND_NAME));
    assertArrayEquals(pathA, path.finish(StringRegion.TAG_KIND_PATH_NODE));
    assertEquals(highWaterCapacity, name.valueStoreCapacity());
  }

  private static void addScratchValue(final StringRegion.Encoder encoder, final byte[] scratch, final String value,
      final boolean compressed) {
    final byte[] source = bytes(value);
    final int offset = 7;
    System.arraycopy(source, 0, scratch, offset, source.length);
    encoder.addValue(23, scratch, offset, source.length, compressed);
    Arrays.fill(scratch, (byte) 0x5a);
  }

  private static void addSharedScratchValue(final StringRegion.Encoder name, final StringRegion.Encoder path,
      final byte[] scratch, final String value, final boolean compressed) {
    final byte[] source = bytes(value);
    final int offset = 7;
    System.arraycopy(source, 0, scratch, offset, source.length);
    name.addValueCopiedAndShareWith(23, scratch, offset, source.length, compressed, path, 101);
    Arrays.fill(scratch, (byte) 0x5a);
  }

  private static void addDistinctValues(final StringRegion.Encoder enc, final int tag, final String prefix,
      final int count) {
    for (int i = 0; i < count; i++) {
      enc.addValue(tag, bytes(prefix + i + "-abcdefghijklmnopqrstuvwxyz"));
    }
  }

  private static void assertReusableWireEqualsDetached(final StringRegion.Encoder enc, final byte tagKind,
      final boolean elementsStaged) {
    final int length = enc.encodeInto(tagKind, elementsStaged);
    final byte[] reusableWire = Arrays.copyOf(enc.output(), length);
    assertEquals(length, enc.encodedLength());
    assertArrayEquals(reusableWire, enc.finish(tagKind, elementsStaged));
  }

}
