package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the BtrBlocks/Umbra-style dictionary-encoded
 * {@link StringRegion}: encode → parse → decode round-trip, plus a
 * compression-ratio sanity check on the reference workload to prove the
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
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
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
      final int dictId = StringRegion.decodeDictIdAt(wire, h, i);
      final int off = StringRegion.decodeStringOffset(wire, h, 0, dictId);
      final int len = StringRegion.decodeStringLength(wire, h, 0, dictId);
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
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
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
      final int dictId = StringRegion.decodeDictIdAt(wire, h, idx);
      final int off = StringRegion.decodeStringOffset(wire, h, deptTag, dictId);
      final int len = StringRegion.decodeStringLength(wire, h, deptTag, dictId);
      assertEquals(deptExpected[i], new String(wire, off, len, StandardCharsets.UTF_8));
    }
    final String[] cityExpected = {"NYC", "NYC", "LA"};
    for (int i = 0; i < 3; i++) {
      final int idx = h.tagStart[cityTag] + i;
      final int dictId = StringRegion.decodeDictIdAt(wire, h, idx);
      final int off = StringRegion.decodeStringOffset(wire, h, cityTag, dictId);
      final int len = StringRegion.decodeStringLength(wire, h, cityTag, dictId);
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
    assertTrue(wire.length < rawBytes,
        "StringRegion payload should be smaller than raw in-record strings: "
            + "raw=" + rawBytes + " encoded=" + wire.length);
    final double ratio = (double) rawBytes / wire.length;
    // Guard against regressions — we expect well above 2× on this workload.
    assertTrue(ratio > 2.0,
        "Expected >2× ratio on reference workload, got " + ratio + "× (raw=" + rawBytes
            + ", encoded=" + wire.length + ")");
  }

  @Test
  @DisplayName("bit-packed dict ids — width scales with max local dict size")
  void bitWidthScalesWithDictSize() {
    // 3 unique → 2 bits
    final StringRegion.Encoder e3 = new StringRegion.Encoder();
    for (int i = 0; i < 10; i++) e3.addValue(1, bytes("A" + (i % 3)));
    final byte[] w3 = e3.finish();
    assertEquals(2, new StringRegion.Header().parseInto(w3).valueBitWidthEff);

    // 9 unique → 4 bits
    final StringRegion.Encoder e9 = new StringRegion.Encoder();
    for (int i = 0; i < 20; i++) e9.addValue(1, bytes("B" + (i % 9)));
    final byte[] w9 = e9.finish();
    assertEquals(4, new StringRegion.Header().parseInto(w9).valueBitWidthEff);
  }

  @Test
  @DisplayName("randomized round-trip (100 pages × 90 records, 2 fields)")
  void randomizedRoundTrip() {
    final String[] depts = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};
    final String[] cities = {"NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL"};
    final java.util.Random rng = new java.util.Random(42);

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
      final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
      final int deptTag = StringRegion.lookupTag(h, 7);
      final int cityTag = StringRegion.lookupTag(h, 9);
      for (int i = 0; i < 90; i++) {
        // Dept
        final int deptIdx = h.tagStart[deptTag] + i;
        final int deptDictId = StringRegion.decodeDictIdAt(wire, h, deptIdx);
        final int deptOff = StringRegion.decodeStringOffset(wire, h, deptTag, deptDictId);
        final int deptLen = StringRegion.decodeStringLength(wire, h, deptTag, deptDictId);
        assertArrayEquals(bytes(expectedDept[i]),
            Arrays.copyOfRange(wire, deptOff, deptOff + deptLen),
            "page " + page + " record " + i + " dept");
        // City
        final int cityIdx = h.tagStart[cityTag] + i;
        final int cityDictId = StringRegion.decodeDictIdAt(wire, h, cityIdx);
        final int cityOff = StringRegion.decodeStringOffset(wire, h, cityTag, cityDictId);
        final int cityLen = StringRegion.decodeStringLength(wire, h, cityTag, cityDictId);
        assertArrayEquals(bytes(expectedCity[i]),
            Arrays.copyOfRange(wire, cityOff, cityOff + cityLen),
            "page " + page + " record " + i + " city");
      }
    }
  }
}
