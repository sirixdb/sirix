package io.sirix.page.pax;

import io.sirix.utils.FSSTCompressor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The property that matters for {@link StringDictSketch} is one-sided: a value that IS in the
 * dictionary must never be reported absent, because the scan takes "absent" as the page's final
 * answer and never looks at the dictionary. A value that is not in it may be reported present —
 * that only costs a decompression.
 */
final class StringDictSketchTest {

  /** An encoded string region: the payload and a header parsed from it. */
  private record Region(byte[] payload, StringRegion.Header header) {
  }

  private static Region encodeRegion(final List<String> values, final int tag) {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.reset();
    for (final String v : values) {
      enc.addValue(tag, v.getBytes(StandardCharsets.UTF_8));
    }
    final byte[] payload = enc.finish(StringRegion.TAG_KIND_NAME);
    assertNotNull(payload, "encoder produced no payload");
    return new Region(payload, new StringRegion.Header().parseInto(payload));
  }

  @Test
  void everyDictionaryEntryIsReportedPresent() {
    final Random rng = new Random(0xB100FL);
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      values.add("value-" + rng.nextInt(1_000_000) + "-" + i);
    }
    final Region r = encodeRegion(values, 42);
    final byte[] sketch = StringDictSketch.encodeFromStringRegion(r.payload(), r.header());
    assertNotNull(sketch, "a raw dictionary must produce a sketch");

    for (final String v : values) {
      assertTrue(StringDictSketch.mayContain(sketch, v.getBytes(StandardCharsets.UTF_8)),
                 "false negative for a value that IS in the dictionary: " + v);
    }
  }

  @Test
  void absentValuesAreUsuallyRuledOut() {
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      values.add("present-" + i);
    }
    final Region r = encodeRegion(values, 7);
    final byte[] sketch = StringDictSketch.encodeFromStringRegion(r.payload(), r.header());

    int falsePositives = 0;
    final int probes = 20_000;
    for (int i = 0; i < probes; i++) {
      if (StringDictSketch.mayContain(sketch, ("absent-" + i).getBytes(StandardCharsets.UTF_8))) {
        falsePositives++;
      }
    }
    // Designed for ~1 %; assert an order of magnitude of headroom so the test pins the sizing
    // without becoming a hash-function tripwire.
    assertTrue(falsePositives < probes / 10,
               "false-positive rate too high: " + falsePositives + "/" + probes);
  }

  @Test
  void anAbsentSketchNeverRulesAnythingOut() {
    assertTrue(StringDictSketch.mayContain(null, "anything".getBytes(StandardCharsets.UTF_8)));
    assertTrue(StringDictSketch.mayContain(new byte[0], "anything".getBytes(StandardCharsets.UTF_8)));
    // A payload claiming a future version must also fail open, never closed.
    final byte[] futureVersion = new byte[32];
    futureVersion[0] = 99;
    assertTrue(StringDictSketch.mayContain(futureVersion, "x".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void emptyDictionaryProducesNoSketch() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.reset();
    final byte[] payload = enc.finish(StringRegion.TAG_KIND_NAME);
    if (payload == null || payload.length == 0) {
      assertNull(StringDictSketch.encodeFromStringRegion(payload, new StringRegion.Header()));
      return;
    }
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    assertNull(StringDictSketch.encodeFromStringRegion(payload, h));
  }

  /**
   * The FSST case, which is the whole reason the sketch hashes STORED bytes: a dictionary of
   * encoded entries must still be probeable, by encoding the literal the same way. Nothing here
   * decompresses anything.
   */
  @Test
  void fsstEncodedEntriesAreProbeableWithoutDecompressing() {
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      values.add("the quick brown fox jumps over the lazy dog number " + i);
    }
    final List<byte[]> samples = new ArrayList<>();
    for (final String v : values) {
      samples.add(v.getBytes(StandardCharsets.UTF_8));
    }
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assumeTrue(table != null && table.length > 0, "corpus not compressible on this build");
    final byte[][] symbols = FSSTCompressor.parsedFor(table);

    // Store each value the way the writer would: encoded when that is beneficial, raw otherwise.
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.reset();
    int encodedCount = 0;
    for (final String v : values) {
      final byte[] raw = v.getBytes(StandardCharsets.UTF_8);
      final byte[] encoded = FSSTCompressor.encodeOrNull(raw, 0, raw.length, symbols);
      if (encoded != null) {
        enc.addValue(3, encoded, true);
        encodedCount++;
      } else {
        enc.addValue(3, raw, false);
      }
    }
    assumeTrue(encodedCount > 0, "nothing was encoded — the FSST path is not exercised");

    final byte[] payload = enc.finish(StringRegion.TAG_KIND_NAME);
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    final byte[] sketch = StringDictSketch.encodeFromStringRegion(payload, h);
    assertNotNull(sketch, "an FSST dictionary must still produce a sketch");

    for (final String v : values) {
      final byte[] raw = v.getBytes(StandardCharsets.UTF_8);
      final byte[] encoded = FSSTCompressor.encodeOrNull(raw, 0, raw.length, symbols);
      final boolean found = StringDictSketch.mayContain(sketch, raw)
          || (encoded != null && StringDictSketch.mayContain(sketch, encoded));
      assertTrue(found, "false negative for an FSST-stored value: " + v);

      // And the dictionary lookup itself must find it, comparing stored bytes only.
      final int tag = StringRegion.lookupTag(h, 3);
      assertTrue(StringRegion.findDictId(payload, h, tag, raw, encoded) >= 0,
                 "dictionary lookup missed an FSST-stored value: " + v);
    }
  }

  /** Without a symbol table an encoded entry must be reported undecidable, never "absent". */
  @Test
  void encodedEntriesWithoutATableAreUndecidable() {
    final byte[] raw = "the quick brown fox jumps over the lazy dog, repeatedly".getBytes(StandardCharsets.UTF_8);
    final List<byte[]> samples = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      samples.add(raw);
    }
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assumeTrue(table != null && table.length > 0, "corpus not compressible on this build");
    final byte[] encoded = FSSTCompressor.encodeOrNull(raw, 0, raw.length, FSSTCompressor.parsedFor(table));
    assumeTrue(encoded != null, "value not beneficially compressible on this build");

    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.reset();
    enc.addValue(9, encoded, true);
    final byte[] payload = enc.finish(StringRegion.TAG_KIND_NAME);
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    final int tag = StringRegion.lookupTag(h, 9);
    assertEquals(StringRegion.DICT_ID_UNDECIDABLE,
                 StringRegion.findDictId(payload, h, tag, raw, null),
                 "an encoded entry with no table in hand must be undecidable, not absent");
  }

  @Test
  void singleEntryDictionaryRoundTrips() {
    final Region r = encodeRegion(List.of("solo"), 1);
    final byte[] sketch = StringDictSketch.encodeFromStringRegion(r.payload(), r.header());
    assertNotNull(sketch);
    assertTrue(StringDictSketch.mayContain(sketch, "solo".getBytes(StandardCharsets.UTF_8)));
    assertFalse(StringDictSketch.mayContain(sketch, "not-solo".getBytes(StandardCharsets.UTF_8)));
    final int tag = StringRegion.lookupTag(r.header(), 1);
    assertEquals(0, StringRegion.findDictId(r.payload(), r.header(), tag,
                                            "solo".getBytes(StandardCharsets.UTF_8), null));
    assertEquals(StringRegion.DICT_ID_ABSENT,
                 StringRegion.findDictId(r.payload(), r.header(), tag,
                                         "nope".getBytes(StandardCharsets.UTF_8), null));
  }
}
