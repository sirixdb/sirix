package io.sirix.page;

import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.pax.StringRegion;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The trie lane: a tag whose dictionary is stored as global ids rather than value bytes.
 *
 * <p>
 * Every case here is written to FAIL if the discriminator is ignored, because the failure this
 * format cannot afford is not an exception — it is an id table read as a length table, which
 * returns plausible bytes and is undetectable afterwards. So the assertions are on the shape of the
 * encoding and on the refusals, not only on a value surviving a round trip.
 * </p>
 */
final class StringRegionGlobalLaneTest {

  private static final int CONVERTED_TAG = 7;
  private static final int PLAIN_TAG = 9;

  /** A dictionary that holds exactly what it was given, so an "absent" case is constructible. */
  private static final class FakeDictionary implements GlobalStringDictionaries {
    private final Map<String, Integer> ids = new HashMap<>();
    private final Map<Integer, byte[]> values = new HashMap<>();
    private final int tag;

    FakeDictionary(final int tag, final String... entries) {
      this.tag = tag;
      for (int i = 0; i < entries.length; i++) {
        ids.put(entries[i], i + 1);
        values.put(i + 1, entries[i].getBytes(StandardCharsets.UTF_8));
      }
    }

    @Override
    public boolean hasDictionary(final int t) {
      return t == tag;
    }

    @Override
    public int idOf(final int t, final byte[] value, final int offset, final int length) {
      if (t != tag) {
        return ID_ABSENT;
      }
      return ids.getOrDefault(new String(value, offset, length, StandardCharsets.UTF_8), ID_ABSENT);
    }

    @Override
    public byte @Nullable [] valueOf(final int t, final int id) {
      return t == tag ? values.get(id) : null;
    }
  }

  private static MemorySegment encode(final @Nullable GlobalStringDictionaries dict, final int tag,
      final String... values) {
    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    encoder.setDictionaries(dict);
    for (final String v : values) {
      encoder.addValue(tag, v.getBytes(StandardCharsets.UTF_8));
    }
    final byte[] payload = encoder.finish((byte) 1);
    return MemorySegment.ofArray(payload);
  }

  private static StringRegion.Header parse(final MemorySegment payload) {
    return new StringRegion.Header().parseInto(payload);
  }

  @Test
  void aConvertedTagStoresIdsAndNoBytes() {
    // "b" repeats so the tag is NOT plain: a plain tag never converts, by design.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);

    assertTrue(header.tagGlobal[tag], "the tag must be marked global on the wire");
    assertEquals(2, header.tagStringDictSize[tag], "magnitude survives the sign discriminator");

    // The ids, in local dictionary order, are what the dictionary assigned.
    assertEquals(1, StringRegion.globalIdAt(payload, header, tag, 0));
    assertEquals(2, StringRegion.globalIdAt(payload, header, tag, 1));

    // And they resolve back to the values, which is the round trip the lane exists to preserve.
    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8),
        dict.valueOf(CONVERTED_TAG, StringRegion.globalIdAt(payload, header, tag, 0)));
    assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8),
        dict.valueOf(CONVERTED_TAG, StringRegion.globalIdAt(payload, header, tag, 1)));
  }

  @Test
  void theConvertedFormIsSmallerThanTheBytesItReplaces() {
    // The lever, in miniature: long values, few distinct, so ids beat bytes decisively.
    final String a = "http://example.com/a-fairly-long-url-that-costs-real-bytes/aaaa";
    final String b = "http://example.com/a-fairly-long-url-that-costs-real-bytes/bbbb";
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, a, b);
    final int withLane = encode(dict, CONVERTED_TAG, a, b, b, a).byteSize() > 0
        ? (int) encode(dict, CONVERTED_TAG, a, b, b, a).byteSize()
        : -1;
    final int withBytes = (int) encode(null, CONVERTED_TAG, a, b, b, a).byteSize();
    assertTrue(withLane < withBytes,
        "the lane must be smaller than the bytes: lane=" + withLane + " bytes=" + withBytes);
  }

  @Test
  void withoutAResolverNothingConverts() {
    // The kill switch is the absence of a resolver, and it must reproduce the old encoding exactly.
    final MemorySegment withResolver = encode(null, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(withResolver);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    assertFalse(header.tagGlobal[tag], "no resolver must mean no conversion");
  }

  @Test
  void oneAbsentValueKeepsTheWholeTagAsBytes() {
    // ALL OR NOTHING. "gamma" is missing from the dictionary, so the tag must not convert at all --
    // a half-converted tag is unreadable and nothing on the wire would say so.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "gamma", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    assertFalse(header.tagGlobal[tag], "one absent value must keep the whole tag on bytes");
  }

  @Test
  void aTagWithoutADictionaryIsUntouched() {
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, PLAIN_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, PLAIN_TAG);
    assertFalse(header.tagGlobal[tag], "a tag the resolver disclaims must not convert");
  }

  @Test
  void theByteReadersRefuseAGlobalTagRatherThanMisreadIt() {
    // This is the case the whole design turns on. decodeStringLength over an id table would NOT
    // throw on its own -- it would read four bytes of an id and call them a length. The guard is
    // what makes that impossible, so the test asserts the guard and not the arithmetic.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);

    assertThrows(IllegalStateException.class, () -> StringRegion.decodeStringLength(payload, header, tag, 0));
    assertThrows(IllegalStateException.class, () -> StringRegion.decodeStringOffset(payload, header, tag, 0));
    assertEquals(StringRegion.DICT_ID_UNDECIDABLE,
        StringRegion.findDictId(payload, header, tag, "alpha".getBytes(StandardCharsets.UTF_8), null),
        "a literal search must route to the dictionary, not scan an id table");
  }

  @Test
  void globalIdAtRefusesATagThatIsNotGlobal() {
    final MemorySegment payload = encode(null, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    assertThrows(IllegalStateException.class, () -> StringRegion.globalIdAt(payload, header, tag, 0));
  }
}
