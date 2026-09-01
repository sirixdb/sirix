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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
  private static class FakeDictionary implements GlobalStringDictionaries {
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
    public byte @Nullable [] valueOf(final int t, final long dictionaryKey, final int recordedEntryCount,
        final int id) {
      // The real shape: the anchor check is part of resolving, not a thing the caller remembers.
      return t == tag && accepts(t, dictionaryKey, recordedEntryCount) ? values.get(id) : null;
    }

    /** Live entry count, separate from the recorded one so a shrink is constructible. */
    int liveEntryCount = -1;

    @Override
    public boolean accepts(final int t, final long dictionaryKey, final int recordedEntryCount) {
      // The real rule, reproduced: right dictionary, and it cannot have shrunk. A rank-ordered
      // dictionary only appends, so a smaller live count is a different dictionary under a reused
      // key and its ids mean something else.
      if (t != tag || dictionaryKey != dictionaryKey(t)) {
        return false;
      }
      final int live = liveEntryCount < 0 ? dictionaryEntryCount(t) : liveEntryCount;
      return live >= recordedEntryCount;
    }

    @Override
    public long dictionaryKey(final int t) {
      return 168227L;
    }

    @Override
    public int dictionaryEntryCount(final int t) {
      return ids.size();
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
    final long key = header.tagDictionaryKey[tag];
    final int count = header.tagDictionaryEntryCount[tag];
    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8),
        dict.valueOf(CONVERTED_TAG, key, count, StringRegion.globalIdAt(payload, header, tag, 0)));
    assertArrayEquals("beta".getBytes(StandardCharsets.UTF_8),
        dict.valueOf(CONVERTED_TAG, key, count, StringRegion.globalIdAt(payload, header, tag, 1)));
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
  void aConvertedTagNamesTheDictionaryItWasEncodedAgainst() {
    // The anchor is the difference between a format and a trap. Without it a copy-on-write leaf from
    // an earlier generation resolves against whatever dictionary is current, and a rank rebuild
    // reassigns every id -- plausible wrong values for a page nobody touched.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);

    assertEquals(168227L, header.tagDictionaryKey[tag], "the page must name its dictionary");
    assertEquals(2, header.tagDictionaryEntryCount[tag], "and the cardinality it was encoded against");
  }

  @Test
  void aTagNamingADictionaryTooSmallForItsIdsIsRefused() {
    // A dictionary reporting fewer entries than the ids it issues is inconsistent, and it is
    // refused. Note WHICH layer refuses: the density assertion catches it at ENCODE, before a byte
    // is written, so the parse-time guard never sees this fixture. That guard is still load-bearing
    // -- it defends a payload this encoder did not write, corrupt or produced elsewhere -- but the
    // reachable path for a live inconsistency is the encoder, and asserting the parse exception
    // here would have been asserting a layer that no longer runs for this input.
    final GlobalStringDictionaries lying = new FakeDictionary(CONVERTED_TAG, "alpha", "beta") {
      @Override
      public int dictionaryEntryCount(final int t) {
        return 1; // fewer entries than the ids this tag stores
      }
    };
    final RuntimeException refusal = assertThrows(RuntimeException.class,
        () -> parse(encode(lying, CONVERTED_TAG, "alpha", "beta", "beta")),
        "a tag whose dictionary is too small for its own ids must be refused");
    assertTrue(refusal instanceof IllegalStateException,
        "expected the ENCODE-time density refusal, got " + refusal.getClass().getSimpleName() + ": "
            + refusal.getMessage());
  }

  @Test
  void idsArePackedAtTheWidthTheDictionarySizeImplies() {
    // The fixture above has a two-entry dictionary, so its ids pack into 2 bits and a bug that
    // defaulted the width to 32 would round-trip anyway. This one uses a dictionary big enough that
    // the width is load-bearing: 1,000 entries need 10 bits, and ids 500 and 900 do not survive a
    // narrower field or land correctly in a wider one.
    final FakeDictionary wide = new FakeDictionary(CONVERTED_TAG, "alpha", "beta") {
      @Override
      public int dictionaryEntryCount(final int t) {
        return 1000;
      }

      @Override
      public int idOf(final int t, final byte[] value, final int offset, final int length) {
        final String v = new String(value, offset, length, StandardCharsets.UTF_8);
        return "alpha".equals(v) ? 500 : ("beta".equals(v) ? 900 : ID_ABSENT);
      }
    };
    final MemorySegment payload = encode(wide, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);

    assertTrue(header.tagGlobal[tag]);
    assertEquals(1000, header.tagDictionaryEntryCount[tag]);
    // The ids survive the pack/unpack at ten bits, which they cannot do at a wrong width.
    assertEquals(500, StringRegion.globalIdAt(payload, header, tag, 0));
    assertEquals(900, StringRegion.globalIdAt(payload, header, tag, 1));

    // And the table really is packed: two ids at 10 bits is 3 bytes, not 8.
    final int idTableBytes = header.tagStringBytesOffset[tag] - header.tagStringDictOffset[tag];
    assertEquals(3, idTableBytes, "two 10-bit ids must occupy 3 bytes, not " + idTableBytes);
  }

  @Test
  void aResolverRefusesADictionaryThatHasSHRUNK() {
    // THE check the anchor exists for, and the one that cannot live in the parse: parse only sees
    // this leaf's ~6 ids, so it cannot tell a reused key from a valid one. Only a resolver holds the
    // live dictionary.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    final long recordedKey = header.tagDictionaryKey[tag];
    final int recordedCount = header.tagDictionaryEntryCount[tag];

    // Unchanged dictionary: accepted.
    assertTrue(dict.accepts(CONVERTED_TAG, recordedKey, recordedCount), "a matching anchor must be accepted");

    // The same key now holding FEWER entries is a different dictionary. Refuse.
    dict.liveEntryCount = recordedCount - 1;
    assertFalse(dict.accepts(CONVERTED_TAG, recordedKey, recordedCount),
        "a dictionary smaller than the page recorded must be REFUSED, not resolved");

    // A page naming some other dictionary is equally unreadable.
    dict.liveEntryCount = -1;
    assertFalse(dict.accepts(CONVERTED_TAG, recordedKey + 1, recordedCount),
        "a page naming a different dictionary must be refused");
  }

  @Test
  void resolvingItselfRefusesAShrunkDictionary() {
    // The check is part of valueOf, so a caller cannot resolve without it. This is the property that
    // an ordering requirement in prose would NOT have given us -- forgetting to call accepts() would
    // simply have returned values.
    final FakeDictionary dict = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    final MemorySegment payload = encode(dict, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    final long key = header.tagDictionaryKey[tag];
    final int count = header.tagDictionaryEntryCount[tag];
    final int id = StringRegion.globalIdAt(payload, header, tag, 0);

    assertNotNull(dict.valueOf(CONVERTED_TAG, key, count, id), "a valid anchor resolves");

    final FakeDictionary shrunk = new FakeDictionary(CONVERTED_TAG, "alpha", "beta");
    shrunk.liveEntryCount = count - 1;
    assertNull(shrunk.valueOf(CONVERTED_TAG, key, count, id),
        "resolution itself must refuse a dictionary smaller than the page recorded");
  }

  @Test
  void aSparseDictionaryIsRefusedAtEncodeTime() {
    // The invariant the width derivation rests on, asserted rather than assumed. Ids run
    // 1..entryCount with no gaps, which is the ONLY reason the count bounds the width. A sparse
    // dictionary -- reserved ranges, tombstones, per-column partitioning -- would issue an id above
    // the count, the derived width would be too narrow, and the id would be written TRUNCATED: a
    // silently different value rather than a failure. So the encoder refuses instead.
    final FakeDictionary sparse = new FakeDictionary(CONVERTED_TAG, "alpha", "beta") {
      @Override
      public int idOf(final int t, final byte[] value, final int offset, final int length) {
        // An id far above the two entries this dictionary reports -- what a reserved range or a
        // tombstoned id space would produce.
        return 5000;
      }
    };
    final IllegalStateException refusal = assertThrows(IllegalStateException.class,
        () -> encode(sparse, CONVERTED_TAG, "alpha", "beta", "beta"));
    assertTrue(refusal.getMessage().contains("dense"),
        "the refusal must name the invariant it is defending, got: " + refusal.getMessage());
  }

  @Test
  void globalIdAtRefusesATagThatIsNotGlobal() {
    final MemorySegment payload = encode(null, CONVERTED_TAG, "alpha", "beta", "beta");
    final StringRegion.Header header = parse(payload);
    final int tag = StringRegion.lookupTag(header, CONVERTED_TAG);
    assertThrows(IllegalStateException.class, () -> StringRegion.globalIdAt(payload, header, tag, 0));
  }
}
