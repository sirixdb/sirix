/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bit-packed {@code mint -> rank} table of a sealed segment dictionary: the pack/read identity at
 * every width (including entries that straddle a word), the wire round trip, and the refusals a
 * corrupt record must earn rather than a wrong position.
 */
final class ValueDictionaryRankTableNodeTest {

  private static final int RECORD = ValueDictionaryRankTableNode.ENTRIES_PER_RECORD;

  private static ValueDictionaryRankTableNode roundTrip(final ValueDictionaryRankTableNode table) {
    try (final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_RANK_TABLE.serialize(sink, table, null);
      return (ValueDictionaryRankTableNode) NodeKind.VALUE_DICTIONARY_RANK_TABLE.deserialize(
          Bytes.wrapForRead(sink.toByteArray()), table.getNodeKey(), null, null);
    }
  }

  private static byte[] serialize(final ValueDictionaryRankTableNode table) {
    try (final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_RANK_TABLE.serialize(sink, table, null);
      return sink.toByteArray();
    }
  }

  private static ValueDictionaryRankTableNode deserialize(final byte[] bytes) {
    return (ValueDictionaryRankTableNode) NodeKind.VALUE_DICTIONARY_RANK_TABLE.deserialize(Bytes.wrapForRead(bytes),
        5L, null, null);
  }

  /** Ranks indexed by mint, slot 0 unused, each a value that fits {@code bits} and is at least 1. */
  private static int[] ranksFitting(final int firstMint, final int count, final int bits, final long seed) {
    final SplittableRandom random = new SplittableRandom(seed);
    final long limit = Math.min(Integer.MAX_VALUE, (1L << bits) - 1L);
    final int[] ranks = new int[firstMint + count];
    for (int i = 0; i < count; i++) {
      // Bias toward the extremes: all-ones and 1 are the values a shift-by-one error mangles.
      final int pick = random.nextInt(4);
      ranks[firstMint + i] = pick == 0
          ? (int) limit
          : pick == 1
              ? 1
              : (int) (1L + random.nextLong(limit));
    }
    return ranks;
  }

  @Test
  @DisplayName("every width from 1 to 32 bits reads back exactly what was packed, straddles included")
  void packReadIdentityAtEveryWidth() {
    for (int bits = 1; bits <= ValueDictionaryRankTableNode.MAX_BITS_PER_ENTRY; bits++) {
      // 3 * 64 + 5 entries: enough to cross several word boundaries at every width, with a ragged end.
      final int count = 197;
      final int[] ranks = ranksFitting(1, count, bits, 0x5EEDL + bits);
      final ValueDictionaryRankTableNode table = ValueDictionaryRankTableNode.pack(9L, 1, ranks, count, bits);
      assertEquals(bits, table.bitsPerEntry());
      assertEquals(count, table.size());
      assertEquals(1, table.firstKey());
      for (int mint = 1; mint <= count; mint++) {
        assertEquals(ranks[mint], table.entryOf(mint), "bits=" + bits + " mint=" + mint);
        assertEquals(ranks[mint], table.entryAt(mint - 1), "bits=" + bits + " index=" + (mint - 1));
      }
      assertNoStrayBits(table, bits);
    }
  }

  /**
   * Every bit at or above {@code count * bits} must be zero: the high half of a straddling entry is
   * placed with {@code rank >>> ~s >>> 1}, which is zero at {@code s == 0}. Written as a plain shift by
   * {@code 64 - s} it would be a shift by 64 — a no-op in Java — and the WHOLE rank would land in the
   * next entry's word, corrupting it. The padding word is included in the sweep.
   */
  private static void assertNoStrayBits(final ValueDictionaryRankTableNode table, final int bits) {
    final long[] words = table.words();
    final long used = (long) table.size() * bits;
    for (long bit = used; bit < (long) words.length * 64; bit++) {
      final long word = words[(int) (bit >>> 6)];
      assertEquals(0L, word >>> (bit & 63) & 1L, "stray bit at " + bit + " for bits=" + bits);
    }
  }

  @Test
  @DisplayName("a record that starts above mint 1 ranks by mint, not by index")
  void secondRecordRanksByMint() {
    final int firstMint = 1 + 2 * RECORD;
    final int count = 300;
    final int[] ranks = ranksFitting(firstMint, count, 19, 77L);
    final ValueDictionaryRankTableNode table = ValueDictionaryRankTableNode.pack(3L, firstMint, ranks, count, 19);
    assertTrue(table.covers(firstMint));
    assertTrue(table.covers(firstMint + count - 1));
    assertFalse(table.covers(firstMint - 1));
    assertFalse(table.covers(firstMint + count));
    for (int mint = firstMint; mint < firstMint + count; mint++) {
      assertEquals(ranks[mint], table.entryOf(mint));
    }
    assertEquals(ranks[firstMint], table.entryAt(0));
    assertThrows(IndexOutOfBoundsException.class, () -> table.entryAt(count));
    assertThrows(IndexOutOfBoundsException.class, () -> table.entryAt(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> table.entryOf(firstMint - 1));
  }

  @Test
  @DisplayName("a full 16384-entry record at 32 bits round-trips through the record codec")
  void fullRecordRoundTrips() {
    final int[] ranks = ranksFitting(1, RECORD, 32, 11L);
    final ValueDictionaryRankTableNode table = ValueDictionaryRankTableNode.pack(7L, 1, ranks, RECORD, 32);
    final ValueDictionaryRankTableNode read = roundTrip(table);
    assertEquals(7L, read.getNodeKey());
    assertEquals(1, read.firstKey());
    assertEquals(RECORD, read.size());
    assertEquals(32, read.bitsPerEntry());
    assertArrayEquals(table.words(), read.words());
    for (int mint = 1; mint <= RECORD; mint++) {
      assertEquals(ranks[mint], read.entryOf(mint));
    }
    assertEquals(NodeKind.VALUE_DICTIONARY_RANK_TABLE, read.getKind());
  }

  @Test
  @DisplayName("the smallest record — one mint, one bit — round-trips")
  void smallestRecordRoundTrips() {
    final ValueDictionaryRankTableNode table = ValueDictionaryRankTableNode.pack(2L, 1, new int[] {0, 1}, 1, 1);
    final ValueDictionaryRankTableNode read = roundTrip(table);
    assertEquals(1, read.size());
    assertEquals(1, read.entryOf(1));
    assertEquals(2, read.words().length, "one data word plus the padding word");
  }

  @Test
  @DisplayName("bitsFor and recordCountFor follow the prefix count exactly")
  void widthAndRecordCountArithmetic() {
    assertEquals(1, ValueDictionaryRankTableNode.bitsFor(1));
    assertEquals(2, ValueDictionaryRankTableNode.bitsFor(2));
    assertEquals(2, ValueDictionaryRankTableNode.bitsFor(3));
    assertEquals(3, ValueDictionaryRankTableNode.bitsFor(4));
    assertEquals(14, ValueDictionaryRankTableNode.bitsFor(16383));
    assertEquals(15, ValueDictionaryRankTableNode.bitsFor(16384));
    assertEquals(19, ValueDictionaryRankTableNode.bitsFor(275_494));
    assertEquals(31, ValueDictionaryRankTableNode.bitsFor(Integer.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.bitsFor(0));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.bitsFor(-1));

    assertEquals(1, ValueDictionaryRankTableNode.recordCountFor(1));
    assertEquals(1, ValueDictionaryRankTableNode.recordCountFor(RECORD));
    assertEquals(2, ValueDictionaryRankTableNode.recordCountFor(RECORD + 1));
    assertEquals(17, ValueDictionaryRankTableNode.recordCountFor(275_494));
    assertEquals(131_072, ValueDictionaryRankTableNode.recordCountFor(Integer.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.recordCountFor(0));

    // Every rank of a prefix fits bitsFor(prefix): the top of the range is the value to check.
    for (final int prefix : new int[] {1, 2, 3, 4, 255, 256, 257, 16384, 275_494}) {
      final int bits = ValueDictionaryRankTableNode.bitsFor(prefix);
      assertTrue(prefix <= (1L << bits) - 1L, "prefix " + prefix + " must fit " + bits + " bits");
      assertTrue(prefix > (1L << bits - 1) - 1L, "prefix " + prefix + " must NOT fit " + (bits - 1) + " bits");
    }
  }

  @Test
  @DisplayName("pack refuses a rank that is zero or does not fit the width")
  void packRefusesUnrepresentableRanks() {
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.pack(1L, 1, new int[] {0, 1, 0, 3}, 3, 2), "rank 0 is not a position");
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.pack(1L, 1, new int[] {0, 1, 4, 3}, 3, 2), "rank 4 needs 3 bits");
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.pack(1L, 1, new int[] {0, 1, -1, 3}, 3, 2), "negative rank");
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.pack(1L, 1, new int[] {0, 1, 2}, 3, 2),
        "fewer ranks than the record covers");
    assertThrows(NullPointerException.class, () -> ValueDictionaryRankTableNode.pack(1L, 1, null, 1, 1));
  }

  @Test
  @DisplayName("both constructors refuse a mis-shaped record")
  void shapeIsChecked() {
    final int[] ranks = ranksFitting(1, 4, 3, 1L);
    // A record must start at 1 + k * ENTRIES_PER_RECORD.
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, 2, ranks, 2, 3));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, 0, ranks, 2, 3));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, RECORD, ranks, 2, 3));
    // 1..ENTRIES_PER_RECORD entries.
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, 1, ranks, 0, 3));
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.pack(1L, 1, new int[RECORD + 2], RECORD + 1, 3));
    // 1..32 bits.
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, 1, ranks, 4, 0));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(1L, 1, ranks, 4, 33));
    // A positive key.
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryRankTableNode.pack(0L, 1, ranks, 4, 3));
    // takeOwnership demands exactly wordsFor(count, bits) words -- the padding word included.
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.takeOwnership(1L, 1, 4, 3, new long[1]), "missing the padding word");
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryRankTableNode.takeOwnership(1L, 1, 4, 3, new long[3]), "one word too many");
    assertThrows(NullPointerException.class, () -> ValueDictionaryRankTableNode.takeOwnership(1L, 1, 4, 3, null));
    final long[] owned = new long[2];
    assertSame(owned, ValueDictionaryRankTableNode.takeOwnership(1L, 1, 4, 3, owned).words(),
        "takeOwnership adopts, never copies");
  }

  /**
   * The codec's own guards (count, width, word budget) refuse with {@link IllegalStateException}, the
   * convention of every record codec here for a record that cannot be what it claims; the start-mint
   * shape is validated once, in {@code takeOwnership}, and surfaces as its
   * {@link IllegalArgumentException}. Either way a corrupt record never yields a wrong position.
   */
  @Test
  @DisplayName("the deserializer refuses a corrupt count, width, start or truncated word array")
  void deserializerRefusesCorruption() {
    final int[] ranks = ranksFitting(1, 100, 7, 3L);
    final byte[] good = serialize(ValueDictionaryRankTableNode.pack(5L, 1, ranks, 100, 7));
    assertEquals(100, deserialize(good).size(), "the uncorrupted bytes read back");
    // Layout: firstMint:int(0..3), count:int(4..7), bitsPerEntry:byte(8), words...
    assertEquals(9 + 12 * Long.BYTES, good.length, "100 entries at 7 bits = 11 data words + 1 padding word");
    assertThrows(IllegalArgumentException.class, () -> deserialize(withInt(good, 0, 2)), "firstMint 2");
    assertThrows(IllegalArgumentException.class, () -> deserialize(withInt(good, 0, RECORD)), "firstMint 16384");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 4, 0)), "count 0");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 4, -1)), "negative count");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 4, RECORD + 1)), "count too large");
    assertThrows(IllegalStateException.class, () -> deserialize(withInt(good, 4, 101)),
        "count 101 needs one more word than the record carries");
    assertThrows(IllegalStateException.class, () -> deserialize(withByte(good, 8, (byte) 0)), "0 bits");
    assertThrows(IllegalStateException.class, () -> deserialize(withByte(good, 8, (byte) 33)), "33 bits");
    assertThrows(IllegalStateException.class, () -> deserialize(withByte(good, 8, (byte) -1)), "negative width");
    assertThrows(IllegalStateException.class, () -> deserialize(withByte(good, 8, (byte) 32)),
        "32 bits for 100 entries needs more words than 7 bits wrote");
    final byte[] truncated = new byte[good.length - Long.BYTES];
    System.arraycopy(good, 0, truncated, 0, truncated.length);
    assertThrows(IllegalStateException.class, () -> deserialize(truncated), "one word short");
    // A smaller count with the same word budget is a DIFFERENT record, not a refusal: 96 entries at 7
    // bits need the same 12 words 100 do, so the codec has no way to tell -- and must not pretend to.
    assertEquals(96, deserialize(withInt(good, 4, 96)).size());
    // A narrower width over the same words likewise reads: 100 entries at 6 bits need 10 + 1 words.
    assertEquals(6, deserialize(withByte(good, 8, (byte) 6)).bitsPerEntry());
  }

  private static byte[] withInt(final byte[] bytes, final int at, final int value) {
    final byte[] copy = bytes.clone();
    // The record codecs are little-endian (LE.INT).
    copy[at] = (byte) value;
    copy[at + 1] = (byte) (value >>> 8);
    copy[at + 2] = (byte) (value >>> 16);
    copy[at + 3] = (byte) (value >>> 24);
    return copy;
  }

  private static byte[] withByte(final byte[] bytes, final int at, final byte value) {
    final byte[] copy = bytes.clone();
    copy[at] = value;
    return copy;
  }
}
