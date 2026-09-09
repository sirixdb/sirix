/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the value-dictionary header's forward-compatibility contract: a resource written by a newer
 * build must make this one DECLINE, never misparse. The decline used to be unreachable — the
 * constructor rejected every non-current version with an {@link IllegalArgumentException} straight
 * out of the page-read path, so {@code GlobalValueDictionary#header}'s documented {@code null}
 * branch could never be taken.
 */
final class ValueDictionaryHeaderLayoutTest {

  @Test
  @DisplayName("An unknown layout version deserializes to a declining carrier, never misparses")
  void unknownVersionDeclines() {
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      bytes.writeInt(ValueDictionaryHeaderNode.VERSION + 1);
      // Arbitrary trailing payload this build must NOT interpret.
      bytes.writeLong(0xDEADBEEFL);
      final ValueDictionaryHeaderNode header = (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(
          Bytes.wrapForRead(bytes.toByteArray()), 42L, null, null);
      assertFalse(header.isCurrentLayout());
      assertEquals(ValueDictionaryHeaderNode.VERSION + 1, header.getVersion());
      assertEquals(0, header.getEntryCount());
    }
  }

  @Test
  @DisplayName("A declining carrier refuses re-serialization — no lossy reconstruction")
  void carrierRefusesSerialization() {
    final ValueDictionaryHeaderNode carrier =
        ValueDictionaryHeaderNode.unknownLayout(42L, ValueDictionaryHeaderNode.VERSION + 1);
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes, carrier, null));
      assertTrue(failure.getMessage().contains("unknown layout version"), failure.getMessage());
    }
  }

  @Test
  @DisplayName("The current layout round-trips unchanged")
  void currentLayoutRoundTrips() {
    final ValueDictionaryHeaderNode header =
        new ValueDictionaryHeaderNode(42L, ValueDictionaryHeaderNode.VERSION, 3, 7L, 9L, 1);
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes, header, null);
      final ValueDictionaryHeaderNode read = (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(
          Bytes.wrapForRead(bytes.toByteArray()), 42L, null, null);
      assertTrue(read.isCurrentLayout());
      assertEquals(ValueDictionaryHeaderNode.VERSION, read.getVersion());
      assertEquals(3, read.getEntryCount());
      assertEquals(7L, read.getForwardRootKey());
      assertEquals(9L, read.getReverseRootKey());
      assertEquals(1, read.getGeneration());
    }
  }

  @Test
  @DisplayName("unknownLayout refuses the current version and corruption-shaped input")
  void unknownLayoutValidation() {
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryHeaderNode.unknownLayout(42L, ValueDictionaryHeaderNode.VERSION));
    assertThrows(IllegalArgumentException.class, () -> ValueDictionaryHeaderNode.unknownLayout(42L, -1));
    assertThrows(IllegalArgumentException.class,
        () -> ValueDictionaryHeaderNode.unknownLayout(0L, ValueDictionaryHeaderNode.VERSION + 1));
  }

  /**
   * A decode-only dictionary — no forward index, ids NOT in collation order — is the shape that makes
   * an incremental, per-segment dictionary affordable, because the forward index is what
   * copy-on-write retains per append. The header used to refuse it outright, so this pins that it is
   * now legal AND that it is still distinguishable from the two shapes it must not be confused with.
   */
  @Test
  @DisplayName("A decode-only header is legal, readable, and refuses the encode direction")
  void decodeOnlyHeaderIsLegalButNotProbeable() {
    final ValueDictionaryHeaderNode decodeOnly =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 0, 0);

    assertTrue(decodeOnly.isDecodeOnly(), "no forward root and no ordering claim IS the decode-only shape");
    assertTrue(decodeOnly.isDirectoryComplete(), "the reverse index answers id -> value, which is what serving needs");
    assertFalse(decodeOnly.supportsValueProbe(), "value -> id cannot be answered without a forward index");
    assertFalse(decodeOnly.isFullyOrdered(), "decode-only makes no ordering claim; that is why it is cheap");
  }

  /**
   * The distinction the two predicates exist for. A rank-ordered dictionary also has no forward root,
   * but it CAN be probed — by binary search over a reverse index sorted by value — so a caller that
   * tested only "is the forward root zero" would refuse it wrongly.
   */
  @Test
  @DisplayName("A fully ordered header has no forward index yet still answers the encode direction")
  void fullyOrderedHeaderIsProbeableWithoutAForwardIndex() {
    final ValueDictionaryHeaderNode ranked =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 0, 512);

    assertTrue(ranked.isFullyOrdered());
    assertTrue(ranked.supportsValueProbe(), "binary search over the sorted reverse index serves as the probe");
    assertFalse(ranked.isDecodeOnly(), "a fully ordered dictionary is not decode-only");
    assertTrue(ranked.isDirectoryComplete());
  }

  /**
   * The invariant that must NOT have been relaxed along with the forward root: the reverse index is
   * the positive witness that a header describes a readable dictionary. Losing it is corruption, and
   * corruption must still be refused rather than reported as "decode-only".
   */
  @Test
  @DisplayName("A missing reverse index is still refused, decode-only or not")
  void missingReverseIndexIsStillCorruption() {
    final IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 0L, 0, 0));
    assertTrue(thrown.getMessage().contains("invalid value dictionary header"), thrown.getMessage());

    final ValueDictionaryHeaderNode empty =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 0, 0L, 0L, 0, 0);
    assertFalse(empty.isDecodeOnly(), "an EMPTY dictionary is not decode-only, it is empty");
    assertTrue(empty.supportsValueProbe(), "and it can be probed vacuously, so no caller need special-case it");
  }

  /**
   * A decode-only header survives the wire unchanged: the shape needs no new field to be expressed.
   */
  @Test
  @DisplayName("A decode-only header round-trips through the record serializer")
  void decodeOnlyHeaderRoundTrips() {
    final ValueDictionaryHeaderNode header =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 3, 0);
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes, header, null);
      final ValueDictionaryHeaderNode read = (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(
          Bytes.wrapForRead(bytes.toByteArray()), 7L, null, null);
      assertTrue(read.isDecodeOnly());
      assertFalse(read.supportsValueProbe());
      assertTrue(read.isDirectoryComplete());
      assertEquals(512, read.getEntryCount());
      assertEquals(0L, read.getForwardRootKey());
      assertEquals(99L, read.getReverseRootKey());
      assertEquals(3, read.getGeneration());
    }
  }

  /**
   * The shape a sealed segment dictionary writes: storage in collation order (so the binary-search
   * probe and the separator array are legal) but ids that are arrival-order MINTS mapped through a
   * rank table. Every arm that compares ids AS values must ask
   * {@link ValueDictionaryHeaderNode#idsAreCollationOrdered()}, which is the one predicate the table
   * turns off; everything {@link ValueDictionaryHeaderNode#isFullyOrdered()} licenses about the
   * STORAGE stays true.
   */
  @Test
  @DisplayName("A rank table keeps the storage ordered but makes the ids NOT collation-ordered")
  void rankTableSeparatesStorageOrderFromIdOrder() {
    final ValueDictionaryHeaderNode tabled =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 512, 300L, 900L);
    assertTrue(tabled.hasRankTable());
    assertEquals(900L, tabled.getRankTableKey());
    assertTrue(tabled.isFullyOrdered(), "the storage IS in collation order");
    assertFalse(tabled.idsAreCollationOrdered(), "but the ids are mints, so comparing them compares arrival order");
    assertTrue(tabled.supportsValueProbe(), "binary search over the ordered storage still answers value -> position");
    assertFalse(tabled.isDecodeOnly());
    assertTrue(tabled.isDirectoryComplete());
    assertEquals(300L, tabled.getBlockIndexKey());

    final ValueDictionaryHeaderNode untabled =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 512, 300L, 0L);
    assertFalse(untabled.hasRankTable());
    assertTrue(untabled.idsAreCollationOrdered(), "without a table, ids ARE storage positions");
    assertNotEquals(tabled, untabled, "the table key is part of the header's identity");
    assertNotEquals(tabled.hashCode(), untabled.hashCode());
    assertTrue(tabled.toString().contains("900"), tabled.toString());
  }

  /**
   * A table translates mints in {@code 1..orderedPrefixCount}; with no prefix there is nothing to
   * translate, and a forward index would answer a probe with a storage POSITION that the pages do not
   * carry. Both are refused at construction, so no reader has to guard against them.
   */
  @Test
  @DisplayName("A rank table needs an ordered prefix and excludes a forward index")
  void rankTableInvariants() {
    final IllegalArgumentException noPrefix = assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 0, 0L, 900L));
    assertTrue(noPrefix.getMessage().contains("ordered prefix"), noPrefix.getMessage());
    final IllegalArgumentException withForward = assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 5L, 99L, 1, 512, 0L, 900L));
    assertTrue(withForward.getMessage().contains("forward index"), withForward.getMessage());
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 512, 0L, -1L),
        "a negative key");
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 513, 0L, 900L),
        "a prefix beyond the entry count");
    // An ordered prefix with an unordered tail may carry a table (the prefix's mints translate, the
    // tail's are their own positions); it is legal, not fully ordered, and not probeable as a whole.
    final ValueDictionaryHeaderNode tailed =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 600, 0L, 99L, 2, 512, 0L, 900L);
    assertTrue(tailed.hasRankTable());
    assertFalse(tailed.isFullyOrdered());
    assertFalse(tailed.idsAreCollationOrdered());
    assertFalse(tailed.supportsValueProbe(), "the tail has neither order nor a forward index");
  }

  @Test
  @DisplayName("The rank table key round-trips as the third trailer field")
  void rankTableKeyRoundTrips() {
    final ValueDictionaryHeaderNode header =
        new ValueDictionaryHeaderNode(7L, ValueDictionaryHeaderNode.VERSION, 512, 0L, 99L, 1, 512, 300L, 900L);
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes, header, null);
      final byte[] wire = bytes.toByteArray();
      assertEquals(HEADER_BYTES + TRIPLE_TRAILER_BYTES, wire.length, "the triple is written when anything is set");
      final ValueDictionaryHeaderNode read =
          (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(Bytes.wrapForRead(wire), 7L, null,
              null);
      assertEquals(header, read);
      assertEquals(900L, read.getRankTableKey());
      assertEquals(300L, read.getBlockIndexKey());
      assertEquals(512, read.getOrderedPrefixCount());
      assertTrue(read.hasRankTable());
      assertFalse(read.idsAreCollationOrdered());
    }
  }

  /** {@code version, entryCount, forwardRootKey, reverseRootKey, generation}: 4 + 4 + 8 + 8 + 4. */
  private static final int HEADER_BYTES = 28;
  /** {@code orderedPrefixCount, blockIndexKey}: 4 + 8 — pinned here, and the codec must agree. */
  private static final int PAIR_TRAILER_BYTES = 12;
  /** The pair plus {@code rankTableKey}. */
  private static final int TRIPLE_TRAILER_BYTES = 20;

  @Test
  @DisplayName("The trailer lengths the codec accepts are the wire shape this test pins")
  void trailerConstantsMatchTheWire() {
    assertEquals(PAIR_TRAILER_BYTES, ValueDictionaryHeaderNode.PAIR_TRAILER_BYTES);
    assertEquals(TRIPLE_TRAILER_BYTES, ValueDictionaryHeaderNode.TRIPLE_TRAILER_BYTES);
  }

  /**
   * A trailer of any other length is corruption, not a shorter or longer header: a lenient reader
   * that took "12 or more" as the pair and "20 or more" as the triple would read a rank table key out
   * of whatever followed. One byte too few, one too many, and a bare 4-byte fragment must all refuse,
   * and the message must name the length it saw and the three it accepts.
   */
  @Test
  @DisplayName("A trailer of 4, 11, 13, 19, 21 or 24 bytes is refused, naming the legal lengths")
  void illegalTrailerLengthsAreRefused() {
    for (final int trailer : new int[] {1, 4, 8, 11, 13, 16, 19, 21, 24, 28}) {
      try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
        bytes.writeInt(ValueDictionaryHeaderNode.VERSION);
        bytes.writeInt(512);
        bytes.writeLong(0L);
        bytes.writeLong(99L);
        bytes.writeInt(3);
        for (int i = 0; i < trailer; i++) {
          bytes.writeByte((byte) 1);
        }
        final byte[] wire = bytes.toByteArray();
        assertEquals(HEADER_BYTES + trailer, wire.length);
        final IllegalStateException refused = assertThrows(IllegalStateException.class,
            () -> NodeKind.VALUE_DICTIONARY_HEADER.deserialize(Bytes.wrapForRead(wire), 7L, null, null),
            () -> "a trailer of " + trailer + " bytes");
        assertTrue(refused.getMessage().contains("trailer of " + trailer + " bytes"), refused.getMessage());
        assertTrue(refused.getMessage().contains("0, 12 and 20"), refused.getMessage());
      }
    }
  }

  /**
   * Every header written before the rank table existed carries the PAIR — including the 100M artefact
   * whose rebuild is disk-blocked — and must keep reading as a dictionary WITHOUT a table. The bytes
   * are written by hand so this pins the wire shape, not merely the current serializer's agreement
   * with itself.
   */
  @Test
  @DisplayName("A pair-only trailer (every pre-rank-table header) reads with no rank table")
  void pairOnlyTrailerReadsWithoutARankTable() {
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      bytes.writeInt(ValueDictionaryHeaderNode.VERSION);
      bytes.writeInt(512);
      bytes.writeLong(0L);
      bytes.writeLong(99L);
      bytes.writeInt(3);
      bytes.writeInt(512);
      bytes.writeLong(77L);
      final byte[] wire = bytes.toByteArray();
      assertEquals(HEADER_BYTES + PAIR_TRAILER_BYTES, wire.length);
      final ValueDictionaryHeaderNode read =
          (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(Bytes.wrapForRead(wire), 7L, null,
              null);
      assertEquals(512, read.getEntryCount());
      assertEquals(3, read.getGeneration());
      assertEquals(512, read.getOrderedPrefixCount());
      assertEquals(77L, read.getBlockIndexKey());
      assertEquals(0L, read.getRankTableKey());
      assertFalse(read.hasRankTable());
      assertTrue(read.isFullyOrdered());
      assertTrue(read.idsAreCollationOrdered(), "a rank-pass dictionary's ids ARE its positions");
      assertTrue(read.supportsValueProbe());
    }
  }

  /** A header written before any trailer existed: exactly 28 bytes, every trailer field a zero. */
  @Test
  @DisplayName("A trailer-less header reads every trailer field as zero")
  void trailerLessHeaderReadsZeros() {
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      bytes.writeInt(ValueDictionaryHeaderNode.VERSION);
      bytes.writeInt(512);
      bytes.writeLong(7L);
      bytes.writeLong(9L);
      bytes.writeInt(1);
      final byte[] wire = bytes.toByteArray();
      assertEquals(HEADER_BYTES, wire.length);
      final ValueDictionaryHeaderNode read =
          (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(Bytes.wrapForRead(wire), 7L, null,
              null);
      assertEquals(0, read.getOrderedPrefixCount());
      assertEquals(0L, read.getBlockIndexKey());
      assertEquals(0L, read.getRankTableKey());
      assertFalse(read.hasRankTable());
      assertFalse(read.isFullyOrdered(), "512 entries, none of them in a proven order");
      assertTrue(read.supportsValueProbe(), "through its forward index");
    }
  }

  /**
   * A header with nothing to say writes NO trailer, so a streaming dictionary's header is
   * byte-for-byte what it was before the rank pass or the segment lane existed; one with anything to
   * say writes the whole triple, never a bare pair.
   */
  @Test
  @DisplayName("An all-zero trailer is omitted; any set field writes the whole triple")
  void trailerIsAllOrNothing() {
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes,
          new ValueDictionaryHeaderNode(42L, ValueDictionaryHeaderNode.VERSION, 3, 7L, 9L, 1), null);
      assertEquals(HEADER_BYTES, bytes.toByteArray().length);
    }
    try (final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer()) {
      NodeKind.VALUE_DICTIONARY_HEADER.serialize(bytes,
          new ValueDictionaryHeaderNode(42L, ValueDictionaryHeaderNode.VERSION, 3, 0L, 9L, 1, 3), null);
      final byte[] wire = bytes.toByteArray();
      assertEquals(HEADER_BYTES + TRIPLE_TRAILER_BYTES, wire.length, "an ordered prefix alone writes the triple");
      final ValueDictionaryHeaderNode read =
          (ValueDictionaryHeaderNode) NodeKind.VALUE_DICTIONARY_HEADER.deserialize(Bytes.wrapForRead(wire), 42L, null,
              null);
      assertEquals(3, read.getOrderedPrefixCount());
      assertEquals(0L, read.getBlockIndexKey());
      assertEquals(0L, read.getRankTableKey());
    }
  }
}
