/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
   * an incremental, per-segment dictionary affordable, because the forward index is what copy-on-write
   * retains per append. The header used to refuse it outright, so this pins that it is now legal AND
   * that it is still distinguishable from the two shapes it must not be confused with.
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

  /** A decode-only header survives the wire unchanged: the shape needs no new field to be expressed. */
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
}
