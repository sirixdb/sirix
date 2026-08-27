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
}
