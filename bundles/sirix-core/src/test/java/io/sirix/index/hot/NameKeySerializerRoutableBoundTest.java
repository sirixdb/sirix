/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The routable key bound of FMT-305, as the NAME serializer applies it: a name that fits is stored
 * whole, a longer one is stored under the longest prefix of its components that fits, truncated on a
 * character boundary so the key stays readable and key order still follows name order.
 */
final class NameKeySerializerRoutableBoundTest {

  private static final int BOUND = NameKeySerializer.MAX_NAME_KEY_BYTES;

  private static final NameKeySerializer SERIALIZER = NameKeySerializer.INSTANCE;

  @Test
  void theBoundIsTheRoutableKeyBoundLessWhatAStoredKeyCarriesAfterIt() {
    assertEquals(HOTKeySerializer.ROUTABLE_KEY_BYTES - HOTKeySerializer.CHUNK_IDX_BYTES - PostingDeltas.SUFFIX_BYTES,
        BOUND, "the name's share of the routable bound");
    assertEquals(4096, HOTKeySerializer.ROUTABLE_KEY_BYTES, "the bound FMT-305 states");
  }

  @Test
  void aNameInsideTheBoundIsStoredWhole() {
    final QNm name = new QNm("a".repeat(BOUND));
    assertFalse(NameKeySerializer.isTruncated(name), "a name that ends exactly at the bound is not truncated");
    assertEquals(BOUND, NameKeySerializer.serializedLength(name));
    assertArrayEquals(name.getLocalName().getBytes(StandardCharsets.UTF_8), serialize(name));
    assertEquals(name, SERIALIZER.deserialize(serialize(name), 0, BOUND));
  }

  @Test
  void aNameLongerThanTheBoundIsStoredUnderItsPrefix() {
    final QNm name = new QNm("a".repeat(BOUND) + "tail");
    assertTrue(NameKeySerializer.isTruncated(name));
    assertEquals(BOUND, NameKeySerializer.serializedLength(name));
    final byte[] stored = serialize(name);
    assertEquals(BOUND, stored.length);
    assertArrayEquals("a".repeat(BOUND).getBytes(StandardCharsets.UTF_8), stored);
    // The stored key is still a readable key — of the shortened name, not of the one asked for.
    assertEquals(new QNm("a".repeat(BOUND)), SERIALIZER.deserialize(stored, 0, stored.length));
  }

  @Test
  void twoNamesPastTheBoundShareOneStoredKeyAndAreFlaggedForRecheck() {
    final String head = "a".repeat(BOUND);
    final QNm first = new QNm(head + "x");
    final QNm second = new QNm(head + "y");
    assertArrayEquals(serialize(first), serialize(second), "the shared prefix is the whole stored key");
    assertNotEquals(first, second);
    assertTrue(NameKeySerializer.mayBeTruncated(first),
        "a read must re-check a key this long against each node's own name");
    assertTrue(NameKeySerializer.mayBeTruncated(NameKeySerializer.serializedLength(first)),
        "the length-only form agrees");
    assertFalse(NameKeySerializer.mayBeTruncated(new QNm("field")), "an ordinary name owes no re-check");
  }

  @Test
  void truncationStopsOnACharacterBoundaryAndKeepsNameOrder() {
    // Four-byte characters: the budget runs out mid-character unless truncation is character-wise.
    final String wide = "😀".repeat(BOUND);
    final QNm name = new QNm(wide);
    final byte[] stored = serialize(name);
    assertEquals(0, stored.length % 4, "a whole number of four-byte characters");
    assertTrue(stored.length <= BOUND && stored.length > BOUND - 4, "the longest character prefix that fits");
    assertEquals(new String(stored, StandardCharsets.UTF_8), SERIALIZER.deserialize(stored, 0, stored.length)
                                                                       .getLocalName(),
        "the truncated key decodes as whole characters");

    final QNm lower = new QNm("a".repeat(BOUND / 2) + "😀".repeat(BOUND));
    final QNm higher = new QNm("b".repeat(BOUND / 2) + "😀".repeat(BOUND));
    assertTrue(Arrays.compareUnsigned(serialize(lower), serialize(higher)) < 0,
        "key order must follow name order across the bound");
  }

  @Test
  void aNamespaceLongerThanTheBoundKeepsItsFramingAndOneLocalNameByte() {
    final QNm name = new QNm("u".repeat(BOUND), "p", "local");
    final byte[] stored = serialize(name);
    assertTrue(stored.length <= BOUND, "the stored key stays inside the bound");
    assertTrue(NameKeySerializer.isTruncated(name));
    final QNm read = SERIALIZER.deserialize(stored, 0, stored.length);
    assertTrue(read.getNamespaceURI().startsWith("u"), "the URI survives as its own prefix");
    assertFalse(read.getLocalName().isEmpty(), "a key with no local name cannot be read back");
  }

  @Test
  void anEscapedNulInTheNamespaceCountsTowardTheBound() {
    final QNm name = new QNm("a\u0000b", "p", "local");
    final byte[] stored = serialize(name);
    assertEquals(NameKeySerializer.serializedLength(name), stored.length, "the reported length is the written one");
    assertEquals(name.getNamespaceURI(), SERIALIZER.deserialize(stored, 0, stored.length).getNamespaceURI());
  }

  @Test
  void theReportedMaximumNeverExceedsTheBound() {
    assertTrue(SERIALIZER.maxSerializedLength(new QNm("a".repeat(BOUND * 2))) <= BOUND,
        "a caller sizes its buffer from this, so it must not promise more than the bound");
    assertEquals(5, SERIALIZER.maxSerializedLength(new QNm("abcde")) / 3, "short names keep their old estimate");
  }

  private static byte[] serialize(final QNm name) {
    final byte[] buffer = new byte[SERIALIZER.maxSerializedLength(name) + 8];
    final int length = SERIALIZER.serialize(name, buffer, 0);
    return Arrays.copyOf(buffer, length);
  }
}
