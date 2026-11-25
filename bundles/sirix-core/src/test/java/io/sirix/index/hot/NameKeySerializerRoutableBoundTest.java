/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The routable key bound of FMT-305, as the NAME serializer applies it: a name that fits is stored
 * whole, a longer one is stored under a marked character-aligned prefix. Complete keys never
 * collide with truncated keys, and only marked keys need a candidate re-check.
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
    final byte[] stored = serialize(name);
    assertFalse(NameKeySerializer.isTruncated(stored, 0, stored.length),
        "a name that ends exactly at the bound is not truncated");
    assertEquals(BOUND, stored.length);
    assertArrayEquals(name.getLocalName().getBytes(StandardCharsets.UTF_8), serialize(name));
    assertEquals(name, SERIALIZER.deserialize(serialize(name), 0, BOUND));
  }

  @Test
  void aNameLongerThanTheBoundIsStoredUnderItsPrefix() {
    final QNm name = new QNm("a".repeat(BOUND) + "tail");
    final byte[] stored = serialize(name);
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length));
    assertEquals(BOUND, stored.length);
    assertArrayEquals("a".repeat(BOUND - 1).getBytes(StandardCharsets.UTF_8), Arrays.copyOf(stored, stored.length - 1));
    assertEquals((byte) 0xFF, stored[stored.length - 1]);
    // The stored key is still a readable key — of the shortened name, not of the one asked for.
    assertEquals(new QNm("a".repeat(BOUND - 1)), SERIALIZER.deserialize(stored, 0, stored.length));
  }

  @Test
  void twoNamesPastTheBoundShareOneStoredKeyAndAreFlaggedForRecheck() {
    final String head = "a".repeat(BOUND);
    final QNm first = new QNm(head + "x");
    final QNm second = new QNm(head + "y");
    assertArrayEquals(serialize(first), serialize(second), "the shared prefix is the whole stored key");
    assertNotEquals(first, second);
    final byte[] stored = serialize(first);
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length), "the stored marker agrees");
    final byte[] ordinary = serialize(new QNm("field"));
    assertFalse(NameKeySerializer.isTruncated(ordinary, 0, ordinary.length), "an ordinary name owes no re-check");
  }

  @Test
  void truncationStopsOnACharacterBoundaryAndKeepsNameOrder() {
    // Four-byte characters: the budget runs out mid-character unless truncation is character-wise.
    final String wide = "😀".repeat(BOUND);
    final QNm name = new QNm(wide);
    final byte[] stored = serialize(name);
    assertEquals(0, (stored.length - 1) % 4, "a whole number of four-byte characters");
    assertTrue(stored.length <= BOUND && stored.length >= BOUND - 4, "the longest character prefix that fits");
    assertEquals(new String(stored, 0, stored.length - 1, StandardCharsets.UTF_8),
        SERIALIZER.deserialize(stored, 0, stored.length).getLocalName(),
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
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length));
    final QNm read = SERIALIZER.deserialize(stored, 0, stored.length);
    assertTrue(read.getNamespaceURI().startsWith("u"), "the URI survives as its own prefix");
    assertFalse(read.getLocalName().isEmpty(), "a key with no local name cannot be read back");
  }

  @Test
  void anEscapedNulInTheNamespaceCountsTowardTheBound() {
    final QNm name = new QNm("a\u0000b", "p", "local");
    final byte[] stored = serialize(name);
    assertEquals(12, stored.length, "sentinel, four escaped URI bytes, terminator and five local bytes");
    assertEquals(name.getNamespaceURI(), SERIALIZER.deserialize(stored, 0, stored.length).getNamespaceURI());

    // A NUL in the very first position is escaped like any other, or the URI reads back as a
    // different URI — and the terminator would be the one byte the escape did not spend.
    final QNm leading = new QNm("\u0000ab", "p", "local");
    final byte[] storedLeading = serialize(leading);
    assertEquals(12, storedLeading.length, "the first character's escape is counted too");
    assertEquals(leading.getNamespaceURI(),
        SERIALIZER.deserialize(storedLeading, 0, storedLeading.length).getNamespaceURI());
  }

  @Test
  void anEmptyKeyRegionIsNotATruncatedKey() {
    // Reading the byte before the region would answer from a neighbour's bytes.
    assertFalse(NameKeySerializer.isTruncated(new byte[] {(byte) 0xFF}, 1, 0), "nothing was stored, nothing was cut");
    assertFalse(NameKeySerializer.isTruncated(new byte[0], 0, 0));
  }

  /**
   * The escape a NUL costs decides the bound on its own. This name is one stored byte over it only
   * because of that escape: counted with the wrong sign, or weighed against the namespace budget with
   * the wrong sign, the key is judged to fit and is written past the bound unmarked.
   */
  @Test
  void aNamespaceOneEscapeOverTheBoundIsCut() {
    final QNm name = new QNm("\u0000" + "u".repeat(BOUND - 5), "p", "a");
    final byte[] stored = serialize(name);
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length),
        "three framing bytes, the escaped NUL, the plain characters and one local byte are one over");
    assertEquals(BOUND, stored.length, "a cut key fills the bound exactly");
    final QNm read = SERIALIZER.deserialize(stored, 0, stored.length);
    assertEquals("\u0000" + "u".repeat(BOUND - 7), read.getNamespaceURI(), "the escape is read back as one NUL");
    assertEquals("a", read.getLocalName());
  }

  /**
   * A NUL past the cut is not in the stored key, so it buys no escape. Counting the whole URI's NULs
   * instead of the surviving prefix's shifts the stored bytes apart and writes past the bound.
   */
  @Test
  void aNulPastTheCutCostsNothing() {
    final QNm name = new QNm("u".repeat(BOUND) + "\u0000\u0000", "p", "a");
    final byte[] stored = serialize(name);
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length));
    assertEquals(BOUND, stored.length, "the surviving prefix holds no NUL, so nothing is escaped");
    final QNm read = SERIALIZER.deserialize(stored, 0, stored.length);
    assertEquals("u".repeat(BOUND - 5), read.getNamespaceURI());
    assertEquals("a", read.getLocalName());
  }

  @Test
  void theReportedMaximumNeverExceedsTheBound() {
    assertTrue(SERIALIZER.maxSerializedLength(new QNm("a".repeat(BOUND * 2))) <= BOUND,
        "a caller sizes its buffer from this, so it must not promise more than the bound");
    assertEquals(5, SERIALIZER.maxSerializedLength(new QNm("abcde")) / 3, "short names keep their old estimate");
  }

  @Test
  void completeBoundaryNamesCannotCollideWithMarkedPrefixes() {
    final String head = "a".repeat(BOUND);
    final QNm below = new QNm(head.substring(1));
    final QNm exact = new QNm(head);
    final QNm above = new QNm(head + "x");
    assertFalse(Arrays.equals(serialize(below), serialize(above)));
    assertFalse(Arrays.equals(serialize(exact), serialize(above)));
    for (final QNm name : new QNm[] {below, exact}) {
      final byte[] stored = serialize(name);
      assertEquals(name, SERIALIZER.deserialize(stored, 0, stored.length));
      assertFalse(NameKeySerializer.isTruncated(stored, 0, stored.length));
    }
  }

  @Test
  void namespacesReserveOnlyTheFirstLocalCharactersEncodedWidth() {
    for (final String local : new String[] {"a", "é", "\u0800", "中", "😀"}) {
      final int width = local.getBytes(StandardCharsets.UTF_8).length;
      for (int difference = -1; difference <= 1; difference++) {
        final QNm name = new QNm("u".repeat(BOUND - 3 - width + difference), "p", local);
        final byte[] stored = serialize(name);
        assertEquals(difference > 0, NameKeySerializer.isTruncated(stored, 0, stored.length));
        if (difference <= 0) {
          assertEquals(name, SERIALIZER.deserialize(stored, 0, stored.length));
          assertEquals(BOUND + difference, stored.length);
        } else {
          // The namespace gave up exactly the first local character's own width, no more and no
          // less: one byte fewer and the character could not be written whole, one more and the
          // stored key stops short of the bound.
          assertEquals(BOUND, stored.length, "a cut namespace fills the bound exactly");
          assertEquals(local, SERIALIZER.deserialize(stored, 0, stored.length).getLocalName(),
              "the first character of the local name survives whole, whatever its width");
        }
      }
    }
    final QNm escaped = new QNm("u".repeat(BOUND - 6) + "\u0000", "p", "a");
    assertEquals(BOUND, serialize(escaped).length);
    assertEquals(escaped, SERIALIZER.deserialize(serialize(escaped), 0, BOUND));
    final byte[] storedEscaped = serialize(escaped);
    assertFalse(NameKeySerializer.isTruncated(storedEscaped, 0, storedEscaped.length));
  }

  @Test
  void aSupplementaryNamespaceAtTheCutStillHasATruncationMarker() {
    final QNm name = new QNm("urn:" + "u".repeat(4074) + "😀tail", "p", "a");
    final byte[] stored = serialize(name);
    assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length));
    assertEquals(BOUND, stored.length);
    assertEquals("a", SERIALIZER.deserialize(stored, 0, stored.length).getLocalName());
  }

  @Test
  void completeLongComponentsKeepTheirReferenceEncoding() {
    for (final QNm name : new QNm[] {new QNm("a".repeat(2000)), new QNm("é".repeat(2000)), new QNm("中".repeat(1000)),
        new QNm("😀".repeat(1000)), new QNm("u".repeat(2000), "p", "a"), new QNm("é".repeat(1000), "p", "field"),
        new QNm("u".repeat(1000), "p", "a".repeat(1000)), new QNm("u\u0000".repeat(1000), "p", "field")}) {
      final ByteArrayOutputStream expected = new ByteArrayOutputStream();
      if (!name.getNamespaceURI().isEmpty()) {
        expected.write(0xFF);
        for (final byte value : name.getNamespaceURI().getBytes(StandardCharsets.UTF_8)) {
          expected.write(value);
          if (value == 0) {
            expected.write(0xFF);
          }
        }
        expected.write(0);
        expected.write(0);
      }
      expected.writeBytes(name.getLocalName().getBytes(StandardCharsets.UTF_8));
      final byte[] stored = serialize(name);
      assertArrayEquals(expected.toByteArray(), stored);
      assertFalse(NameKeySerializer.isTruncated(stored, 0, stored.length));
      assertEquals(name, SERIALIZER.deserialize(stored, 0, stored.length));
    }
  }

  /**
   * The stored key of an over-long name is the longest whole-character prefix that fits beside the
   * truncation marker, for characters of every UTF-8 width. The character count is the oracle: it
   * follows from the width of the character and the bound alone, so a width judged one class too
   * narrow or too wide, or a budget compared one byte off, stores a different number of them.
   */
  @Test
  void anOverLongNameStoresTheLongestWholeCharacterPrefixThatFitsForEveryCharacterWidth() {
    for (final String character : new String[] {"a", "é", "\u0800", "中", "😀"}) {
      final int width = character.getBytes(StandardCharsets.UTF_8).length;
      final int fitting = (BOUND - 1) / width;
      final QNm name = new QNm(character.repeat(fitting + 8));
      final byte[] stored = serialize(name);
      assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length),
          "a name of " + width + "-byte characters past the bound is marked");
      assertEquals(fitting * width + 1, stored.length,
          "width " + width + ": " + fitting + " whole characters plus the marker");
      assertEquals(character.repeat(fitting), new String(stored, 0, stored.length - 1, StandardCharsets.UTF_8),
          "the stored prefix decodes as whole " + width + "-byte characters");

      final QNm fits = new QNm(character.repeat(BOUND / width));
      final byte[] whole = serialize(fits);
      assertFalse(NameKeySerializer.isTruncated(whole, 0, whole.length),
          "a name of width " + width + " that fits the bound carries no marker");
      assertEquals(BOUND / width * width, whole.length);
      assertEquals(fits, SERIALIZER.deserialize(whole, 0, whole.length));
    }
  }

  /**
   * A NUL in the namespace costs two stored bytes, so a namespace cut at the bound keeps one
   * character fewer for every NUL inside the part that survives. The surviving URI is what the stored
   * key decodes to, which is the only way to see that the escape was counted on both the budget and
   * the rewrite.
   */
  @Test
  void anEscapedNulInsideACutNamespaceCostsTwoStoredBytes() {
    final String head = "u".repeat(BOUND);
    for (final int nuls : new int[] {1, 2, 3}) {
      final String uri = "\u0000".repeat(nuls) + head;
      // A local name long enough to spend whatever budget the namespace left: the escaped NULs must
      // not be handed back to it, or the stored key runs past the bound.
      final QNm name = new QNm(uri, "p", "abcdefgh");
      final byte[] stored = serialize(name);
      assertTrue(NameKeySerializer.isTruncated(stored, 0, stored.length), "the namespace is cut");
      final QNm read = SERIALIZER.deserialize(stored, 0, stored.length);
      final String survivingUri = read.getNamespaceURI();
      assertEquals(nuls, survivingUri.chars().filter(c -> c == 0).count(),
          "every NUL of the surviving prefix is read back exactly once");
      // Framing, the escaped NULs, the plain characters, one local-name byte and the marker.
      assertEquals(BOUND, 3 + survivingUri.length() + nuls + 1 + 1,
          "each NUL spends one stored byte more than the character it is");
      assertEquals("a", read.getLocalName(), "one local byte is all the cut namespace left");
      assertEquals(BOUND, stored.length, "a cut key fills the bound exactly, never more");
    }
  }

  private static byte[] serialize(final QNm name) {
    final byte[] buffer = new byte[SERIALIZER.maxSerializedLength(name) + 8];
    final int length = SERIALIZER.serialize(name, buffer, 0);
    return Arrays.copyOf(buffer, length);
  }
}
