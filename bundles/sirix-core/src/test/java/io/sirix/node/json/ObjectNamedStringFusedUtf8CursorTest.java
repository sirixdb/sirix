/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node.json;

import io.sirix.access.trx.node.json.FusedStringCursor;
import io.sirix.page.NodeFieldLayout;
import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct bound-page coverage for the allocation-free fused semantic-string cursor. */
final class ObjectNamedStringFusedUtf8CursorTest {

  @Test
  void copiesRawUtf8WithoutMaterializingTheNodeValue() {
    final byte[] expected = "Grüße 🦄".getBytes(StandardCharsets.UTF_8);
    final ObjectNamedStringNode node = boundNode(expected, false, null);
    final byte[] tooSmall = {(byte) 0x5A};

    final int firstResult = node.readFusedStringUtf8(tooSmall);
    assertTrue(firstResult < FusedStringCursor.UNAVAILABLE);
    assertEquals(expected.length, FusedStringCursor.requiredCapacity(firstResult));
    assertEquals((byte) 0x5A, tooSmall[0], "a capacity probe must leave caller storage untouched");

    final byte[] destination = new byte[expected.length];
    assertEquals(expected.length, node.readFusedStringUtf8(destination));
    assertArrayEquals(expected, destination);
    assertArrayEquals(expected, node.getRawValue(), "the internal copy must not alter public value semantics");
  }

  @Test
  void decodesHeaderedFsstStraightFromTheBoundPage() {
    // One two-byte symbol (code zero = "ab") and four codes in the encoded payload.
    final byte[] table = {1, 2, 'a', 'b'};
    final byte[] encoded = {FSSTCompressor.HEADER_COMPRESSED, 0, 0, 0, 0};
    final byte[] expected = "abababab".getBytes(StandardCharsets.UTF_8);
    final ObjectNamedStringNode node = boundNode(encoded, true, table);

    final int capacityResult = node.readFusedStringUtf8(new byte[0]);
    assertTrue(capacityResult < FusedStringCursor.UNAVAILABLE);
    final byte[] destination = new byte[FusedStringCursor.requiredCapacity(capacityResult)];
    assertEquals(expected.length, node.readFusedStringUtf8(destination));
    assertArrayEquals(expected, java.util.Arrays.copyOf(destination, expected.length));
    assertArrayEquals(expected, node.getRawValue(), "public FSST decoding must remain byte-identical");
  }

  private static ObjectNamedStringNode boundNode(final byte[] storedValue, final boolean compressed,
      final byte[] symbolTable) {
    final MemorySegment page = MemorySegment.ofArray(new byte[512]);
    ObjectNamedStringNode.writeNewRecord(page, 0,
        new int[NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT],
        7L, 1L, -1L, -1L, 11, 13L, -1, 0, 0L, storedValue, compressed);
    final ObjectNamedStringNode node = new ObjectNamedStringNode(7L, LongHashFunction.xx3());
    node.bind(page, 0, 7L, 0);
    node.setFsstSymbolTable(symbolTable);
    return node;
  }
}
