package io.sirix.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The packed reverse sub-block record: wire round trip, bounds, and the packing rule that makes
 * sub-blocks necessary in the first place.
 */
final class ValueDictionaryValueBlockNodeTest {

  private static ValueDictionaryValueBlockNode pack(final long key, final int firstId, final String... values) {
    final int[] offsets = new int[values.length + 1];
    int total = 0;
    for (int i = 0; i < values.length; i++) {
      total += values[i].getBytes(StandardCharsets.UTF_8).length;
      offsets[i + 1] = total;
    }
    final byte[] bytes = new byte[total];
    int at = 0;
    for (final String value : values) {
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(utf8, 0, bytes, at, utf8.length);
      at += utf8.length;
    }
    return new ValueDictionaryValueBlockNode(key, firstId, offsets, bytes);
  }

  private static ValueDictionaryValueBlockNode roundTrip(final ValueDictionaryValueBlockNode block) {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    NodeKind.VALUE_DICTIONARY_VALUE_BLOCK.serialize(sink, block, null);
    return (ValueDictionaryValueBlockNode) NodeKind.VALUE_DICTIONARY_VALUE_BLOCK.deserialize(
        new ByteArrayBytesIn(sink.toByteArray()), block.getNodeKey(), null, null);
  }

  private static String valueAt(final ValueDictionaryValueBlockNode block, final int id) {
    return new String(block.rawBytes(), block.valueOffset(id), block.valueLength(id), StandardCharsets.UTF_8);
  }

  @Test
  @DisplayName("a block round-trips through the codec byte-exactly")
  void roundTrips() {
    final ValueDictionaryValueBlockNode block = pack(7L, 513, "alpha", "", "𐐀", "！", "zzz");
    final ValueDictionaryValueBlockNode decoded = roundTrip(block);
    assertEquals(block, decoded, "the decoded block must equal the encoded one");
    assertEquals(513, decoded.getFirstId());
    assertEquals(5, decoded.size());
    assertEquals("alpha", valueAt(decoded, 513));
    assertEquals("", valueAt(decoded, 514), "an empty value must survive as a zero-length slice");
    assertEquals("𐐀", valueAt(decoded, 515));
    assertEquals("！", valueAt(decoded, 516));
    assertEquals("zzz", valueAt(decoded, 517));
  }

  @Test
  @DisplayName("the public constructor copies; takeOwnership adopts and never re-copies")
  void ownershipTransfersAndRawViewIsNotCopied() {
    final int[] offsets = {0, 1, 2};
    final byte[] bytes = {'a', 'b'};
    // The PUBLIC constructor copies, so ownership transfer is never accidental.
    final ValueDictionaryValueBlockNode copied = new ValueDictionaryValueBlockNode(3L, 1, offsets, bytes);
    assertNotSame(bytes, copied.rawBytes(), "the public constructor must not adopt the caller's array");
    assertEquals(1, copied.offsetAt(1), "offsets are readable without materialising the table");

    // takeOwnership is the explicit ingestion path: adopted, never re-copied per access.
    final byte[] owned = {'a', 'b'};
    final ValueDictionaryValueBlockNode adopted =
        ValueDictionaryValueBlockNode.takeOwnership(4L, 1, new int[] {0, 1, 2}, owned);
    assertSame(owned, adopted.rawBytes(), "takeOwnership adopts");
    assertSame(adopted.rawBytes(), adopted.rawBytes(), "and never re-copies per access");

    bytes[0] = 'z';
    assertEquals("ab", new String(copied.rawBytes(), StandardCharsets.UTF_8),
        "a copying-constructed block must be insulated from later caller mutation");
  }

  @Test
  @DisplayName("a firstId near the id ceiling is refused by the codec before allocating")
  void idSpaceOverrunIsRefused() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    sink.writeInt(Integer.MAX_VALUE); // firstId
    sink.writeInt(4); // count -> last id wraps
    sink.writeInt(0);
    final ByteArrayBytesIn source = new ByteArrayBytesIn(sink.toByteArray());
    assertThrows(IllegalStateException.class,
        () -> NodeKind.VALUE_DICTIONARY_VALUE_BLOCK.deserialize(source, 1L, null, null));
  }

  @Test
  @DisplayName("coverage and id mapping are exact at both ends")
  void coverageIsExact() {
    final ValueDictionaryValueBlockNode block = pack(3L, 100, "a", "bb", "ccc");
    assertFalse(block.covers(99));
    assertTrue(block.covers(100));
    assertTrue(block.covers(102));
    assertFalse(block.covers(103));
    assertEquals(0, block.valueOffset(100));
    assertEquals(1, block.valueLength(100));
    assertEquals(3, block.valueOffset(102));
    assertEquals(3, block.valueLength(102));
    assertThrows(IllegalArgumentException.class, () -> block.valueOffset(99));
    assertThrows(IllegalArgumentException.class, () -> block.valueOffset(103));
  }

  @Test
  @DisplayName("malformed shapes are refused rather than stored")
  void malformedShapesAreRefused() {
    final byte[] bytes = {1, 2, 3};
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(0L, 1, new int[] {0, 3}, bytes));
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(1L, 0, new int[] {0, 3}, bytes));
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(1L, 1, new int[] {0, 2}, bytes), "offsets must span the bytes");
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(1L, 1, new int[] {0, 3, 2}, new byte[2]), "offsets must ascend");
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(1L, 1, new int[] {0}, new byte[0]), "a block holds at least one value");
    assertThrows(IllegalArgumentException.class,
        () -> new ValueDictionaryValueBlockNode(1L, 1, new int[] {0, ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES + 1},
            new byte[ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES + 1]),
        "a block is byte-bounded");
  }

  @Test
  @DisplayName("a one-value block whose id IS Integer.MAX_VALUE is valid, and the codec agrees")
  void maximumIdBoundaryIsValid() {
    // LAST id = firstId + count - 1 = MAX_VALUE. Comparing the END-EXCLUSIVE id against MAX_VALUE
    // rejected this block while NodeKind's decode accepted it — the two disagreed about a block the
    // codec would happily produce.
    final ValueDictionaryValueBlockNode block =
        ValueDictionaryValueBlockNode.takeOwnership(11L, Integer.MAX_VALUE, new int[] {0, 1}, new byte[] {'z'});
    assertTrue(block.covers(Integer.MAX_VALUE));
    assertFalse(block.covers(Integer.MAX_VALUE - 1));
    assertEquals(0, block.valueOffset(Integer.MAX_VALUE));
    assertEquals(1, block.valueLength(Integer.MAX_VALUE));
    assertEquals(block, roundTrip(block), "the codec must accept what the record accepts");
    // An extreme negative id must not wrap into a valid index.
    assertThrows(IllegalArgumentException.class, () -> block.valueOffset(Integer.MIN_VALUE));
  }

  @Test
  @DisplayName("ordinary values never spill: a bucket's values fill CONSECUTIVE sub-blocks")
  void ordinaryValuesFillConsecutiveSubBlocksRatherThanSpilling() {
    // THE design rule. 256 values of 1 KiB is 256 KiB — four times a single block's target. With one
    // capped blob per bucket, everything past the first ~64 would spill to its own record and the
    // per-row decode would be back. Packing into consecutive sub-blocks spills nothing.
    final int perValue = 1024;
    final String value = "x".repeat(perValue);
    final List<ValueDictionaryValueBlockNode> blocks = new ArrayList<>();
    final List<String> pending = new ArrayList<>();
    int firstId = 1;
    int pendingBytes = 0;
    for (int i = 0; i < ValueDictionaryValueBucketNode.VALUES_PER_BUCKET; i++) {
      if (pendingBytes + perValue > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES && !pending.isEmpty()) {
        blocks.add(pack(blocks.size() + 1L, firstId, pending.toArray(new String[0])));
        firstId += pending.size();
        pending.clear();
        pendingBytes = 0;
      }
      pending.add(value);
      pendingBytes += perValue;
    }
    blocks.add(pack(blocks.size() + 1L, firstId, pending.toArray(new String[0])));

    assertEquals(4, blocks.size(), "256 KiB of values must occupy four 64 KiB sub-blocks");
    int covered = 0;
    for (final ValueDictionaryValueBlockNode block : blocks) {
      assertTrue(block.rawBytes().length <= ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES, "each block stays bounded");
      covered += block.size();
    }
    assertEquals(ValueDictionaryValueBucketNode.VALUES_PER_BUCKET, covered,
        "every id in the bucket is packed; none spilled");
    // Consecutive and gapless, which is what makes id -> sub-block a directory lookup.
    int expected = 1;
    for (final ValueDictionaryValueBlockNode block : blocks) {
      assertEquals(expected, block.getFirstId());
      expected += block.size();
    }
  }

  @Test
  @DisplayName("only an individually oversized value cannot be packed")
  void onlyAnIndividuallyOversizedValueCannotBePacked() {
    final String oversized = "y".repeat(ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES + 1);
    assertThrows(IllegalArgumentException.class, () -> pack(1L, 1, oversized),
        "a value longer than the block target is the ONLY case that must take the spill lane");
    // One byte under the target still packs, so the boundary is the value's own size and nothing else.
    final String largest = "y".repeat(ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES);
    assertEquals(largest, valueAt(pack(1L, 1, largest), 1));
  }

  @Test
  @DisplayName("a truncated record is refused before it can size an allocation")
  void truncatedRecordIsRefused() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    sink.writeInt(1); // firstId
    sink.writeInt(4); // count
    sink.writeInt(1 << 20); // byteLength far beyond what follows
    final ByteArrayBytesIn source = new ByteArrayBytesIn(sink.toByteArray());
    assertThrows(IllegalStateException.class,
        () -> NodeKind.VALUE_DICTIONARY_VALUE_BLOCK.deserialize(source, 1L, null, null));
  }

  @Test
  @DisplayName("offsets survive a block whose values are all empty")
  void allEmptyValuesRoundTrip() {
    final ValueDictionaryValueBlockNode decoded = roundTrip(pack(9L, 1, "", "", ""));
    assertEquals(3, decoded.size());
    assertArrayEquals(new int[] {0, 0, 0, 0}, decoded.copyOffsets());
    assertEquals(0, decoded.rawBytes().length);
  }
}
