/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Random;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedDataBlockTest {
  @Test
  void columnCopiesPreserveCanonicalBytesAtEveryUnsignedPayloadWidth() {
    for (int width = 0; width <= Long.BYTES; width++) {
      for (final long base : new long[] {0, Long.MIN_VALUE, -1024L}) {
        final long span = width == 0
            ? 0
            : 1L << (width * Byte.SIZE - 1);
        final byte[][] keys = new byte[64][];
        final byte[][] payloads = new byte[keys.length][];
        for (int row = 0; row < keys.length; row++) {
          keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("columns", row * 2L, row + 1));
          payloads[row] = new byte[4 * Long.BYTES];
          for (int word = 0; word < 4; word++) {
            ProjectionIndexRowGroupCodec.putLongLEAt(payloads[row], word * Long.BYTES, base + (row == word
                ? span
                : 0));
          }
        }
        final ProjectionSortedLeaf source = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
        assertNotNull(source);
        assertNotNull(source.dataBlock());
        final byte[] original = source.encodedBytes().clone();
        for (final int from : new int[] {0, 1, 5, 31}) {
          assertSameEncoding(ProjectionSortedLeaf.encode(keys, payloads, from, 17), source.sliceBlock(from, 17));
        }
        for (final int position : new int[] {0, 1, 31, 63}) {
          final byte[][] removedKeys = new byte[keys.length - 1][];
          final byte[][] removedPayloads = new byte[removedKeys.length][];
          System.arraycopy(keys, 0, removedKeys, 0, position);
          System.arraycopy(payloads, 0, removedPayloads, 0, position);
          System.arraycopy(keys, position + 1, removedKeys, position, keys.length - position - 1);
          System.arraycopy(payloads, position + 1, removedPayloads, position, keys.length - position - 1);
          assertSameEncoding(ProjectionSortedLeaf.encode(removedKeys, removedPayloads, removedKeys.length),
              source.withRemoved(keys[position]));

          final byte[] replacement = payloads[(position + 1) % payloads.length];
          final byte[][] replaced = payloads.clone();
          replaced[position] = replacement;
          assertSameEncoding(ProjectionSortedLeaf.encode(keys, replaced, keys.length),
              source.withReplaced(position, keys[position], replacement));
        }
        assertArrayEquals(original, source.encodedBytes(), "column copies must preserve the historical source");
      }
    }
  }

  @Test
  void columnEditsMatchCanonicalRowsAcrossWidthsAndEndpoints() {
    for (final long step : new long[] {0, 1, 256, 1L << 48}) {
      for (final int words : new int[] {0, 1, 4}) {
        final byte[][] keys = new byte[64][];
        final byte[][] payloads = new byte[keys.length][];
        for (int row = 0; row < keys.length; row++) {
          keys[row] =
              SortedScanFixtures.key(new SortedScanFixtures.Row("a\0ß", Long.MIN_VALUE + row * step, row * 2 + 2));
          payloads[row] = new byte[words * Long.BYTES];
          for (int word = 0; word < words; word++) {
            ProjectionIndexRowGroupCodec.putLongLEAt(payloads[row], word * Long.BYTES, (row + word) * 257L);
          }
        }
        final ProjectionSortedLeaf source = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
        assertNotNull(Objects.requireNonNull(source).dataBlock());
        final byte[] original = source.encodedBytes().clone();
        final byte[] outlier = new byte[words * Long.BYTES];
        for (int word = 0; word < words; word++) {
          ProjectionIndexRowGroupCodec.putLongLEAt(outlier, word * Long.BYTES, Long.MIN_VALUE + word);
        }
        for (int position = 0; position <= keys.length; position++) {
          final byte[] inserted = SortedScanFixtures.key(new SortedScanFixtures.Row("a\0ß",
              Long.MIN_VALUE + Math.min(position, keys.length - 1) * step, position * 2 + 1));
          final byte[][] nextKeys = new byte[keys.length + 1][];
          final byte[][] nextPayloads = new byte[nextKeys.length][];
          System.arraycopy(keys, 0, nextKeys, 0, position);
          System.arraycopy(payloads, 0, nextPayloads, 0, position);
          nextKeys[position] = inserted;
          nextPayloads[position] = outlier;
          System.arraycopy(keys, position, nextKeys, position + 1, keys.length - position);
          System.arraycopy(payloads, position, nextPayloads, position + 1, keys.length - position);
          assertSameEncoding(ProjectionSortedLeaf.encode(nextKeys, nextPayloads, nextKeys.length),
              source.withInserted(inserted, outlier));
          if (position == keys.length)
            continue;

          final byte[][] removedKeys = new byte[keys.length - 1][];
          final byte[][] removedPayloads = new byte[removedKeys.length][];
          System.arraycopy(keys, 0, removedKeys, 0, position);
          System.arraycopy(payloads, 0, removedPayloads, 0, position);
          System.arraycopy(keys, position + 1, removedKeys, position, keys.length - position - 1);
          System.arraycopy(payloads, position + 1, removedPayloads, position, keys.length - position - 1);
          assertSameEncoding(ProjectionSortedLeaf.encode(removedKeys, removedPayloads, removedKeys.length),
              source.withRemoved(keys[position]));

          final byte[][] replacement = payloads.clone();
          replacement[position] = outlier;
          assertSameEncoding(ProjectionSortedLeaf.encode(keys, replacement, keys.length),
              source.withReplaced(position, keys[position], outlier));
        }
        assertArrayEquals(original, source.encodedBytes(), "historical source remains immutable");
      }
    }
  }

  private static void assertSameEncoding(final @Nullable ProjectionSortedLeaf expected,
      final @Nullable ProjectionSortedLeaf actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertArrayEquals(expected.encodedBytes(), actual.encodedBytes());
    final ProjectionSortedLeaf reopened = ProjectionSortedLeaf.open(actual.encodedBytes());
    for (int row = 0; row < expected.rowCount(); row++) {
      assertArrayEquals(expected.copyKey(row), reopened.copyKey(row));
      assertArrayEquals(expected.copyPayload(row), reopened.copyPayload(row));
    }
  }

  @Test
  void columnSlicesMatchGenericPackingAtEveryWidthAndBoundary() {
    final Random random = new Random(31032);
    for (final long step : new long[] {0, 1, 256, 65536, 1L << 40}) {
      for (final int payloadWords : new int[] {0, 1, 4}) {
        final byte[][] keys = new byte[256][];
        final byte[][] payloads = new byte[keys.length][];
        for (int row = 0; row < keys.length; row++) {
          keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("a\0ß", Long.MIN_VALUE + row * step, row + 1));
          payloads[row] = new byte[payloadWords * Long.BYTES];
          random.nextBytes(payloads[row]);
        }
        final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
        assertNotNull(leaf);
        assertNotNull(leaf.dataBlock());
        final byte[] original = leaf.encodedBytes().clone();
        for (final int target : new int[] {32, 128, 384, 512}) {
          final ProjectionSortedLeaf[] expected =
              ProjectionSortedChunks.pack(keys, payloads, keys.length, SortedScanFixtures.GROUP_VALUE, target);
          final ProjectionSortedLeaf[] actual =
              ProjectionSortedChunks.pack(leaf, SortedScanFixtures.GROUP_VALUE, target);
          assertEquals(expected.length, actual.length);
          for (int i = 0; i < actual.length; i++) {
            assertArrayEquals(expected[i].encodedBytes(), actual[i].encodedBytes(), "canonical slice " + i);
            ProjectionSortedLeaf.open(actual[i].encodedBytes());
          }
        }
        assertArrayEquals(original, leaf.encodedBytes(), "source is immutable");
      }
    }
  }

  @Test
  void fittingSliceRejectsInvalidRangesAndKeepsRejectedWidthJumpOutOfTheEncoding() {
    final byte[][] keys = new byte[128][];
    final byte[][] payloads = new byte[keys.length][];
    for (int row = 0; row < keys.length; row++) {
      keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("slice", row, row));
      payloads[row] = new byte[8];
      ProjectionIndexRowGroupCodec.putLongLEAt(payloads[row], 0, row == 63
          ? Long.MIN_VALUE
          : row);
    }
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
    assertNotNull(leaf);
    final ProjectionSortedDataBlock block = leaf.dataBlock();
    assertNotNull(block);
    assertThrows(IllegalArgumentException.class, () -> block.sliceFitting(0, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> block.sliceFitting(128, 384));
    for (int limit = 128; limit <= 512; limit++) {
      final ProjectionSortedLeaf[] expected =
          ProjectionSortedChunks.pack(keys, payloads, keys.length, SortedScanFixtures.GROUP_VALUE, limit);
      final ProjectionSortedLeaf[] actual = ProjectionSortedChunks.pack(leaf, SortedScanFixtures.GROUP_VALUE, limit);
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++) {
        assertArrayEquals(expected[i].encodedBytes(), actual[i].encodedBytes());
      }
    }
  }

  @Test
  void singleDictionarySummaryPreservesSignedExtremaAndEscapedGroup() {
    final byte[][] keys = new byte[128][];
    for (int row = 0; row < keys.length; row++) {
      final long value = row == 0
          ? Long.MIN_VALUE
          : row == keys.length - 1
              ? Long.MAX_VALUE
              : row - 64L;
      keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("a\0ß", value, row + 1));
    }
    final ProjectionSortedLeaf source = ProjectionSortedLeaf.encode(keys, null, keys.length);
    assertNotNull(source);
    assertNotNull(source.dataBlock(), "exercise the dictionary/SMA shortcut");
    assertTrue(source.dataBlock().hasSingleDictionaryEntry());
    final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(
        ProjectionSortedLeaf.open(source.encodedBytes()), SortedScanFixtures.GROUP_VALUE);
    assertNotNull(summary);
    assertEquals(1, summary.rowCount());
    assertArrayEquals(Arrays.copyOf(keys[0], keys[0].length - 17), summary.copyKey(0));
    final byte[] extrema = summary.copyPayload(0);
    assertEquals(Long.MIN_VALUE, ProjectionIndexRowGroupCodec.getLongLE(extrema, 0));
    assertEquals(Long.MAX_VALUE, ProjectionIndexRowGroupCodec.getLongLE(extrema, Long.BYTES));
  }

  @Test
  void byteWidthsAndSignedExtremesPreserveEveryPointPrefixAndPayload() {
    final Random random = new Random(137);
    for (final long span : new long[] {0, 255, 256, 65535, 65536, 1L << 32, Long.MAX_VALUE, -1L}) {
      final byte[][] keys = new byte[96][];
      final byte[][] payloads = new byte[keys.length][];
      for (int row = 0; row < keys.length; row++) {
        final long value = span < 0
            ? (row % 2 == 0
                ? Long.MIN_VALUE
                : Long.MAX_VALUE)
            : row % 2 == 0
                ? -100
                : -100 + span;
        keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("a\0ß", value, row + 1));
      }
      Arrays.sort(keys, Arrays::compareUnsigned);
      for (int row = 0; row < keys.length; row++) {
        payloads[row] = new byte[24];
        random.nextBytes(payloads[row]);
      }
      final ProjectionSortedDataBlock block = ProjectionSortedDataBlock.encode(keys, payloads, 0, keys.length);
      assertNotNull(block);
      final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.open(block.bytes());
      for (int row = 0; row < keys.length; row++) {
        assertArrayEquals(keys[row], leaf.copyKey(row));
        assertArrayEquals(payloads[row], leaf.copyPayload(row));
        assertEquals(row, leaf.lowerBound(keys[row]));
        for (int length = 0; length <= keys[row].length; length++) {
          final byte[] prefix = Arrays.copyOf(keys[row], length);
          int expected = 0;
          while (expected < keys.length && Arrays.compareUnsigned(keys[expected], prefix) < 0)
            expected++;
          assertEquals(expected, leaf.lowerBound(prefix));
          assertTrue(leaf.keyHasPrefix(row, prefix, length));
        }
      }
    }
  }

  @Test
  void dictionariesAndSingleValuesRemainDirectlyAddressableAfterLocalEdits() {
    final byte[][] keys = new byte[128][];
    final byte[][] payloads = new byte[128][];
    for (int row = 0; row < keys.length; row++) {
      keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("g" + (100 + row / 32), row * 2L, row + 1));
      payloads[row] = new byte[8];
    }
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
    assertNotNull(leaf);
    assertEquals(ProjectionSortedDataBlock.SCHEME, leaf.encodedBytes()[4]);
    assertTrue(leaf.encodedBytes().length < 1024);
    final byte[] inserted = SortedScanFixtures.key(new SortedScanFixtures.Row("g101", 75, 999));
    final byte[] payload = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
    final ProjectionSortedLeaf edited =
        ProjectionSortedLeaf.open(Objects.requireNonNull(leaf.withInserted(inserted, payload)).encodedBytes());
    assertArrayEquals(payload, edited.copyPayload(edited.lowerBound(inserted)));
    assertArrayEquals(leaf.encodedBytes(), Objects.requireNonNull(edited.withRemoved(inserted)).encodedBytes());
    final ProjectionSortedLeaf replaced =
        Objects.requireNonNull(edited.withReplaced(edited.lowerBound(inserted), inserted, new byte[8]));
    assertArrayEquals(new byte[8], replaced.copyPayload(replaced.lowerBound(inserted)));
  }

  @Test
  void malformedDescriptorsVectorsAndSmasFailClosed() {
    final byte[][] keys = new byte[64][];
    for (int row = 0; row < keys.length; row++)
      keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("a", row, row + 1));
    final byte[] bytes = Objects.requireNonNull(ProjectionSortedDataBlock.encode(keys, null, 0, keys.length)).bytes();
    for (int length = 0; length < bytes.length; length++) {
      final byte[] truncated = Arrays.copyOf(bytes, length);
      assertThrows(IllegalArgumentException.class, () -> ProjectionSortedLeaf.open(truncated));
    }
    for (final int offset : new int[] {5, 7, 9, 10, 11, 20}) {
      final byte[] corrupt = bytes.clone();
      corrupt[offset] = (byte) 255;
      assertThrows(IllegalArgumentException.class, () -> ProjectionSortedLeaf.open(corrupt));
    }
  }
}
