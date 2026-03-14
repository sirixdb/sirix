package io.sirix.page;

import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Extracts string-type node columns directly from slotted page MemorySegments.
 *
 * <p>Scans the page bitmap, filters for STRING_VALUE (kindId=30) and
 * OBJECT_STRING_VALUE (kindId=40) nodes, and extracts their metadata
 * (nodeKey, parentKey, payload offset/length, compression flag, DeweyID)
 * into caller-provided primitive arrays — zero allocation on the hot path.</p>
 *
 * <p>This enables page-at-a-time columnar extraction: instead of
 * per-node moveTo + flyweight bind + field extract, we scan the page
 * MemorySegment directly with sequential memory access patterns.</p>
 *
 * <p>The extractor does NOT own the output arrays. The caller (e.g.,
 * ColumnarScanAxis) owns them and passes them in, enabling multi-page
 * accumulation without intermediate copies.</p>
 */
public final class ColumnarPageExtractor {

  /** NodeKind ID for STRING_VALUE. */
  private static final int KIND_STRING_VALUE = NodeKind.STRING_VALUE.getId();

  /** NodeKind ID for OBJECT_STRING_VALUE. */
  private static final int KIND_OBJECT_STRING_VALUE = NodeKind.OBJECT_STRING_VALUE.getId();

  /** Field count for STRING_VALUE nodes. */
  private static final int STRING_VALUE_FIELD_COUNT = NodeFieldLayout.STRING_VALUE_FIELD_COUNT; // 6

  /** Field count for OBJECT_STRING_VALUE nodes. */
  private static final int OBJECT_STRING_VALUE_FIELD_COUNT = NodeFieldLayout.OBJECT_STRING_VALUE_FIELD_COUNT; // 4

  /** Payload field index for STRING_VALUE. */
  private static final int STRVAL_PAYLOAD_IDX = NodeFieldLayout.STRVAL_PAYLOAD; // 5

  /** Payload field index for OBJECT_STRING_VALUE. */
  private static final int OBJSTRVAL_PAYLOAD_IDX = NodeFieldLayout.OBJSTRVAL_PAYLOAD; // 3

  /**
   * Parent key field index — same for both string node types.
   * Uses STRVAL_PARENT_KEY as the canonical source; asserted equal to OBJSTRVAL_PARENT_KEY.
   */
  private static final int PARENT_KEY_IDX = NodeFieldLayout.STRVAL_PARENT_KEY;

  static {
    assert NodeFieldLayout.STRVAL_PARENT_KEY == NodeFieldLayout.OBJSTRVAL_PARENT_KEY
        : "Parent key field index must be the same for STRING_VALUE and OBJECT_STRING_VALUE";
  }

  /**
   * Extract all string-type slots from a page, appending to the given arrays
   * starting at writePos. Returns the new write position.
   *
   * @param kvlPage     the page to extract from
   * @param nodeKeys    output: absolute node keys
   * @param parentKeys  output: delta-decoded parent keys
   * @param payloadOffs output: absolute byte offset of value bytes in page
   * @param valueLens   output: byte length of value (compressed or raw)
   * @param compressed  output: per-row compression flag
   * @param deweyOffs   output: absolute byte offset of DeweyID (or -1)
   * @param deweyLens   output: byte length of DeweyID (or 0)
   * @param writePos    starting index in output arrays
   * @return new write position (writePos + number of extracted rows)
   */
  public int extractStringsFromPage(
      final KeyValueLeafPage kvlPage,
      final long[] nodeKeys, final long[] parentKeys,
      final int[] payloadOffs, final int[] valueLens, final boolean[] compressed,
      final int[] deweyOffs, final int[] deweyLens,
      int writePos) {

    final MemorySegment page = kvlPage.getSlottedPage();
    if (page == null) {
      return writePos;
    }

    final int populatedCount = PageLayout.getPopulatedCount(page);
    if (populatedCount == 0) {
      return writePos;
    }

    final boolean hasDewey = PageLayout.areDeweyIDsStored(page);
    final long pageBaseNodeKey = PageLayout.getRecordPageKey(page) << PageLayout.SLOT_COUNT_EXPONENT;

    // Local scratch buffer for bitmap iteration — 16 longs = 128 bytes.
    // Trivial allocation that the JIT will likely scalar-replace, and avoids
    // thread-safety issues with a mutable instance field.
    final long[] bitmapWords = new long[PageLayout.BITMAP_WORDS];
    PageLayout.copyBitmapTo(page, bitmapWords);

    int seen = 0;
    for (int wordIdx = 0; wordIdx < PageLayout.BITMAP_WORDS && seen < populatedCount; wordIdx++) {
      long word = bitmapWords[wordIdx];
      while (word != 0) {
        final int bit = Long.numberOfTrailingZeros(word);
        final int slot = (wordIdx << 6) | bit;
        word &= word - 1; // Kernighan's trick: clear lowest set bit
        seen++;

        // Fast kindId check from directory entry
        final int kindId = PageLayout.getDirNodeKindId(page, slot);
        if (kindId != KIND_STRING_VALUE && kindId != KIND_OBJECT_STRING_VALUE) {
          continue;
        }

        // Compute record base
        final int heapOffset = PageLayout.getDirHeapOffset(page, slot);
        final long recordBase = PageLayout.HEAP_START + (long) heapOffset;

        // Determine field layout based on node kind
        final int fieldCount;
        final int payloadFieldIdx;
        if (kindId == KIND_STRING_VALUE) {
          fieldCount = STRING_VALUE_FIELD_COUNT;
          payloadFieldIdx = STRVAL_PAYLOAD_IDX;
        } else {
          fieldCount = OBJECT_STRING_VALUE_FIELD_COUNT;
          payloadFieldIdx = OBJSTRVAL_PAYLOAD_IDX;
        }

        // Read field offsets from the offset table
        final long dataStart = PageLayout.dataRegionStart(recordBase, fieldCount);
        final int parentOff = PageLayout.readFieldOffset(page, recordBase, PARENT_KEY_IDX);
        final int payloadOff = PageLayout.readFieldOffset(page, recordBase, payloadFieldIdx);

        // Absolute node key
        final long nodeKey = pageBaseNodeKey | slot;

        // Delta-decode parent key
        parentKeys[writePos] = DeltaVarIntCodec.decodeDeltaFromSegment(
            page, dataStart + parentOff, nodeKey);

        // Read payload header: [isCompressed:1][valueLength:varint][valueBytes...]
        final long payloadAbs = dataStart + payloadOff;
        final boolean isComp = page.get(ValueLayout.JAVA_BYTE, payloadAbs) != 0;
        final int valueLen = DeltaVarIntCodec.decodeSignedFromSegment(page, payloadAbs + 1);
        final int varintWidth = DeltaVarIntCodec.readSignedVarintWidth(page, payloadAbs + 1);
        final int valueBytesOffset = (int) (payloadAbs + 1 + varintWidth);

        // Store results
        nodeKeys[writePos] = nodeKey;
        payloadOffs[writePos] = valueBytesOffset;
        valueLens[writePos] = valueLen;
        compressed[writePos] = isComp;

        // DeweyID extraction from record tail (skip entirely when page has no DeweyIDs)
        if (hasDewey) {
          final int deweyLen = PageLayout.getDeweyIdLength(page, slot);
          if (deweyLen > 0) {
            final int recordLen = PageLayout.getRecordOnlyLength(page, slot);
            deweyOffs[writePos] = (int) (recordBase + recordLen);
            deweyLens[writePos] = deweyLen;
          } else {
            deweyOffs[writePos] = -1;
            deweyLens[writePos] = 0;
          }
        } else {
          deweyOffs[writePos] = -1;
          deweyLens[writePos] = 0;
        }

        writePos++;
        if (writePos >= nodeKeys.length) {
          return writePos; // batch full
        }
      }
    }

    return writePos;
  }
}
