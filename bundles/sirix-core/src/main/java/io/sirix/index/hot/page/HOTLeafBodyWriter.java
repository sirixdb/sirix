package io.sirix.index.hot.page;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTPostingFrame;
import io.sirix.node.BytesOut;
import io.sirix.node.SegmentAccess;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/** Write-side scratch and exact size selection for one packed HOT leaf frame. */
public final class HOTLeafBodyWriter {
  private static final int MAX_ENTRIES = HOTLeafPage.MAX_ENTRIES;
  private static final int GROUP_SIZE = 1 << HOTLeafBody.GROUP_SHIFT;
  // Fixed-size owned primitive buffers only; retain per thread to keep serialization allocation-free.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<HOTLeafBodyWriter> WRITER = ThreadLocal.withInitial(HOTLeafBodyWriter::new);

  private final int[] keys = new int[MAX_ENTRIES];
  private final int[] keyLengths = new int[MAX_ENTRIES];
  private final int[] values = new int[MAX_ENTRIES];
  private final int[] valueLengths = new int[MAX_ENTRIES];
  private final int[] offsets = new int[MAX_ENTRIES];
  private final int[] owners = new int[MAX_ENTRIES];
  private final int[] hashes = new int[MAX_ENTRIES];
  private final int[] hashTable = new int[2 * MAX_ENTRIES];
  private final boolean[] packedValues = new boolean[MAX_ENTRIES];
  private final byte[] shared = new byte[HOTLeafBody.bitmapBytes(MAX_ENTRIES)];
  private final int[] groupLengths = new int[HOTLeafBody.groupCount(MAX_ENTRIES)];
  private final MemorySegment postings = MemorySegment.ofArray(new byte[HOTLeafPage.DEFAULT_SIZE]);
  private int used;
  private int mode;

  private HOTLeafBodyWriter() {}

  public static HOTLeafBodyWriter prepare(final HOTLeafPage leaf, final int count, final boolean sparse) {
    final HOTLeafBodyWriter writer = WRITER.get();
    writer.select(leaf, count, sparse);
    return writer;
  }

  public int mode() {
    return mode;
  }

  public int used() {
    return used;
  }

  private void select(final HOTLeafPage leaf, final int count, final boolean sparse) {
    final MemorySegment slots = leaf.slots();
    final int sourceUsed = leaf.getUsedSlotsSize();
    if (sourceUsed < 0 || sourceUsed > slots.byteSize()) {
      throw new IllegalStateException("Invalid used HOT slot region " + sourceUsed);
    }
    final boolean postingIndex = HOTLeafBody.supportsPostingFrames(leaf.getIndexType());
    int ordinaryBytes = 0;
    int postingBytes = 0;
    mode = 0;
    int ordinal = 0;
    boolean fixedWordProjection = leaf.getIndexType() == IndexType.PROJECTION;
    final int prefixLength = leaf.getCommonPrefixLen();
    for (int index = 0; index < leaf.getEntryCount(); index++) {
      if (sparse && !leaf.isEntryDirty(index)) {
        continue;
      }
      final int slot = leaf.getSlotOffset(index);
      if (slot < 0 || slot > sourceUsed - 2 * Short.BYTES) {
        throw invalidSourceSlot(index);
      }
      final int keyLength = Short.toUnsignedInt(SegmentAccess.getShortLE(slots, slot));
      final int key = slot + Short.BYTES;
      fixedWordProjection &= prefixLength + keyLength == Long.BYTES;
      if (keyLength > sourceUsed - key - Short.BYTES) {
        throw invalidSourceSlot(index);
      }
      final int valueLength = Short.toUnsignedInt(SegmentAccess.getShortLE(slots, (long) key + keyLength));
      final int value = key + keyLength + Short.BYTES;
      ordinaryBytes += 2 * Short.BYTES + keyLength + valueLength;
      if (valueLength > sourceUsed - value || ordinaryBytes > sourceUsed) {
        throw invalidSourceSlot(index);
      }
      final int packed = postingIndex
          ? HOTPostingFrame.encodeIfSmaller(slots, value, valueLength, postings, postingBytes)
          : 0;
      keys[ordinal] = key;
      keyLengths[ordinal] = keyLength;
      packedValues[ordinal] = packed != 0;
      if (packed != 0) {
        values[ordinal] = postingBytes;
        valueLengths[ordinal] = packed;
        postingBytes += packed;
        mode |= HOTLeafBody.POSTING_FRAMES;
      } else {
        values[ordinal] = value;
        valueLengths[ordinal] = valueLength;
      }
      ordinal++;
    }
    if (ordinal != count) {
      throw new IllegalStateException("Invalid live HOT leaf slots: count " + ordinal + ", bytes " + ordinaryBytes);
    }
    // Full fixed-word projection reads use the ordinary slot reader to avoid packed validation.
    if (fixedWordProjection) {
      used = ordinaryBytes;
      return;
    }
    if (selectGroups(slots, count)) {
      mode |= HOTLeafBody.GROUP_PREFIXES;
    }
    if (selectSharing(slots, count)) {
      mode |= HOTLeafBody.SHARED_VALUES;
    }
    int maxKey = 0;
    int maxValue = 0;
    final boolean groups = (mode & HOTLeafBody.GROUP_PREFIXES) != 0;
    for (int i = 0; i < count; i++) {
      maxKey = Math.max(maxKey, keyLengths[i] - (groups
          ? groupLengths[i >>> HOTLeafBody.GROUP_SHIFT]
          : 0));
      maxValue = Math.max(maxValue, valueLengths[i]);
    }
    if (maxKey <= 255) {
      mode |= HOTLeafBody.KEY_U8;
    }
    if (maxValue <= 255) {
      mode |= HOTLeafBody.VALUE_U8;
    }

    final int headerBytes = keyWidth() + valueWidth();
    final boolean sharing = (mode & HOTLeafBody.SHARED_VALUES) != 0;
    used = 0;
    for (int i = 0; i < count; i++) {
      offsets[i] = used;
      used += headerBytes + keyLengths[i] - (groups
          ? groupLengths[i >>> HOTLeafBody.GROUP_SHIFT]
          : 0) + (sharing && owners[i] >= 0
              ? Short.BYTES
              : valueLengths[i]);
    }
    int modeOverhead = 1;
    if (sharing) {
      modeOverhead += HOTLeafBody.bitmapBytes(count);
    }
    if (groups) {
      for (int i = 0, end = HOTLeafBody.groupCount(count); i < end; i++) {
        modeOverhead += HOTLeafBody.GROUP_BYTES + groupLengths[i];
      }
    }
    if (used + modeOverhead >= ordinaryBytes) {
      mode = 0;
    }
  }

  private static IllegalStateException invalidSourceSlot(final int index) {
    return new IllegalStateException("HOT source slot " + index + " extends beyond its used region");
  }

  private boolean selectGroups(final MemorySegment slots, final int count) {
    int saving = -HOTLeafBody.GROUP_BYTES * HOTLeafBody.groupCount(count);
    for (int start = 0; start < count; start += GROUP_SIZE) {
      final int end = Math.min(start + GROUP_SIZE, count);
      int length = 0;
      if (end - start > 1) {
        final int last = end - 1;
        final int max = Math.min(255, Math.min(keyLengths[start], keyLengths[last]));
        final long mismatch = MemorySegment.mismatch(slots, keys[start], keys[start] + (long) max, slots, keys[last],
            keys[last] + (long) max);
        length = mismatch < 0
            ? max
            : (int) mismatch;
      }
      groupLengths[start >>> HOTLeafBody.GROUP_SHIFT] = length;
      saving += (end - start - 1) * length;
    }
    return saving > 0;
  }

  private boolean selectSharing(final MemorySegment slots, final int count) {
    if (count < 2) {
      return false;
    }
    final int bitmapBytes = HOTLeafBody.bitmapBytes(count);
    Arrays.fill(shared, 0, bitmapBytes, (byte) 0);
    Arrays.fill(hashTable, 0);
    int saving = -bitmapBytes;
    for (int i = 0; i < count; i++) {
      owners[i] = -1;
      final int len = valueLengths[i];
      if (len <= Short.BYTES) {
        continue;
      }
      final MemorySegment data = valueSource(slots, i);
      final int at = values[i];
      final int hash = hash(data, at, len);
      hashes[i] = hash;
      final int mask = hashTable.length - 1;
      int cell = hash & mask;
      while (hashTable[cell] != 0) {
        final int candidate = hashTable[cell] - 1;
        final int candidateAt = values[candidate];
        if (hashes[candidate] == hash && valueLengths[candidate] == len && MemorySegment.mismatch(data, at,
            at + (long) len, valueSource(slots, candidate), candidateAt, candidateAt + (long) len) < 0) {
          owners[i] = candidate;
          shared[i >>> 3] |= (byte) (1 << (i & 7));
          saving += len - Short.BYTES;
          break;
        }
        cell = (cell + 1) & mask;
      }
      if (owners[i] < 0) {
        hashTable[cell] = i + 1;
      }
    }
    return saving > 0;
  }

  private MemorySegment valueSource(final MemorySegment slots, final int index) {
    return packedValues[index]
        ? postings
        : slots;
  }

  private static int hash(final MemorySegment data, final int start, final int length) {
    long hash = length;
    int i = 0;
    for (; i <= length - Long.BYTES; i += Long.BYTES) {
      hash = Long.rotateLeft(hash, 23) ^ SegmentAccess.getLongLE(data, start + (long) i);
    }
    for (; i < length; i++) {
      hash = Long.rotateLeft(hash, 5) ^ (SegmentAccess.getByte(data, start + (long) i) & 255L);
    }
    hash *= 0x9e3779b97f4a7c15L;
    return (int) (hash ^ (hash >>> Integer.SIZE));
  }

  public void write(final BytesOut<?> sink, final HOTLeafPage leaf, final int count) {
    final MemorySegment slots = leaf.slots();
    for (int i = 0; i < count; i++) {
      sink.writeShort((short) offsets[i]);
    }
    final boolean sharing = (mode & HOTLeafBody.SHARED_VALUES) != 0;
    if (sharing) {
      sink.write(shared, 0, HOTLeafBody.bitmapBytes(count));
    }
    final boolean groups = (mode & HOTLeafBody.GROUP_PREFIXES) != 0;
    if (groups) {
      writeGroups(sink, slots, count);
    }
    final int keyWidth = keyWidth();
    final int valueWidth = valueWidth();
    for (int i = 0; i < count; i++) {
      final int skip = groups
          ? groupLengths[i >>> HOTLeafBody.GROUP_SHIFT]
          : 0;
      writeLength(sink, keyLengths[i] - skip, keyWidth);
      sink.writeSegment(slots, keys[i] + (long) skip, keyLengths[i] - (long) skip);
      writeLength(sink, valueLengths[i], valueWidth);
      if (sharing && owners[i] >= 0) {
        sink.writeShort((short) (owners[i] * Short.BYTES));
      } else {
        sink.writeSegment(valueSource(slots, i), values[i], valueLengths[i]);
      }
    }
  }

  private void writeGroups(final BytesOut<?> sink, final MemorySegment slots, final int count) {
    int offset = 0;
    for (int group = 0, end = HOTLeafBody.groupCount(count); group < end; group++) {
      sink.writeByte((byte) groupLengths[group]);
      sink.writeShort((short) offset);
      offset += groupLengths[group];
    }
    for (int group = 0, end = HOTLeafBody.groupCount(count); group < end; group++) {
      sink.writeSegment(slots, keys[group << HOTLeafBody.GROUP_SHIFT], groupLengths[group]);
    }
  }

  private int keyWidth() {
    return (mode & HOTLeafBody.KEY_U8) == 0
        ? Short.BYTES
        : 1;
  }

  private int valueWidth() {
    return (mode & HOTLeafBody.VALUE_U8) == 0
        ? Short.BYTES
        : 1;
  }

  private static void writeLength(final BytesOut<?> sink, final int length, final int width) {
    if (width == 1) {
      sink.writeByte((byte) length);
    } else {
      sink.writeShort((short) length);
    }
  }
}
