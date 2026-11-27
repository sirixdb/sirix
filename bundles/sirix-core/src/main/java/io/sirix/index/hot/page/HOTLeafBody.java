package io.sirix.index.hot.page;

import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTPostingFrame;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.SegmentAccess;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

/** Offsets and bounded access over the packed wire bytes of a nonzero HOT leaf mode. */
public final class HOTLeafBody {
  public static final byte FLAG_BODY_MODE = 0x08;
  public static final int KEY_U8 = 1;
  public static final int VALUE_U8 = 2;
  public static final int SHARED_VALUES = 4;
  public static final int GROUP_PREFIXES = 8;
  public static final int POSTING_FRAMES = 16;
  public static final int GROUP_SHIFT = 4;
  public static final int GROUP_BYTES = 3;
  /** A leaf has at most 512 owners, each with an unsigned 16-bit sub-id namespace. */
  public static final int MAX_SIDE_REFERENCES =
      HOTLeafPage.MAX_ENTRIES * (HOTLeafPage.MAX_OVERFLOW_PAGE_REF_SUB_ID + 1);
  private static final int KNOWN_MODES = KEY_U8 | VALUE_U8 | SHARED_VALUES | GROUP_PREFIXES | POSTING_FRAMES;
  private static final VarHandle ARRAY_LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle ARRAY_SHORT_LE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle ARRAY_SHORT_BE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle ARRAY_INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle ARRAY_INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  // Borrowed selectors keep only a synchronous, thread-confined view. They clear the backing
  // before returning the copied result, so a query retains neither its bytes nor its allocator frame.
  @SuppressWarnings("java:S5164") // Backing references are cleared in selectBorrowed's finally block.
  private static final ThreadLocal<HOTLeafBody> BORROWED = ThreadLocal.withInitial(HOTLeafBody::new);

  // All source coordinates are relative to the directory start. Once published a body is immutable.
  private @Nullable MemorySegment image;
  private byte @Nullable [] array;
  private long base;
  private int count;
  private int used;
  private int mode;
  private int bitmap;
  private int groups;
  private int prefixes;
  private int slots;
  private int keyWidth;
  private int valueWidth;
  private int uniformSuffixLength = -1;

  private HOTLeafBody() {}

  private void setLayout(final int count, final int used, final int mode, final int bitmap, final int groups,
      final int prefixes, final int slots) {
    this.count = count;
    this.used = used;
    this.mode = mode;
    this.bitmap = bitmap;
    this.groups = groups;
    this.prefixes = prefixes;
    this.slots = slots;
    keyWidth = (mode & KEY_U8) == 0
        ? Short.BYTES
        : 1;
    valueWidth = (mode & VALUE_U8) == 0
        ? Short.BYTES
        : 1;
  }

  /** Consume the mode byte before any index-specific page body is read. */
  public static int readMode(final BytesIn<?> source, final int flags) {
    if ((flags & FLAG_BODY_MODE) == 0) {
      return 0;
    }
    // A body without the narrow directory is refused by requireDirectory (entries) or by the body
    // dimensions (no entries), which every reader applies before reading the body.
    if (source.remaining() == 0) {
      throw new IllegalStateException("Missing packed HOT leaf mode");
    }
    final int mode = Byte.toUnsignedInt(source.readByte());
    if (mode == 0 || (mode & ~KNOWN_MODES) != 0) {
      throw new IllegalStateException("Unknown packed HOT leaf mode " + mode);
    }
    return mode;
  }

  /** Only the empty V0 leaf omits the unsigned-short directory flag. */
  public static void requireDirectory(final int flags, final int count, final int used, final long remaining) {
    if (((flags & HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS) != 0) != (count != 0) || count > HOTLeafPage.MAX_ENTRIES
        || used < 0 || used > HOTLeafPage.DEFAULT_SIZE || (long) count * Short.BYTES + used > remaining) {
      throw new IllegalStateException("Invalid HOT leaf directory or dimensions");
    }
  }

  public static long readPageKey(final BytesIn<?> source) {
    return readStopBit(source, 63, 1);
  }

  /** Read the optional trailer count and verify the byte span before allocating from it. */
  public static int readSideReferenceCount(final BytesIn<?> source) {
    final long count = readStopBit(source, 28, 0x0f);
    if (count > MAX_SIDE_REFERENCES) {
      throw new IllegalStateException("Invalid HOT leaf side-reference count " + count);
    }
    final long remaining = source.remaining();
    final long expected = count * (2 * Long.BYTES);
    if (remaining < expected) {
      throw new IndexOutOfBoundsException("Truncated HOT leaf side-reference trailer");
    }
    if (remaining > expected) {
      throw new IllegalStateException("Trailing HOT leaf bytes");
    }
    return (int) count;
  }

  private static long readStopBit(final BytesIn<?> source, final int lastShift, final int lastPayloadMask) {
    int next = source.readByte();
    if (next >= 0) {
      return next;
    }
    long result = next & 0x7f;
    for (int shift = 7; shift < lastShift; shift += 7) {
      next = source.readByte();
      if (next >= 0) {
        return result | (long) next << shift;
      }
      result |= (long) (next & 0x7f) << shift;
    }
    next = Byte.toUnsignedInt(source.readByte());
    if (next > lastPayloadMask) {
      throw new IllegalStateException("HOT leaf stop-bit integer exceeds its wire width");
    }
    return result | (long) next << lastShift;
  }

  public static boolean supportsPostingFrames(final IndexType indexType) {
    return indexType == IndexType.CAS || indexType == IndexType.VALIDTIME;
  }

  public static void requireIndexMode(final int mode, final IndexType indexType) {
    if ((mode & POSTING_FRAMES) != 0 && !supportsPostingFrames(indexType)) {
      throw new IllegalStateException("Frame-of-reference postings require a CAS or VALIDTIME HOT leaf");
    }
  }

  /** Consume a body region. Input owners must keep these bytes intact while any view is live. */
  public static HOTLeafBody read(final BytesIn<?> source, final int mode, final int count, final int used) {
    final HOTLeafBody body = new HOTLeafBody();
    body.readRegion(source, mode, count, used);
    return body;
  }

  public static byte @Nullable [] selectBorrowed(final BytesIn<?> source, final int mode, final int count,
      final int used, final byte[] key, final int prefixLength, final boolean prefixMatches) {
    // Thread-confined and cleared below; a foreign source gets its own copied array in readRegion.
    final HOTLeafBody body = BORROWED.get();
    try {
      body.readRegion(source, mode, count, used);
      final long ref = prefixMatches
          ? body.findValueRef(key, prefixLength)
          : HOTLeafPage.NO_VALUE_REF;
      return ref == HOTLeafPage.NO_VALUE_REF
          ? null
          : body.copyValue(ref);
    } finally {
      body.image = null;
      body.array = null;
    }
  }

  private void readRegion(final BytesIn<?> source, final int mode, final int count, final int used) {
    uniformSuffixLength = -1;
    if (count <= 0 || count > HOTLeafPage.MAX_ENTRIES || used <= 0 || used > HOTLeafPage.DEFAULT_SIZE) {
      throw malformed("dimensions");
    }
    final int directory = count * Short.BYTES;
    final int bitmapBytes = (mode & SHARED_VALUES) == 0
        ? 0
        : bitmapBytes(count);
    final int groups = directory + bitmapBytes;
    final int groupCount = (mode & GROUP_PREFIXES) == 0
        ? 0
        : groupCount(count);
    final int prefixes = groups + GROUP_BYTES * groupCount;
    if ((long) prefixes + used > source.remaining()) {
      throw malformed("truncated directory or metadata");
    }
    final long start = source.position();
    int prefixBytes = 0;
    if (groupCount != 0) {
      source.skip(groups);
      for (int group = 0; group < groupCount; group++) {
        final int length = Byte.toUnsignedInt(source.readByte());
        if (Short.toUnsignedInt(source.readShort()) != prefixBytes) {
          throw malformed("group prefix offset");
        }
        prefixBytes += length;
      }
      source.position(start);
      if ((long) prefixes + prefixBytes + used > source.remaining()) {
        throw malformed("truncated prefixes or slots");
      }
    }
    final int slots = prefixes + prefixBytes;
    final int bytes = slots + used;
    if (source instanceof MemorySegmentBytesIn nativeSource) {
      image = nativeSource.getSource();
      base = start;
      source.skip(bytes);
    } else if (source instanceof ByteArrayBytesIn arraySource) {
      array = arraySource.getSource();
      base = start;
      source.skip(bytes);
    } else {
      array = new byte[bytes];
      base = 0;
      source.read(array);
    }
    if (bitmapBytes != 0 && (count & 7) != 0 && unsignedByte(groups - 1) >>> (count & 7) != 0) {
      throw malformed("unused sharing bits");
    }
    if (slotOffset(0) != 0) {
      throw malformed("initial slot offset");
    }
    setLayout(count, used, mode, bitmapBytes == 0
        ? 0
        : directory,
        groupCount == 0
            ? 0
            : groups,
        prefixes, slots);
  }

  public int byteSize() {
    return slots + used;
  }

  /** Rebase an unpublished view to memory its leaf can retain. */
  public HOTLeafBody retainIn(final MemorySegment destination) {
    copyRange(0, destination, 0, byteSize());
    image = destination;
    array = null;
    base = 0;
    return this;
  }

  /** Keep a cached packed body independent of the lifetime of an existing leaf. */
  HOTLeafBody copyTo(final MemorySegment destination) {
    copyRange(0, destination, 0, byteSize());
    final HOTLeafBody copy = new HOTLeafBody();
    copy.image = destination;
    copy.setLayout(count, used, mode, bitmap, groups, prefixes, slots);
    copy.uniformSuffixLength = uniformSuffixLength;
    return copy;
  }

  public MemorySegment slots() {
    return image.asSlice(base + slots, used);
  }

  public boolean hasPostingFrames() {
    return (mode & POSTING_FRAMES) != 0;
  }

  public static int bitmapBytes(final int count) {
    return (count + 7) >>> 3;
  }

  public static int groupCount(final int count) {
    return (count + 15) >>> GROUP_SHIFT;
  }

  public int slotOffset(final int index) {
    return u16(index * Short.BYTES);
  }

  public int suffixStart(final int index) {
    return slotOffset(index) + keyWidth;
  }

  public int keyTailLength(final int index) {
    return groupLength(index) + suffixLength(index);
  }

  private int suffixLength(final int index) {
    return lengthAt(slots + slotOffset(index), keyWidth);
  }

  private int groupEntry(final int index) {
    return groups + (index >>> GROUP_SHIFT) * GROUP_BYTES;
  }

  private int groupLength(final int index) {
    return groups == 0
        ? 0
        : unsignedByte(groupEntry(index));
  }

  private int groupStart(final int index) {
    return prefixes + u16(groupEntry(index) + 1);
  }

  public int keyTailByte(final int index, final int pos) {
    final int groupLength = groupLength(index);
    return unsignedByte(pos < groupLength
        ? groupStart(index) + pos
        : slots + suffixStart(index) + pos - groupLength);
  }

  /** Big-endian int of four key-tail bytes starting at {@code pos}; the caller checks the range. */
  public int keyTailInt(final int index, final int pos) {
    return (int) (keyTailWord(index, pos) >>> Integer.SIZE);
  }

  /** High-aligned bytes of a key tail, padding only beyond the end of the key. */
  public long keyTailWord(final int index, final int pos) {
    final int groupLength = groupLength(index);
    final int suffixLength = suffixLength(index);
    final int suffixStart = slots + suffixStart(index);
    if (pos >= groupLength) {
      return readWord(suffixStart + pos - groupLength, suffixLength + groupLength - pos);
    }
    final int groupBytes = groupLength - pos;
    final long word = readWord(groupStart(index) + pos, groupBytes);
    return groupBytes >= Long.BYTES
        ? word
        : word | (readWord(suffixStart, suffixLength) >>> (groupBytes * Byte.SIZE));
  }

  private long readWord(final int start, final int length) {
    if (length >= Long.BYTES) {
      return longBE(start);
    }
    long word = 0;
    int i = 0;
    if (length >= Integer.BYTES) {
      word = Integer.toUnsignedLong(intBE(start)) << Integer.SIZE;
      i = Integer.BYTES;
    }
    if (length - i >= Short.BYTES) {
      word |= (long) Short.toUnsignedInt(shortBE(start + i)) << (48 - i * Byte.SIZE);
      i += Short.BYTES;
    }
    if (i < length) {
      word |= (long) unsignedByte(start + i) << (56 - i * Byte.SIZE);
    }
    return word;
  }

  public int differingBit(final int left, final int right) {
    final int a = keyTailLength(left);
    final int b = keyTailLength(right);
    final int end = Math.min(a, b);
    for (int pos = 0; pos < end; pos += Long.BYTES) {
      final long difference = keyTailWord(left, pos) ^ keyTailWord(right, pos);
      if (difference != 0) {
        return Math.min(end * Byte.SIZE, pos * Byte.SIZE + Long.numberOfLeadingZeros(difference));
      }
    }
    return a == b
        ? -1
        : end * Byte.SIZE;
  }

  public void copyKeyTail(final int index, final MemorySegment destination, final int at) {
    final int groupLength = groupLength(index);
    if (groupLength != 0) {
      copyRange(groupStart(index), destination, at, groupLength);
    }
    copyRange(slots + suffixStart(index), destination, at + groupLength, suffixLength(index));
  }

  int findLongSuffix(final byte[] key, final int commonPrefixLength) {
    final int wanted = Long.BYTES - commonPrefixLength;
    final long probe = (long) ARRAY_LONG_BE.get(key, 0) << (commonPrefixLength * Byte.SIZE);
    final int maxWordStart = slots + used - Long.BYTES;
    int low = 0;
    int high = count;
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final int offset = slots + slotOffset(middle);
      final int suffixLength = lengthAt(offset, keyWidth);
      final int groupLength = groupLength(middle);
      final int comparison;
      if (groupLength == 0 && suffixLength == wanted) {
        final int start = offset + keyWidth;
        final int shift = commonPrefixLength * Byte.SIZE;
        final long stored;
        if (start <= maxWordStart) {
          HOTLeafPage.countSuffixProbe();
          stored = longBE(start) & (-1L << shift);
        } else if (start - commonPrefixLength >= slots) {
          // End the word at the end of the suffix when a forward load would cross the region.
          HOTLeafPage.countSuffixProbe();
          stored = longBE(start - commonPrefixLength) << shift;
        } else {
          stored = readWord(start, wanted);
          HOTLeafPage.countSuffixProbes(Integer.bitCount(wanted));
        }
        comparison = Long.compareUnsigned(stored, probe);
      } else {
        final int length = groupLength + suffixLength;
        final int prefixComparison = compareKeyTail(middle, key, commonPrefixLength, Math.min(length, wanted));
        comparison = prefixComparison == 0
            ? length - wanted
            : prefixComparison;
      }
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle;
      } else {
        return middle;
      }
    }
    return -low - 1;
  }

  /** Compare the requested prefix of a key's suffix without joining its separate ranges. */
  public int compareKeyTail(final int index, final byte[] key, final int keyStart, final int length) {
    final int groupLength = groupLength(index);
    return compareKeyTail(index, groupLength, suffixStart(index), key, keyStart, length);
  }

  private int compareKeyTail(final int index, final int groupLength, final int suffixStart, final byte[] key,
      final int keyStart, final int length) {
    final int groupCompared = Math.min(groupLength, length);
    if (groupCompared != 0) {
      final int comparison = compareRange(groupStart(index), key, keyStart, groupCompared);
      if (comparison != 0 || groupCompared == length) {
        return comparison;
      }
    }
    return compareRange(slots + suffixStart, key, keyStart + groupCompared, length - groupCompared);
  }

  private int compareRange(final int sourceStart, final byte[] key, final int keyStart, final int length) {
    int i = 0;
    for (; i <= length - Long.BYTES; i += Long.BYTES) {
      HOTLeafPage.countSuffixProbe();
      final long actual = longBE(sourceStart + i);
      final long expected = (long) ARRAY_LONG_BE.get(key, keyStart + i);
      if (actual != expected) {
        return Long.compareUnsigned(actual, expected);
      }
    }
    for (; i < length; i++) {
      HOTLeafPage.countSuffixProbe();
      final int delta = unsignedByte(sourceStart + i) - Byte.toUnsignedInt(key[keyStart + i]);
      if (delta != 0) {
        return delta;
      }
    }
    return 0;
  }

  private int checkedSuffixLength(final int offset, final int end) {
    if (offset > end - keyWidth - valueWidth) {
      throw malformed("slot header");
    }
    final int suffixLength = lengthAt(slots + offset, keyWidth);
    if (suffixLength > end - offset - keyWidth - valueWidth) {
      throw malformed("slot suffix length");
    }
    return suffixLength;
  }

  public long valueRef(final int index) {
    final int slot = slotOffset(index);
    return valueRef(index, slot, lengthAt(slots + slot, keyWidth));
  }

  private long valueRef(final int index, final int slot, final int suffixLength) {
    final int valueStart = slot + keyWidth + suffixLength + valueWidth;
    final int length = lengthAt(slots + valueStart - valueWidth, valueWidth);
    final int from;
    if (isShared(index)) {
      final int target = u16(u16(slots + valueStart));
      final int targetLength = lengthAt(slots + target, keyWidth);
      from = target + keyWidth + targetLength + valueWidth;
    } else {
      from = valueStart;
    }
    return ((long) from << Integer.SIZE) | length;
  }

  public long checkedValueRef(final int index) {
    final int offset = slotOffset(index);
    final int end = slotEnd(index, offset);
    return checkedValueRef(index, offset, end, checkedSuffixLength(offset, end));
  }

  private long checkedValueRef(final int index, final int offset, final int end, final int suffixLength) {
    final int at = offset + keyWidth + suffixLength + valueWidth;
    final int length = lengthAt(slots + at - valueWidth, valueWidth);
    if (!isShared(index)) {
      return checkedInlineValueRef(at, end, length);
    }
    final int target = checkedOwnerEntry(index, at, end, length);
    return checkedSharedValueRef(offset, target, length);
  }

  private static long checkedInlineValueRef(final int at, final int end, final int length) {
    if (length != end - at) {
      throw malformed("inline value length");
    }
    return ((long) at << Integer.SIZE) | length;
  }

  private int checkedOwnerEntry(final int index, final int at, final int end, final int length) {
    if (end - at != Short.BYTES || length <= Short.BYTES) {
      throw malformed("shared value length");
    }
    final int target = u16(slots + at);
    requireEarlierOwner(index, target);
    return target;
  }

  private static void requireEarlierOwner(final int index, final int target) {
    if ((target & 1) != 0 || target >= index * Short.BYTES) {
      throw malformed("shared target directory entry");
    }
  }

  private long aliasSignature(final int at, final int end) {
    if (end - at != Short.BYTES) {
      throw malformed("shared value length");
    }
    if (valueWidth == 1 && image != null) {
      // The last four slot bytes include one key/header byte, the length and the complete target.
      final int lane = SegmentAccess.getIntLE(image, base + slots + at - 2);
      return Integer.toUnsignedLong((lane & 0xffff0000) | ((lane >>> Byte.SIZE) & 255));
    }
    return ((long) u16(slots + at) << Short.SIZE) | lengthAt(slots + at - valueWidth, valueWidth);
  }

  private long checkedSharedValueRef(final int offset, final int targetEntry, final int length) {
    final int owner = targetEntry / Short.BYTES;
    final int target = u16(targetEntry);
    final int ownerEnd = slotEnd(owner, target);
    if (ownerEnd > offset) {
      throw malformed("overlapping shared target");
    }
    if (isShared(owner)) {
      throw malformed("nested shared value");
    }
    final int from = target + keyWidth + checkedSuffixLength(target, ownerEnd) + valueWidth;
    if (length != lengthAt(slots + from - valueWidth, valueWidth) || length != ownerEnd - from) {
      throw malformed("shared target value length");
    }
    return ((long) from << Integer.SIZE) | length;
  }

  public void validate(final int commonPrefixLength, final IndexType indexType) {
    final boolean postingIndex = supportsPostingFrames(indexType);
    int ordinaryUsed = 0;
    int offset = 0;
    long lastAlias = -1L;
    long lastSharedRef = HOTLeafPage.NO_VALUE_REF;
    int firstLength = -1;
    boolean uniform = groups == 0;
    MemorySegment postingSource = image;
    for (int index = 0; index < count; index++) {
      final int end = slotEnd(index, offset);
      final int suffixLength = checkedSuffixLength(offset, end);
      if (index == 0) {
        firstLength = suffixLength;
      }
      uniform &= suffixLength == firstLength;
      final int keyLength = groupLength(index) + suffixLength;
      if (commonPrefixLength + keyLength > 0xffff) {
        throw malformed("reconstructed key length");
      }
      final int at = offset + keyWidth + suffixLength + valueWidth;
      int valueLength;
      final boolean shared = isShared(index);
      final long ref;
      if (shared) {
        final long alias = aliasSignature(at, end);
        valueLength = (int) alias & 0xffff;
        if (alias != lastAlias) {
          if (valueLength <= Short.BYTES) {
            throw malformed("shared value length");
          }
          final int owner = (int) (alias >>> Short.SIZE);
          requireEarlierOwner(index, owner);
          lastSharedRef = checkedSharedValueRef(offset, owner, valueLength);
          // Directory order makes the same proved target and length valid for later aliases.
          lastAlias = alias;
        }
        ref = lastSharedRef;
      } else {
        valueLength = lengthAt(slots + at - valueWidth, valueWidth);
        ref = checkedInlineValueRef(at, end, valueLength);
      }
      if (postingIndex && valueLength != 0
          && unsignedByte(slots + (int) (ref >>> Integer.SIZE)) == HOTPostingFrame.TAG) {
        if (!hasPostingFrames()) {
          throw malformed("undeclared posting mode");
        }
        if (postingSource == null) {
          postingSource = MemorySegment.ofArray(array);
        }
        final int start = Math.toIntExact(base + slots + (ref >>> Integer.SIZE));
        if (!shared) {
          // The inline owner has already been validated in directory order.
          HOTPostingFrame.validate(postingSource, start, valueLength);
        }
        valueLength = HOTPostingFrame.ordinaryLength(postingSource, start);
      }
      ordinaryUsed += 2 * Short.BYTES + keyLength + valueLength;
      if (ordinaryUsed > HOTLeafPage.DEFAULT_SIZE) {
        throw malformed("ordinary slot capacity");
      }
      offset = end;
    }
    uniformSuffixLength = uniform
        ? firstLength
        : -1;
  }

  public long findValueRef(final byte[] key, final int commonPrefixLength) {
    final int wanted = key.length - commonPrefixLength;
    final byte[] bytes = array;
    if (bytes != null && groups == 0 && wanted == 1) {
      return findSingleByteValueRef(bytes, Byte.toUnsignedInt(key[commonPrefixLength]));
    }
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int offset = slotOffset(middle);
      final int end = slotEnd(middle, offset);
      final int suffixLength = checkedSuffixLength(offset, end);
      final int sharedPrefix = groupLength(middle);
      final int length = suffixLength + sharedPrefix;
      int comparison =
          compareKeyTail(middle, sharedPrefix, offset + keyWidth, key, commonPrefixLength, Math.min(wanted, length));
      if (comparison == 0) {
        comparison = length - wanted;
      }
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle - 1;
      } else {
        return checkedValueRef(middle, offset, end, suffixLength);
      }
    }
    return HOTLeafPage.NO_VALUE_REF;
  }

  private long findSingleByteValueRef(final byte[] bytes, final int wanted) {
    final int directoryBase = (int) base;
    final int suffixBase = directoryBase + slots + keyWidth;
    final int last = count - 1;
    int low = 0;
    int high = last;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int at = directoryBase + middle * Short.BYTES;
      final int offset;
      final int end;
      if (middle == last) {
        offset = Short.toUnsignedInt((short) ARRAY_SHORT_LE.get(bytes, at));
        end = used;
      } else {
        // The bounded directory already contains both complete entries.
        final int adjacent = (int) ARRAY_INT_LE.get(bytes, at);
        offset = adjacent & 0xffff;
        end = adjacent >>> Short.SIZE;
      }
      requireSlotOrder(offset, end);
      final int suffixLength = checkedSuffixLength(offset, end);
      int comparison = -1;
      if (suffixLength != 0) {
        HOTLeafPage.countSuffixProbe();
        comparison = Byte.toUnsignedInt(bytes[suffixBase + offset]) - wanted;
        if (comparison == 0) {
          comparison = suffixLength - 1;
        }
      }
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle - 1;
      } else {
        return checkedValueRef(middle, offset, end, suffixLength);
      }
    }
    return HOTLeafPage.NO_VALUE_REF;
  }

  int uniformSuffixLength() {
    return uniformSuffixLength;
  }

  /** Select from an immutable cache image after the whole body was validated. */
  long findValidatedValueRef(final byte[] key, final int commonPrefixLength) {
    final int wanted = key.length - commonPrefixLength;
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      final int middle = (low + high) >>> 1;
      final int offset = slotOffset(middle);
      final int suffixLength = lengthAt(slots + offset, keyWidth);
      final int groupLength = groupLength(middle);
      final int length = suffixLength + groupLength;
      final int prefix =
          compareKeyTail(middle, groupLength, offset + keyWidth, key, commonPrefixLength, Math.min(wanted, length));
      final int comparison = prefix == 0
          ? length - wanted
          : prefix;
      if (comparison < 0) {
        low = middle + 1;
      } else if (comparison > 0) {
        high = middle - 1;
      } else {
        return valueRef(middle, offset, suffixLength);
      }
    }
    return HOTLeafPage.NO_VALUE_REF;
  }

  public byte[] copyValue(final long ref) {
    final int length = (int) ref;
    final long from = base + slots + (ref >>> Integer.SIZE);
    if (array != null) {
      return Arrays.copyOfRange(array, (int) from, (int) from + length);
    }
    final byte[] result = new byte[length];
    MemorySegment.copy(image, ValueLayout.JAVA_BYTE, from, result, 0, length);
    return result;
  }

  private int slotEnd(final int index, final int offset) {
    final int end = index + 1 == count
        ? used
        : slotOffset(index + 1);
    requireSlotOrder(offset, end);
    return end;
  }

  /** An empty or inverted slot is refused by checkedSuffixLength, which every caller runs next. */
  private void requireSlotOrder(final int offset, final int end) {
    if (end > used) {
      throw malformed("slot order");
    }
  }

  private boolean isShared(final int index) {
    return bitmap != 0 && (unsignedByte(bitmap + (index >>> 3)) & (1 << (index & 7))) != 0;
  }

  private int lengthAt(final int at, final int width) {
    return width == 1
        ? unsignedByte(at)
        : u16(at);
  }

  private void copyRange(final int start, final MemorySegment to, final int offset, final int length) {
    if (array == null) {
      MemorySegment.copy(image, base + start, to, offset, length);
    } else {
      MemorySegment.copy(array, (int) base + start, to, ValueLayout.JAVA_BYTE, offset, length);
    }
  }

  private int u16(final int at) {
    return Short.toUnsignedInt(array == null
        ? SegmentAccess.getShortLE(image, base + at)
        : (short) ARRAY_SHORT_LE.get(array, (int) base + at));
  }

  private int unsignedByte(final int at) {
    return Byte.toUnsignedInt(array == null
        ? SegmentAccess.getByte(image, base + at)
        : array[(int) base + at]);
  }

  private short shortBE(final int at) {
    return array == null
        ? SegmentAccess.getShortBE(image, base + at)
        : (short) ARRAY_SHORT_BE.get(array, (int) base + at);
  }

  private int intBE(final int at) {
    return array == null
        ? SegmentAccess.getIntBE(image, base + at)
        : (int) ARRAY_INT_BE.get(array, (int) base + at);
  }

  private long longBE(final int at) {
    return array == null
        ? SegmentAccess.getLongBE(image, base + at)
        : (long) ARRAY_LONG_BE.get(array, (int) base + at);
  }

  private static IllegalStateException malformed(final String description) {
    return new IllegalStateException("Invalid packed HOT leaf " + description);
  }
}
