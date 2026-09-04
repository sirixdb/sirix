/**
 * Copyright (c) 2026.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * One value of a global projection value dictionary: the reverse (id &rarr; value) direction.
 *
 * <p>
 * The reverse-id radix maps a stable dictionary id to this record's opaque persistence key. Append
 * generations can therefore reserve dense key runs without renumbering any existing id. Keeping
 * each immutable value as its own record lets a lookup read only the radix path, its small value
 * bucket, and the page holding the requested value rather than materialising the dictionary.
 *
 * <p>
 * Immutable once written, for the same reason an FSST symbol table is
 * ({@link FsstSymbolTableNode}): row cells in every already-written row group refer to values by
 * id, so re-pointing an id at a different value would silently rewrite the meaning of data in
 * revisions that have already been committed. Ids are therefore minted monotonically and never
 * reused, and copy-on-write keeps each revision's view of the dictionary reachable from that
 * revision's root.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryEntryNode implements DataRecord {

  /**
   * Largest value payload admitted by V0. The minimum G1 region is 1 MiB and an object becomes
   * humongous at half a region; a 256 KiB payload plus its array header therefore retains almost
   * another 256 KiB of safety margin even under that smallest region configuration.
   */
  public static final int MAX_VALUE_LENGTH = 256 << 10;

  private final long nodeKey;

  /** The dictionary value, UTF-8 encoded. Empty is a legitimate value (the empty string). */
  private final byte[] value;

  /**
   * Constructor.
   *
   * @param nodeKey the positive persistence key reserved for this entry
   * @param value the UTF-8 encoded value; never {@code null}, possibly empty
   */
  public ValueDictionaryEntryNode(final long nodeKey, final byte[] value) {
    this(nodeKey, value, false);
  }

  private ValueDictionaryEntryNode(final long nodeKey, final byte[] value, final boolean takeOwnership) {
    if (nodeKey <= 0L) {
      throw new IllegalArgumentException("value dictionary entry key must be positive");
    }
    this.nodeKey = nodeKey;
    final byte[] checkedValue = requireNonNull(value, "value must not be null");
    if (checkedValue.length > MAX_VALUE_LENGTH) {
      throw new IllegalArgumentException(
          "value dictionary entry exceeds the safe V0 payload limit of " + MAX_VALUE_LENGTH + " bytes");
    }
    this.value = takeOwnership
        ? checkedValue
        : checkedValue.clone();
  }

  /**
   * Create an entry by transferring ownership of {@code value} to the immutable node.
   *
   * <p>
   * This is the allocation-free persistence seam for dictionary writers and deserializers that have
   * just produced a fresh byte array. The caller must neither retain nor mutate the array after this
   * call. Ordinary callers should use {@link #ValueDictionaryEntryNode(long, byte[])}, which
   * defensively copies its input.
   */
  public static ValueDictionaryEntryNode takeOwnership(final long nodeKey, final byte[] value) {
    return new ValueDictionaryEntryNode(nodeKey, value, true);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_ENTRY;
  }

  /**
   * The UTF-8 encoded value.
   *
   * @return a copy of the UTF-8 encoded value
   */
  public byte[] getValue() {
    return value.clone();
  }

  /**
   * UTF-16 collation of this value against a byte RANGE, exposing and copying neither side.
   *
   * <p>
   * The mixed case: a dictionary read view holds packed values as {@code (backing, offset, length)}
   * slices but keeps a spilled value as its RECORD, because a record's array is its own and must not
   * escape. Comparing the two shapes therefore needs an entry point on the node — which is why there
   * is no accessor here handing the internal array out.
   *
   * @param bytes backing array of the other value
   * @param offset start of the other value
   * @param length length of the other value
   * @return negative, zero or positive as THIS value orders before, with, or after the range
   */
  public int compareToRange(final byte[] bytes, final int offset, final int length) {
    requireNonNull(bytes, "bytes must not be null");
    Objects.checkFromIndexSize(offset, length, bytes.length);
    return compareUtf16Range(value, 0, value.length, bytes, offset, length);
  }

  /** Compare a caller-owned byte range without exposing or copying the stored bytes. */
  public boolean valueEquals(final byte[] candidate, final int offset, final int length) {
    requireNonNull(candidate, "candidate must not be null");
    Objects.checkFromIndexSize(offset, length, candidate.length);
    return Arrays.equals(value, 0, value.length, candidate, offset, offset + length);
  }

  /** Compare a caller-owned byte range to the stored bytes using unsigned byte ordering. */
  /**
   * Byte-wise substring containment of {@code needle} in this value — {@code fn:contains} semantics
   * (UTF-8 is self-synchronizing, so a byte-level needle match IS a codepoint substring match).
   *
   * <p>
   * An entry point rather than an accessor for the same reason as {@link #compareToRange}: a spilled
   * value's array is the record's own and must not escape.
   */
  public boolean containsNeedle(final byte[] needle, final int offset, final int length) {
    Objects.checkFromIndexSize(offset, length, needle.length);
    if (length == 0) {
      return true;
    }
    final byte[] hay = value;
    final int limit = hay.length - length;
    final byte first = needle[offset];
    for (int i = 0; i <= limit; i++) {
      if (hay[i] != first) {
        continue;
      }
      int j = 1;
      while (j < length && hay[i + j] == needle[offset + j]) {
        j++;
      }
      if (j == length) {
        return true;
      }
    }
    return false;
  }

  public int compareCandidateUnsigned(final byte[] candidate, final int offset, final int length) {
    requireNonNull(candidate, "candidate must not be null");
    Objects.checkFromIndexSize(offset, length, candidate.length);
    final int commonLength = Math.min(length, value.length);
    for (int index = 0; index < commonLength; index++) {
      final int comparison =
          Integer.compare(Byte.toUnsignedInt(candidate[offset + index]), Byte.toUnsignedInt(value[index]));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(length, value.length);
  }

  /**
   * Compare this UTF-8 value with another one under Java/Brackit's UTF-16 string order without
   * materialising either string.
   *
   * <p>
   * The distinction matters for supplementary characters: unsigned UTF-8 order follows Unicode scalar
   * values, while {@link String#compareTo(String)} compares UTF-16 code units. Dictionary ids are
   * first-seen ids and therefore carry no ordering information of their own.
   * </p>
   *
   * @param other the other immutable dictionary entry
   * @return a negative value, zero, or a positive value as this value is less than, equal to, or
   *         greater than {@code other}
   * @throws IllegalStateException if either persisted payload is not well-formed UTF-8
   */
  public int compareValueUtf16(final ValueDictionaryEntryNode other) {
    requireNonNull(other, "other must not be null");
    return compareUtf16Range(value, 0, value.length, other.value, 0, other.value.length);
  }

  /**
   * UTF-16 collation over two byte RANGES, so a caller holding packed values can compare in place.
   *
   * <p>
   * This is the identical decode-and-compare the instance form used; only the bounds moved from
   * {@code value.length} to explicit offset/limit pairs. Comparison operands are never materialised —
   * a dictionary scan compares far more values than it ever emits, so building a String or a
   * defensive copy per comparison is exactly the per-row garbage the packed layout exists to remove.
   *
   * @param left backing array of the left value
   * @param leftOffset start of the left value
   * @param leftLength length of the left value
   * @param right backing array of the right value
   * @param rightOffset start of the right value
   * @param rightLength length of the right value
   * @return negative, zero or positive as left orders before, with, or after right
   */
  public static int compareUtf16Range(final byte[] left, final int leftOffset, final int leftLength, final byte[] right,
      final int rightOffset, final int rightLength) {
    requireNonNull(left, "left must not be null");
    requireNonNull(right, "right must not be null");
    Objects.checkFromIndexSize(leftOffset, leftLength, left.length);
    Objects.checkFromIndexSize(rightOffset, rightLength, right.length);
    final int leftLimit = leftOffset + leftLength;
    final int rightLimit = rightOffset + rightLength;
    int thisOffset = leftOffset;
    int otherOffset = rightOffset;
    int thisPendingLowSurrogate = -1;
    int otherPendingLowSurrogate = -1;
    while (thisOffset < leftLimit || thisPendingLowSurrogate >= 0) {
      if (otherOffset >= rightLimit && otherPendingLowSurrogate < 0) {
        return 1;
      }

      final int thisUnit;
      if (thisPendingLowSurrogate >= 0) {
        thisUnit = thisPendingLowSurrogate;
        thisPendingLowSurrogate = -1;
      } else {
        final int codePoint = decodeCodePoint(left, thisOffset, leftLimit);
        thisOffset += utf8Width(codePoint);
        if (codePoint > Character.MAX_VALUE) {
          thisUnit = Character.highSurrogate(codePoint);
          thisPendingLowSurrogate = Character.lowSurrogate(codePoint);
        } else {
          thisUnit = codePoint;
        }
      }

      final int otherUnit;
      if (otherPendingLowSurrogate >= 0) {
        otherUnit = otherPendingLowSurrogate;
        otherPendingLowSurrogate = -1;
      } else {
        final int codePoint = decodeCodePoint(right, otherOffset, rightLimit);
        otherOffset += utf8Width(codePoint);
        if (codePoint > Character.MAX_VALUE) {
          otherUnit = Character.highSurrogate(codePoint);
          otherPendingLowSurrogate = Character.lowSurrogate(codePoint);
        } else {
          otherUnit = codePoint;
        }
      }

      final int comparison = Integer.compare(thisUnit, otherUnit);
      if (comparison != 0) {
        return comparison;
      }
    }
    return otherOffset >= rightLimit && otherPendingLowSurrogate < 0
        ? 0
        : -1;
  }


  /**
   * Allocation-free {@code xs:integer(substring(value, start, length))} for the admitted ASCII
   * transform used by projection grouping.
   *
   * @return the parsed value, or {@link Long#MIN_VALUE} when the generic evaluator must handle the
   *         input (non-ASCII, an empty/invalid slice, or a value outside the signed-long domain)
   */
  public long xsIntegerOfSubstring(final int start, final int length) {
    if (start < 1 || length < 0) {
      return Long.MIN_VALUE;
    }
    for (final byte current : value) {
      if (current < 0) {
        return Long.MIN_VALUE;
      }
    }
    final int startOffset = start - 1;
    int from = Math.min(startOffset, value.length);
    int end = (int) Math.min(value.length, (long) startOffset + length);
    while (from < end && isXmlWhitespace(value[from])) {
      from++;
    }
    while (end > from && isXmlWhitespace(value[end - 1])) {
      end--;
    }
    if (from >= end) {
      return Long.MIN_VALUE;
    }
    boolean negative = false;
    if (value[from] == '+' || value[from] == '-') {
      negative = value[from] == '-';
      from++;
    }
    if (from >= end) {
      return Long.MIN_VALUE;
    }
    long parsed = 0L;
    for (int offset = from; offset < end; offset++) {
      final int digit = value[offset] - '0';
      if (digit < 0 || digit > 9) {
        return Long.MIN_VALUE;
      }
      if (parsed > Long.MAX_VALUE / 10L || (parsed == Long.MAX_VALUE / 10L && digit > Long.MAX_VALUE % 10L)) {
        return Long.MIN_VALUE;
      }
      parsed = parsed * 10L + digit;
    }
    return negative
        ? -parsed
        : parsed;
  }

  /**
   * Pack an ASCII ISO-minute substring as {@code yyyyMMddHHmm + 1}, preserving lexical order.
   *
   * @return the positive packed key, or {@link Long#MIN_VALUE} when the exact transform is not
   *         admissible
   */
  public long packIsoMinuteSubstring(final int start, final int length) {
    if (start < 1 || length != 16) {
      return Long.MIN_VALUE;
    }
    for (final byte current : value) {
      if (current < 0) {
        return Long.MIN_VALUE;
      }
    }
    final int offset = start - 1;
    if ((long) offset + 16L > value.length || value[offset + 4] != '-' || value[offset + 7] != '-'
        || value[offset + 10] != 'T' || value[offset + 13] != ':') {
      return Long.MIN_VALUE;
    }
    long packed = 0L;
    for (final int digitOffset : ISO_MINUTE_DIGIT_OFFSETS) {
      final int digit = value[offset + digitOffset] - '0';
      if (digit < 0 || digit > 9) {
        return Long.MIN_VALUE;
      }
      packed = packed * 10L + digit;
    }
    return packed + 1L;
  }

  /** Materialise a previously validated ASCII substring; intended only for emitted winners. */
  public String materializeAsciiSubstring(final int start, final int length) {
    if (packIsoMinuteSubstring(start, length) == Long.MIN_VALUE) {
      throw new IllegalArgumentException("dictionary value is not an admissible ISO-minute substring");
    }
    return new String(value, start - 1, length, StandardCharsets.US_ASCII);
  }

  private static boolean isXmlWhitespace(final byte current) {
    return current == ' ' || current == '\t' || current == '\n' || current == '\r';
  }

  private static int utf8Width(final int codePoint) {
    if (codePoint <= 0x7F) {
      return 1;
    }
    if (codePoint <= 0x7FF) {
      return 2;
    }
    if (codePoint <= Character.MAX_VALUE) {
      return 3;
    }
    return 4;
  }

  private static int decodeCodePoint(final byte[] bytes, final int offset) {
    return decodeCodePoint(bytes, offset, bytes.length);
  }

  /**
   * Decode one code point, refusing any sequence that would read past {@code limit}.
   *
   * <p>
   * The limit is not decoration. Packed dictionary values share ONE backing array, so a value whose
   * last byte is a multi-byte lead — corrupt data, or a truncated slice — would otherwise consume
   * continuation bytes belonging to the NEXT value and compare against bytes that are not part of the
   * operand at all. Before packing, every value owned its own array and the array bound was the value
   * bound; now they differ and only the explicit limit is correct.
   */
  private static int decodeCodePoint(final byte[] bytes, final int offset, final int limit) {
    final int first = Byte.toUnsignedInt(bytes[offset]);
    if (first <= 0x7F) {
      return first;
    }
    final int width = first >= 0xF0
        ? 4
        : first >= 0xE0
            ? 3
            : 2;
    if (offset + width > limit) {
      throw new IllegalStateException("truncated UTF-8 sequence in value dictionary entry");
    }
    if (first >= 0xC2 && first <= 0xDF) {
      requireContinuation(bytes, offset, 1);
      return (first & 0x1F) << 6 | bytes[offset + 1] & 0x3F;
    }
    if (first >= 0xE0 && first <= 0xEF) {
      requireContinuation(bytes, offset, 2);
      final int second = Byte.toUnsignedInt(bytes[offset + 1]);
      if ((first == 0xE0 && second < 0xA0) || (first == 0xED && second >= 0xA0)) {
        throw new IllegalStateException("invalid UTF-8 in value dictionary entry");
      }
      return (first & 0x0F) << 12 | (second & 0x3F) << 6 | bytes[offset + 2] & 0x3F;
    }
    if (first >= 0xF0 && first <= 0xF4) {
      requireContinuation(bytes, offset, 3);
      final int second = Byte.toUnsignedInt(bytes[offset + 1]);
      if ((first == 0xF0 && second < 0x90) || (first == 0xF4 && second > 0x8F)) {
        throw new IllegalStateException("invalid UTF-8 in value dictionary entry");
      }
      return (first & 0x07) << 18 | (second & 0x3F) << 12 | (bytes[offset + 2] & 0x3F) << 6 | bytes[offset + 3] & 0x3F;
    }
    throw new IllegalStateException("invalid UTF-8 in value dictionary entry");
  }

  private static void requireContinuation(final byte[] bytes, final int offset, final int continuationBytes) {
    if ((long) offset + continuationBytes >= bytes.length) {
      throw new IllegalStateException("truncated UTF-8 in value dictionary entry");
    }
    for (int index = 1; index <= continuationBytes; index++) {
      if ((bytes[offset + index] & 0xC0) != 0x80) {
        throw new IllegalStateException("invalid UTF-8 continuation in value dictionary entry");
      }
    }
  }

  private static final int[] ISO_MINUTE_DIGIT_OFFSETS = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15};

  /** Number of UTF-8 bytes retained by this entry. */
  /** Codepoint count of this value — non-continuation UTF-8 bytes; no array escapes. */
  public int codePointLength() {
    int codePoints = 0;
    for (final byte b : value) {
      if ((b & 0xC0) != 0x80) {
        codePoints++;
      }
    }
    return codePoints;
  }

  public int getValueLength() {
    return value.length;
  }

  /** Package-private, allocation-free serialization seam for the owning node package. */
  byte[] borrowedValueForSerialization() {
    return value;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryEntryNode other && Arrays.equals(value, other.value);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("nodeKey", nodeKey).add("valueBytes", value.length).toString();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }
}
