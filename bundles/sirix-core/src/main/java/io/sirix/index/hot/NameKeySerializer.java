/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/**
 * The durable NAME key format is specified in {@code docs/HOT_INDEX_SPECIFICATION.md}, section
 * 2.3.2; matching semantics are documented in the README's Indexes section.
 *
 * <p>
 * The sentinel cannot occur in valid UTF-8. URI escaping keeps component boundaries unambiguous
 * even for programmatically supplied URIs; terminators sort before URI extensions.
 *
 * <p>
 * ASCII components write directly into the caller's reusable buffer to avoid temporary arrays on
 * the per-posting serialization path.
 *
 * <p>
 * A name is bounded by {@link #MAX_NAME_KEY_BYTES} (FMT-305): HOT routing can only address the
 * discriminative bytes of a key inside {@link HOTKeySerializer#ROUTABLE_KEY_BYTES}, and names have
 * no length of their own. A longer name is stored under the longest prefix of its components that
 * fits, so its postings are candidates that the reader re-checks against each node's actual name.
 * Truncation happens at a character boundary, so the stored key stays a well-formed key of a
 * shortened name and the order of two keys still follows the order of their names. A name inside the
 * bound is stored and read exactly as before and pays nothing for this.
 */
public final class NameKeySerializer implements HOTKeySerializer<QNm> {
  private static final byte NAMESPACE_SENTINEL = (byte) 0xFF;

  /** Bytes the namespace framing costs: the sentinel plus the two-byte terminator. */
  private static final int NAMESPACE_FRAMING_BYTES = 3;

  /** Longest UTF-8 encoding of one Java character pair, and so of one truncation step. */
  private static final int MAX_UTF8_CHAR_BYTES = 4;

  /**
   * The longest stored NAME key (FMT-305): the routable bound less the four-byte chunk index and the
   * four-byte posting-delta suffix a stored key may carry after it.
   */
  public static final int MAX_NAME_KEY_BYTES =
      HOTKeySerializer.ROUTABLE_KEY_BYTES - HOTKeySerializer.CHUNK_IDX_BYTES - PostingDeltas.SUFFIX_BYTES;

  public static final NameKeySerializer INSTANCE = new NameKeySerializer();

  private NameKeySerializer() {}

  @Override
  public int serialize(final QNm key, final byte[] dest, final int offset) {
    requireNonNull(key, "Key cannot be null");
    requireNonNull(dest, "Destination cannot be null");
    checkFromIndexSize(offset, 0, dest.length);
    final String localName = checkedLocalName(key);
    final String uri = key.getNamespaceURI();
    int pos = offset;
    int budget = MAX_NAME_KEY_BYTES;
    if (!uri.isEmpty()) {
      // One character's worth of the budget is kept back so the local name cannot come out empty,
      // which is not a key this serializer can read back — and one character can be four bytes.
      final int uriChars = charsWithinBudget(uri, budget - NAMESPACE_FRAMING_BYTES - MAX_UTF8_CHAR_BYTES, true);
      dest[pos++] = NAMESPACE_SENTINEL;
      final int start = pos;
      final int uriLength = writeUtf8(uri, uriChars, dest, start);
      pos += uriLength;
      // Only NUL needs escaping. Expand backwards in the caller buffer, with no temporary array.
      int zeros = 0;
      for (int i = uri.indexOf(0); i >= 0 && i < uriChars; i = uri.indexOf(0, i + 1)) {
        zeros++;
      }
      if (zeros != 0) {
        checkFromIndexSize(start, uriLength + zeros, dest.length);
        int target = pos + zeros;
        for (int source = pos - 1; source >= start; source--) {
          final byte value = dest[source];
          if (value == 0) {
            dest[--target] = NAMESPACE_SENTINEL;
          }
          dest[--target] = value;
        }
        pos += zeros;
      }
      dest[pos++] = 0;
      dest[pos++] = 0;
      budget -= NAMESPACE_FRAMING_BYTES + uriLength + zeros;
    }
    pos += writeUtf8(localName, charsWithinBudget(localName, budget, false), dest, pos);
    return pos - offset;
  }

  /**
   * The length {@link #serialize} would return for {@code key} — what the index stores for it.
   *
   * @param key the name
   * @return its stored key length in bytes, never above {@link #MAX_NAME_KEY_BYTES}
   */
  public static int serializedLength(final QNm key) {
    requireNonNull(key, "Key cannot be null");
    final String localName = checkedLocalName(key);
    final String uri = key.getNamespaceURI();
    int budget = MAX_NAME_KEY_BYTES;
    int length = 0;
    if (!uri.isEmpty()) {
      final int uriChars = charsWithinBudget(uri, budget - NAMESPACE_FRAMING_BYTES - MAX_UTF8_CHAR_BYTES, true);
      final int uriBytes = escapedUtf8Length(uri, uriChars);
      length += NAMESPACE_FRAMING_BYTES + uriBytes;
      budget -= NAMESPACE_FRAMING_BYTES + uriBytes;
    }
    return length + utf8Length(localName, charsWithinBudget(localName, budget, false));
  }

  /**
   * Whether {@code key} is longer than the bound and therefore stored under a prefix of itself, so
   * its postings are candidates an exact read has to re-check (FMT-305).
   *
   * @param key the name a lookup asks for
   * @return {@code true} iff its stored key is a prefix rather than the whole name
   */
  public static boolean isTruncated(final QNm key) {
    requireNonNull(key, "Key cannot be null");
    final String localName = checkedLocalName(key);
    final String uri = key.getNamespaceURI();
    if (certainlyWithin(localName, uri, MAX_NAME_KEY_BYTES)) {
      return false; // an ordinary name decides this without walking its characters
    }
    final long full = uri.isEmpty()
        ? utf8Length(localName, localName.length())
        : (long) NAMESPACE_FRAMING_BYTES + escapedUtf8Length(uri, uri.length())
            + utf8Length(localName, localName.length());
    return full > MAX_NAME_KEY_BYTES;
  }

  /**
   * Whether {@code key}'s stored key may be the shared prefix of several names, and so whether a
   * read that answers for a name has to re-check its candidates (FMT-305). Decided in constant time
   * for any name short enough that the question cannot arise.
   *
   * @param key the name whose stored key is in question
   * @return {@code true} iff its stored key could stand for more than one name
   */
  public static boolean mayBeTruncated(final QNm key) {
    requireNonNull(key, "Key cannot be null");
    final String localName = checkedLocalName(key);
    final String uri = key.getNamespaceURI();
    return !certainlyWithin(localName, uri, MAX_NAME_KEY_BYTES - MAX_UTF8_CHAR_BYTES)
        && mayBeTruncated(serializedLength(key));
  }

  /**
   * Whether the key of these components is certainly no longer than {@code limit}, judged from
   * character counts alone: three bytes bound one character's UTF-8 encoding, surrogate pairs
   * included, and an escaped NUL is two bytes, which that bound already covers.
   */
  private static boolean certainlyWithin(final String localName, final String uri, final int limit) {
    return 3L * localName.length() + (uri.isEmpty()
        ? 0L
        : NAMESPACE_FRAMING_BYTES + 3L * uri.length()) <= limit;
  }

  /**
   * Whether a stored key of this length may be the truncated prefix of a longer name. Truncation
   * stops within one character of the bound, so every truncated key is this long; a name that ends
   * exactly here is not truncated and only pays a re-check that keeps all of its postings.
   *
   * @param storedKeyLength length of a stored NAME key, chunk index and delta suffix excluded
   * @return {@code true} iff the key could be a prefix of a longer name
   */
  public static boolean mayBeTruncated(final int storedKeyLength) {
    return storedKeyLength > MAX_NAME_KEY_BYTES - MAX_UTF8_CHAR_BYTES;
  }

  /**
   * The number of leading characters of {@code value} whose encoding fits {@code budget} bytes,
   * never splitting a surrogate pair. Truncating on a character boundary keeps the stored key a
   * readable key and keeps key order following name order, because the first byte two keys differ in
   * is either inside both prefixes or past the end of the shorter one.
   */
  private static int charsWithinBudget(final String value, final int budget, final boolean escapeZeros) {
    final int length = value.length();
    if (budget >= (escapeZeros
        ? escapedUtf8Length(value, length)
        : utf8Length(value, length))) {
      return length;
    }
    int bytes = 0;
    int chars = 0;
    while (chars < length) {
      final int codePoint = value.codePointAt(chars);
      final int width = utf8Width(codePoint) + (escapeZeros && codePoint == 0
          ? 1
          : 0);
      if (bytes + width > budget) {
        break;
      }
      bytes += width;
      chars += Character.charCount(codePoint);
    }
    return chars;
  }

  /** UTF-8 length of the first {@code chars} characters of {@code value}. */
  private static int utf8Length(final String value, final int chars) {
    int bytes = 0;
    for (int index = 0; index < chars;) {
      final int codePoint = value.codePointAt(index);
      bytes += utf8Width(codePoint);
      index += Character.charCount(codePoint);
    }
    return bytes;
  }

  /** As {@link #utf8Length}, plus one byte for every NUL the namespace escaping expands. */
  private static int escapedUtf8Length(final String value, final int chars) {
    int bytes = utf8Length(value, chars);
    for (int index = value.indexOf(0); index >= 0 && index < chars; index = value.indexOf(0, index + 1)) {
      bytes++;
    }
    return bytes;
  }

  private static int utf8Width(final int codePoint) {
    if (codePoint < 0x80) {
      return 1;
    }
    if (codePoint < 0x800) {
      return 2;
    }
    return codePoint < 0x10000
        ? 3
        : 4;
  }

  private static String checkedLocalName(final QNm key) {
    final String localName = key.getLocalName();
    if (localName == null || localName.isEmpty()) {
      throw new IllegalArgumentException("QNm local name cannot be null or empty");
    }
    return localName;
  }

  /** Write the first {@code chars} characters of {@code value} as UTF-8 and return the bytes written. */
  private static int writeUtf8(final String value, final int chars, final byte[] dest, final int offset) {
    if (AsciiKeyBytes.isAsciiPrefix(value, chars)) {
      return AsciiKeyBytes.writeAsciiPrefix(value, chars, dest, offset);
    }
    final byte[] bytes = (chars == value.length()
        ? value
        : value.substring(0, chars)).getBytes(StandardCharsets.UTF_8);
    System.arraycopy(bytes, 0, dest, offset, bytes.length);
    return bytes.length;
  }

  @Override
  public int maxSerializedLength(final QNm key) {
    requireNonNull(key, "Key cannot be null");
    final String localName = checkedLocalName(key);
    final String uri = key.getNamespaceURI();
    // Three bytes per UTF-16 char bounds UTF-8, including surrogate pairs. A NUL is two
    // escaped bytes, already covered by that bound. No prefix bytes are ever stored.
    final long bound = 3L * localName.length() + (uri.isEmpty()
        ? 0L
        : 3L + 3L * uri.length());
    // Serialization clamps to the routable bound (FMT-305), so that is the real ceiling whenever
    // the unclamped estimate is larger. Reporting it keeps a caller's buffer from being sized for a
    // name no key ever stores in full.
    return (int) Math.min(bound, MAX_NAME_KEY_BYTES);
  }

  @Override
  public QNm deserialize(final byte[] bytes, final int offset, final int length) {
    requireNonNull(bytes, "Bytes cannot be null");
    checkFromIndexSize(offset, length, bytes.length);
    if (length == 0) {
      throw new IllegalArgumentException("Invalid QNm serialization: zero length");
    }
    if (bytes[offset] != NAMESPACE_SENTINEL) {
      return new QNm(new String(bytes, offset, length, StandardCharsets.UTF_8));
    }
    final int end = offset + length;
    final int start = offset + 1;
    int escapes = 0;
    for (int pos = start; pos < end - 1; pos++) {
      if (bytes[pos] != 0) {
        continue;
      }
      final byte next = bytes[pos + 1];
      if (next == NAMESPACE_SENTINEL) {
        escapes++;
        pos++;
        continue;
      }
      if (next != 0 || pos == start || pos + 2 == end) {
        throw new IllegalArgumentException("Invalid namespaced QNm serialization");
      }
      final String uri;
      if (escapes == 0) {
        uri = new String(bytes, start, pos - start, StandardCharsets.UTF_8);
      } else {
        final byte[] unescaped = new byte[pos - start - escapes];
        int target = 0;
        for (int source = start; source < pos; source++) {
          final byte value = bytes[source];
          unescaped[target++] = value;
          if (value == 0) {
            source++;
          }
        }
        uri = new String(unescaped, StandardCharsets.UTF_8);
      }
      return new QNm(uri, "", new String(bytes, pos + 2, end - pos - 2, StandardCharsets.UTF_8));
    }
    throw new IllegalArgumentException("Invalid namespaced QNm serialization: missing URI terminator");
  }
}
