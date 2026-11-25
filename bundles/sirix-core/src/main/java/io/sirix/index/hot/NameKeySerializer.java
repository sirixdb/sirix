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
import org.jspecify.annotations.Nullable;

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
 * no length of their own. A name inside the bound is stored and read exactly as before.
 */
public final class NameKeySerializer implements HOTKeySerializer<QNm> {
  private static final byte NAMESPACE_SENTINEL = (byte) 0xFF;

  /** Bytes the namespace framing costs: the sentinel plus the two-byte terminator. */
  private static final int NAMESPACE_FRAMING_BYTES = 3;

  private static final byte TRUNCATION_MARKER = (byte) 0xFF;

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
    final boolean localAscii = AsciiKeyBytes.isAsciiPrefix(localName, localName.length());
    final byte[] localBytes = localAscii
        ? null
        : localName.getBytes(StandardCharsets.UTF_8);
    final int localLength = localBytes == null
        ? localName.length()
        : localBytes.length;
    final boolean uriAscii = AsciiKeyBytes.isAsciiPrefix(uri, uri.length());
    final byte[] uriBytes = uriAscii
        ? null
        : uri.getBytes(StandardCharsets.UTF_8);
    final int uriLength = uriBytes == null
        ? uri.length()
        : uriBytes.length;
    int uriZeros = 0;
    for (int index = uri.indexOf(0); index >= 0; index = uri.indexOf(0, index + 1)) {
      uriZeros++;
    }
    final boolean truncated = (long) localLength + (uri.isEmpty()
        ? 0L
        : (long) NAMESPACE_FRAMING_BYTES + uriLength + uriZeros) > MAX_NAME_KEY_BYTES;
    int pos = offset;
    int budget = MAX_NAME_KEY_BYTES - (truncated
        ? 1
        : 0);
    if (!uri.isEmpty()) {
      final int firstLocalWidth = localBytes == null
          ? 1
          : utf8Width(localBytes[0] & 0xFF);
      final int uriBudget = budget - NAMESPACE_FRAMING_BYTES - firstLocalWidth;
      final int uriPrefix = prefixOrWhole(uri, uriBytes, uriLength, (long) uriLength + uriZeros, uriBudget, true);
      dest[pos++] = NAMESPACE_SENTINEL;
      final int start = pos;
      writeEncodedPrefix(uri, uriBytes, uriPrefix, dest, start);
      pos += uriPrefix;
      if (uriPrefix != uriLength) {
        uriZeros = 0;
        for (int index = start; index < pos; index++) {
          if (dest[index] == 0) {
            uriZeros++;
          }
        }
      }
      if (uriZeros != 0) {
        checkFromIndexSize(start, uriPrefix + uriZeros, dest.length);
        int target = pos + uriZeros;
        for (int source = pos - 1; source >= start; source--) {
          final byte value = dest[source];
          if (value == 0) {
            dest[--target] = NAMESPACE_SENTINEL;
          }
          dest[--target] = value;
        }
        pos += uriZeros;
      }
      dest[pos++] = 0;
      dest[pos++] = 0;
      budget -= NAMESPACE_FRAMING_BYTES + uriPrefix + uriZeros;
    }
    final int localPrefix = prefixOrWhole(localName, localBytes, localLength, localLength, budget, false);
    writeEncodedPrefix(localName, localBytes, localPrefix, dest, pos);
    pos += localPrefix;
    if (truncated) {
      dest[pos++] = TRUNCATION_MARKER;
    }
    return pos - offset;
  }

  public static boolean isTruncated(final byte[] bytes, final int offset, final int length) {
    requireNonNull(bytes, "Bytes cannot be null");
    checkFromIndexSize(offset, length, bytes.length);
    return length > 0 && bytes[offset + length - 1] == TRUNCATION_MARKER;
  }

  /**
   * The prefix of {@code value} this key stores: the whole of it when the {@code stored} bytes it
   * would occupy fit {@code budget}, and otherwise the longest whole-character prefix that does. One
   * decision for both components, so that a name inside the bound pays a comparison rather than a
   * walk over its characters.
   */
  private static int prefixOrWhole(final String value, final byte @Nullable [] encoded, final int length,
      final long stored, final int budget, final boolean escapeZeros) {
    return stored <= budget
        ? length
        : prefixWithinBudget(value, encoded, length, budget, escapeZeros);
  }

  private static int prefixWithinBudget(final String value, final byte @Nullable [] encoded, final int length,
      final int budget, final boolean escapeZeros) {
    int bytes = 0;
    int prefix = 0;
    while (prefix < length) {
      final int leadingByte = encoded == null
          ? value.charAt(prefix)
          : encoded[prefix] & 0xFF;
      final int width = utf8Width(leadingByte);
      final int storedWidth = width + (escapeZeros && leadingByte == 0
          ? 1
          : 0);
      if (bytes + storedWidth > budget) {
        break;
      }
      bytes += storedWidth;
      prefix += width;
    }
    return prefix;
  }

  private static int utf8Width(final int leadingByte) {
    if (leadingByte < 0x80) {
      return 1;
    }
    if (leadingByte < 0xE0) {
      return 2;
    }
    return leadingByte < 0xF0
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

  private static void writeEncodedPrefix(final String value, final byte @Nullable [] encoded, final int length,
      final byte[] dest, final int offset) {
    if (encoded == null) {
      AsciiKeyBytes.writeAsciiPrefix(value, length, dest, offset);
    } else {
      System.arraycopy(encoded, 0, dest, offset, length);
    }
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
    final int contentLength = length - (isTruncated(bytes, offset, length)
        ? 1
        : 0);
    if (contentLength == 0) {
      throw new IllegalArgumentException("Invalid QNm serialization: missing name");
    }
    if (bytes[offset] != NAMESPACE_SENTINEL) {
      return new QNm(new String(bytes, offset, contentLength, StandardCharsets.UTF_8));
    }
    final int end = offset + contentLength;
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
