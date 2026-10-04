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
 * Canonical NAME keys identify expanded names: namespace URI plus local name, never the prefix.
 *
 * <p>
 * Namespace-free keys (including JSON field names) remain raw UTF-8 local names, with no overhead.
 * Namespaced keys are {@code [0xFF][uri UTF-8][0x00,0x00][local UTF-8]}. Within the URI, a NUL byte
 * is escaped as {@code [0x00,0xFF]}, so component boundaries are unambiguous even for
 * programmatically supplied URIs. The sentinel cannot occur in valid UTF-8. URI terminators sort
 * before extensions, preserving namespace/local byte ordering. Prefix aliases serialize
 * identically.
 *
 * <p>
 * ASCII components write directly into the caller's reusable buffer. Deserialization returns an
 * empty prefix because prefixes are not part of the index's identity or durable key format.
 */
public final class NameKeySerializer implements HOTKeySerializer<QNm> {
  private static final byte NAMESPACE_SENTINEL = (byte) 0xFF;

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
    if (!uri.isEmpty()) {
      dest[pos++] = NAMESPACE_SENTINEL;
      final int start = pos;
      final int uriLength = writeUtf8(uri, dest, start);
      pos += uriLength;
      // Only NUL needs escaping. Expand backwards in the caller buffer, with no temporary array.
      int zeros = 0;
      for (int i = uri.indexOf(0); i >= 0; i = uri.indexOf(0, i + 1)) {
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
    }
    pos += writeUtf8(localName, dest, pos);
    return pos - offset;
  }

  private static String checkedLocalName(final QNm key) {
    final String localName = key.getLocalName();
    if (localName == null || localName.isEmpty()) {
      throw new IllegalArgumentException("QNm local name cannot be null or empty");
    }
    return localName;
  }

  private static int writeUtf8(final String value, final byte[] dest, final int offset) {
    if (AsciiKeyBytes.isAsciiPrefix(value, value.length())) {
      return AsciiKeyBytes.writeAsciiPrefix(value, value.length(), dest, offset);
    }
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
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
    if (bound > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("QNm too long to serialize: " + bound + " bytes");
    }
    return (int) bound;
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
