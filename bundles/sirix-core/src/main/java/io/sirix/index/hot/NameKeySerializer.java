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

import static java.util.Objects.requireNonNull;

/**
 * Serializer for NAME index keys (qualified names as {@link QNm}).
 *
 * <p>
 * Serializes QNm to bytes using UTF-8 encoding. UTF-8 is already lexicographically ordered, so no
 * special encoding is needed for the local name.
 * </p>
 *
 * <h2>Format</h2>
 * <p>
 * Two formats depending on whether a namespace prefix is present:
 * </p>
 * <ul>
 * <li><b>No prefix (common case for JSON):</b> {@code [localName:M]} — raw UTF-8 bytes, zero
 * overhead</li>
 * <li><b>With prefix (XML namespaces):</b>
 * {@code [0xFF][prefixLen:1byte][nsPrefix:N][localName:M]}</li>
 * </ul>
 *
 * <p>
 * The sentinel byte {@code 0xFF} is safe because it is never produced by valid UTF-8 encoding
 * (valid start bytes range from 0x00-0x7F, 0xC2-0xF4; continuation bytes 0x80-0xBF). This
 * guarantees that all empty-prefix keys sort before all prefixed keys under unsigned byte
 * comparison, preserving the ordering contract.
 * </p>
 *
 * <h2>HOT Trie Optimization</h2>
 * <p>
 * The previous format {@code [0x00][localName]} wasted the first byte on a constant separator,
 * which prevented the discriminative bit computer from using byte 0 for trie discrimination. By
 * encoding the local name directly starting at byte 0, the trie structure maps to the actual key
 * content, avoiding degenerate splits.
 * </p>
 *
 * <h2>Zero Allocation</h2>
 * <p>
 * All methods write to caller-provided buffers. String.getBytes() is the only allocation, which is
 * unavoidable for variable-length strings.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class NameKeySerializer implements HOTKeySerializer<QNm> {

  /**
   * Sentinel byte indicating a namespace prefix follows. 0xFF is never a valid UTF-8 byte, so it
   * unambiguously marks the prefixed format.
   */
  private static final byte PREFIX_SENTINEL = (byte) 0xFF;

  /**
   * Singleton instance (stateless, thread-safe).
   */
  public static final NameKeySerializer INSTANCE = new NameKeySerializer();

  private NameKeySerializer() {
    // Singleton
  }

  @Override
  public int serialize(final QNm key, final byte[] dest, final int offset) {
    requireNonNull(key, "Key cannot be null");

    final String localName = key.getLocalName();
    if (localName == null || localName.isEmpty()) {
      throw new IllegalArgumentException("QNm local name cannot be null or empty");
    }

    final String prefix = key.getPrefix();
    final boolean hasPrefix = prefix != null && !prefix.isEmpty();

    int pos = offset;

    if (hasPrefix) {
      // Prefixed format: [0xFF][prefixLen:1][prefix:N][localName:M]
      final byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
      if (prefixBytes.length > 255) {
        throw new IllegalArgumentException("Namespace prefix too long: " + prefixBytes.length + " bytes (max 255)");
      }
      dest[pos++] = PREFIX_SENTINEL;
      dest[pos++] = (byte) prefixBytes.length;
      System.arraycopy(prefixBytes, 0, dest, pos, prefixBytes.length);
      pos += prefixBytes.length;
    }

    // Local name: raw UTF-8 bytes
    final byte[] localBytes = localName.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(localBytes, 0, dest, pos, localBytes.length);
    pos += localBytes.length;

    return pos - offset;
  }

  @Override
  public QNm deserialize(final byte[] bytes, final int offset, final int length) {
    if (length == 0) {
      throw new IllegalArgumentException("Invalid QNm serialization: zero length");
    }

    if ((bytes[offset] & 0xFF) == 0xFF) {
      // Prefixed format: [0xFF][prefixLen:1][prefix:N][localName:M]
      if (length < 3) {
        throw new IllegalArgumentException("Invalid prefixed QNm serialization: too short");
      }
      final int prefixLen = bytes[offset + 1] & 0xFF;
      final String prefix = new String(bytes, offset + 2, prefixLen, StandardCharsets.UTF_8);
      final int localOffset = offset + 2 + prefixLen;
      final int localLen = length - 2 - prefixLen;
      final String localName = new String(bytes, localOffset, localLen, StandardCharsets.UTF_8);
      return new QNm(null, prefix, localName);
    } else {
      // Unprefixed format: [localName:M]
      final String localName = new String(bytes, offset, length, StandardCharsets.UTF_8);
      return new QNm(null, "", localName);
    }
  }
}

