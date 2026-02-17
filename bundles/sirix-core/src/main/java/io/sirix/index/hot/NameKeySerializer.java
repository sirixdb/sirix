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
 * Serializes QNm to bytes using UTF-8 encoding of the local name. UTF-8 is already
 * lexicographically ordered, so no special encoding is needed.
 * </p>
 *
 * <p>
 * Format: {@code [nsPrefix:N][0x00][localName:M]}
 * </p>
 * <ul>
 * <li>Namespace prefix (UTF-8, variable length)</li>
 * <li>Separator byte (0x00)</li>
 * <li>Local name (UTF-8, variable length)</li>
 * </ul>
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
   * Separator byte between namespace prefix and local name.
   */
  private static final byte SEPARATOR = 0x00;

  /**
   * Singleton instance (stateless, thread-safe).
   */
  public static final NameKeySerializer INSTANCE = new NameKeySerializer();

  private NameKeySerializer() {
    // Singleton
  }

  @Override
  public int serialize(QNm key, byte[] dest, int offset) {
    requireNonNull(key, "Key cannot be null");
    int start = offset;

    // 1. Namespace prefix (may be empty)
    String prefix = key.getPrefix();
    if (prefix != null && !prefix.isEmpty()) {
      byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(prefixBytes, 0, dest, offset, prefixBytes.length);
      offset += prefixBytes.length;
    }

    // 2. Separator
    dest[offset++] = SEPARATOR;

    // 3. Local name
    String localName = key.getLocalName();
    if (localName == null || localName.isEmpty()) {
      throw new IllegalArgumentException("QNm local name cannot be null or empty");
    }
    byte[] localBytes = localName.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(localBytes, 0, dest, offset, localBytes.length);
    offset += localBytes.length;

    return offset - start;
  }

  @Override
  public QNm deserialize(byte[] bytes, int offset, int length) {
    // Find separator
    int separatorPos = -1;
    for (int i = offset; i < offset + length; i++) {
      if (bytes[i] == SEPARATOR) {
        separatorPos = i;
        break;
      }
    }

    if (separatorPos == -1) {
      throw new IllegalArgumentException("Invalid QNm serialization: no separator found");
    }

    // Extract prefix
    String prefix = "";
    int prefixLen = separatorPos - offset;
    if (prefixLen > 0) {
      prefix = new String(bytes, offset, prefixLen, StandardCharsets.UTF_8);
    }

    // Extract local name
    int localOffset = separatorPos + 1;
    int localLen = length - (separatorPos - offset) - 1;
    String localName = new String(bytes, localOffset, localLen, StandardCharsets.UTF_8);

    // QNm with null namespace URI (we don't store it, just prefix:localName)
    return new QNm(null, prefix, localName);
  }
}

