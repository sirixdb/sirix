/*
 * Copyright (c) 2024, Sirix Contributors
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

package io.sirix.node.json;

import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;

/**
 * Helper class for JSON node tests. Provides convenience methods for writing test data
 * in the correct serialization format.
 */
public final class JsonNodeTestHelper {

  private JsonNodeTestHelper() {
    // Utility class
  }

  /**
   * Write the full serialization header for test data: NodeKind byte, size placeholder (4 bytes),
   * and 3 bytes padding. This makes the total header 8 bytes (1 NodeKind + 4 size + 3 padding)
   * for 8-byte alignment.
   * 
   * @param data the output buffer
   * @param nodeKind the NodeKind enum value
   * @return the position where the size prefix was written (needed for finalizeSerialization)
   */
  public static long writeHeader(BytesOut<?> data, NodeKind nodeKind) {
    data.writeByte(nodeKind.getId()); // NodeKind byte
    return JsonNodeSerializer.writeSizePrefix(data);
  }

  /**
   * Convenience method to write end padding and update size prefix in one call.
   * 
   * @param data the output buffer
   * @param sizePos the position where the size prefix was written (returned by writeHeader)
   * @param startPos the position after the header where node data started
   */
  public static void finalizeSerialization(BytesOut<?> data, long sizePos, long startPos) {
    JsonNodeSerializer.writeEndPadding(data, startPos);
    JsonNodeSerializer.updateSizePrefix(data, sizePos, startPos);
  }
}