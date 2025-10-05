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
 * Helper class for JSON node tests to handle serialization format with size prefix and padding.
 */
public class JsonNodeTestHelper {

  /**
   * Write the serialization header: NodeKind byte, size placeholder (4 bytes), and 3 bytes padding.
   * This makes the total header 8 bytes (1 NodeKind + 4 size + 3 padding) for 8-byte alignment.
   * 
   * @param data the output buffer
   * @param nodeKind the NodeKind enum value
   * @return the position where the size prefix was written (needed for updateSizePrefix)
   */
  public static long writeHeader(BytesOut<?> data, NodeKind nodeKind) {
    data.writeByte(nodeKind.getId()); // NodeKind byte
    long sizePos = data.writePosition();
    data.writeInt(0); // Size placeholder
    data.writeByte((byte) 0); // 3 bytes padding (total header = 8 bytes with NodeKind)
    data.writeByte((byte) 0);
    data.writeByte((byte) 0);
    return sizePos;
  }

  /**
   * Write end padding to make the total node size a multiple of 8 bytes.
   * This ensures the next node will also be 8-byte aligned.
   * 
   * @param data the output buffer
   * @param startPos the position after writing the header where node data started
   */
  public static void writeEndPadding(BytesOut<?> data, long startPos) {
    long nodeDataSize = data.writePosition() - startPos;
    int remainder = (int)(nodeDataSize % 8);
    if (remainder != 0) {
      int padding = 8 - remainder;
      for (int i = 0; i < padding; i++) {
        data.writeByte((byte) 0);
      }
    }
  }

  /**
   * Update the size prefix that was written earlier in the header.
   * 
   * @param data the output buffer
   * @param sizePos the position where the size prefix was written (returned by writeHeader)
   * @param startPos the position after the header where node data started
   */
  public static void updateSizePrefix(BytesOut<?> data, long sizePos, long startPos) {
    long endPos = data.writePosition();
    long nodeDataSize = endPos - startPos;
    long currentPos = data.writePosition();
    data.writePosition(sizePos);
    data.writeInt((int) nodeDataSize);
    data.writePosition(currentPos);
  }

  /**
   * Convenience method to write end padding and update size prefix in one call.
   * 
   * @param data the output buffer
   * @param sizePos the position where the size prefix was written (returned by writeHeader)
   * @param startPos the position after the header where node data started
   */
  public static void finalizeSerialization(BytesOut<?> data, long sizePos, long startPos) {
    writeEndPadding(data, startPos);
    updateSizePrefix(data, sizePos, startPos);
  }
}
