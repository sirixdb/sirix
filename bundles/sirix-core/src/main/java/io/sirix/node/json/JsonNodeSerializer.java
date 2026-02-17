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

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;

/**
 * Utility class for JSON node serialization with size prefix and padding for 8-byte alignment.
 * 
 * <p>
 * The serialization format ensures that node data starts at 8-byte aligned offsets for efficient
 * MemorySegment access with VarHandles:
 * 
 * <pre>
 * [4-byte size][3-byte padding][node data...][end padding]
 * </pre>
 * 
 * <p>
 * The total header (size + padding) is 7 bytes, but when combined with the 1-byte NodeKind that
 * precedes it, the total is 8 bytes, ensuring the node data starts at an 8-byte boundary.
 */
public final class JsonNodeSerializer {

  private JsonNodeSerializer() {
    // Utility class
  }

  /**
   * Write a placeholder for the node size (4 bytes), plus 3 bytes padding. When combined with the
   * 1-byte NodeKind that precedes this, the total header is 8 bytes, ensuring node data starts at an
   * 8-byte aligned offset.
   * 
   * @param sink the output sink
   * @return the position where size was written so it can be updated later
   */
  public static long writeSizePrefix(final BytesOut<?> sink) {
    long sizePos = sink.writePosition();
    sink.writeInt(0); // Size placeholder (will be updated later)
    // Write 3 bytes padding to make total header = 8 bytes (with 1-byte NodeKind)
    sink.writeByte((byte) 0);
    sink.writeByte((byte) 0);
    sink.writeByte((byte) 0);
    return sizePos;
  }

  /**
   * Update the size prefix that was written earlier.
   * 
   * @param sink the output sink
   * @param sizePos the position where the size prefix was written
   * @param startPos the position after the size prefix where node data started
   */
  public static void updateSizePrefix(final BytesOut<?> sink, final long sizePos, final long startPos) {
    long endPos = sink.writePosition();
    long nodeDataSize = endPos - startPos;
    long currentPos = sink.writePosition();
    sink.writePosition(sizePos);
    sink.writeInt((int) nodeDataSize);
    sink.writePosition(currentPos);
  }

  /**
   * Read the node size prefix (4 bytes) and skip 3 bytes padding.
   * 
   * @param source the input source
   * @return the size of the node data (not including size prefix or padding)
   */
  public static int readSizePrefix(final BytesIn<?> source) {
    int size = source.readInt(); // Read 4-byte size
    source.position(source.position() + 3); // Skip 3-byte padding
    return size;
  }

  /**
   * Write padding bytes at the end to make total node size a multiple of 8. This ensures the NEXT
   * node will also be 8-byte aligned.
   * 
   * @param sink the output sink
   * @param startPos the position after writing the size prefix where node data started
   */
  public static void writeEndPadding(final BytesOut<?> sink, final long startPos) {
    long currentPos = sink.writePosition();
    long nodeDataSize = currentPos - startPos;
    int remainder = (int) (nodeDataSize % 8);
    if (remainder != 0) {
      int padding = 8 - remainder;
      for (int i = 0; i < padding; i++) {
        sink.writeByte((byte) 0);
      }
    }
  }

  /**
   * Calculate padding bytes needed at the end to make size a multiple of 8.
   * 
   * @param size the current size
   * @return number of padding bytes needed (0-7)
   */
  public static int calculateEndPadding(final long size) {
    int remainder = (int) (size % 8);
    return remainder == 0
        ? 0
        : 8 - remainder;
  }
}
