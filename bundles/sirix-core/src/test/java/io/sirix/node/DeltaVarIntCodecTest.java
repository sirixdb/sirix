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

package io.sirix.node;

import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link DeltaVarIntCodec} MemorySegment support.
 * Verifies zero-allocation reading from MemorySegment.
 *
 * @author Johannes Lichtenberger
 */
class DeltaVarIntCodecTest {

  /**
   * Test reading unsigned varints from MemorySegment.
   */
  @Test
  void testReadVarLongFromSegment() {
    long[] values = {0, 1, 127, 128, 255, 256, 16383, 16384, 100000};
    
    for (long value : values) {
      // Encode using existing method
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      writeVarLong(bytesOut, value);
      
      // Get the data as byte array
      byte[] data = bytesOut.toByteArray();
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        
        // Decode from MemorySegment
        long decoded = DeltaVarIntCodec.readVarLongFromSegment(segment, 0);
        assertEquals(value, decoded, "VarLong round-trip failed for value: " + value);
      }
    }
  }
  
  /**
   * Test delta-encoded key decoding from MemorySegment.
   */
  @Test
  void testDecodeDeltaFromSegment() {
    long[][] testCases = {
      {100, 101},    // delta +1 (typical sibling)
      {100, 99},     // delta -1
      {1000, 1010},  // delta +10
      {1000, 990},   // delta -10
    };
    
    for (long[] testCase : testCases) {
      long baseKey = testCase[0];
      long targetKey = testCase[1];
      
      // Encode using existing method
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeDelta(bytesOut, targetKey, baseKey);
      
      // Get the data as byte array
      byte[] data = bytesOut.toByteArray();
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        
        // Decode from MemorySegment
        long decoded = DeltaVarIntCodec.decodeDeltaFromSegment(segment, 0, baseKey);
        assertEquals(targetKey, decoded, 
            "Delta decode failed for base=" + baseKey + ", target=" + targetKey);
      }
    }
  }
  
  /**
   * Test NULL key decoding from MemorySegment.
   */
  @Test
  void testDecodeDeltaFromSegmentNullKey() {
    long baseKey = 100;
    long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    
    // Encode NULL key
    BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
    DeltaVarIntCodec.encodeDelta(bytesOut, nullKey, baseKey);
    
    // Get the data as byte array
    byte[] data = bytesOut.toByteArray();
    
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment segment = arena.allocate(data.length);
      MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
      
      // Decode from MemorySegment
      long decoded = DeltaVarIntCodec.decodeDeltaFromSegment(segment, 0, baseKey);
      assertEquals(nullKey, decoded, "NULL key decode failed");
    }
  }
  
  /**
   * Test signed integer decoding from MemorySegment.
   */
  @Test
  void testDecodeSignedFromSegment() {
    int[] values = {0, 1, -1, 127, -128, 1000, -1000};
    
    for (int value : values) {
      // Encode using existing method
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeSigned(bytesOut, value);
      
      // Get the data as byte array
      byte[] data = bytesOut.toByteArray();
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        
        // Decode from MemorySegment
        int decoded = DeltaVarIntCodec.decodeSignedFromSegment(segment, 0);
        assertEquals(value, decoded, "Signed int decode failed for value: " + value);
      }
    }
  }
  
  /**
   * Test varint length calculation from MemorySegment.
   */
  @Test
  void testVarintLength() {
    long[] values = {0, 1, 127, 128, 255, 256, 16383, 16384, 100000};
    
    for (long value : values) {
      // Encode using existing method
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      writeVarLong(bytesOut, value);
      
      // Get the data as byte array
      byte[] data = bytesOut.toByteArray();
      int expectedLength = data.length;
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        
        // Calculate length from MemorySegment
        int length = DeltaVarIntCodec.varintLength(segment, 0);
        assertEquals(expectedLength, length, "Varint length mismatch for value: " + value);
      }
    }
  }
  
  /**
   * Test delta length calculation from MemorySegment.
   */
  @Test
  void testDeltaLength() {
    long[][] testCases = {
      {100, 101},
      {100, 99},
      {100, Fixed.NULL_NODE_KEY.getStandardProperty()},  // NULL_NODE_KEY
      {1000, 1010},
    };
    
    for (long[] testCase : testCases) {
      long baseKey = testCase[0];
      long targetKey = testCase[1];
      
      // Encode using existing method
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeDelta(bytesOut, targetKey, baseKey);
      
      // Get the data as byte array
      byte[] data = bytesOut.toByteArray();
      int expectedLength = data.length;
      
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(data.length);
        MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
        
        // Calculate length from MemorySegment
        int length = DeltaVarIntCodec.deltaLength(segment, 0);
        assertEquals(expectedLength, length, 
            "Delta length mismatch for base=" + baseKey + ", target=" + targetKey);
      }
    }
  }
  
  /**
   * Test reading 8-byte long from MemorySegment.
   */
  @Test
  void testReadLongFromSegment() {
    long[] values = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 0x123456789ABCDEF0L};
    
    for (long value : values) {
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(8);
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value);
        
        long decoded = DeltaVarIntCodec.readLongFromSegment(segment, 0);
        assertEquals(value, decoded, "Long read failed for value: " + value);
      }
    }
  }
  
  /**
   * Test reading from non-zero offset.
   */
  @Test
  void testReadFromNonZeroOffset() {
    try (Arena arena = Arena.ofConfined()) {
      // Create segment with padding before the data
      MemorySegment segment = arena.allocate(16);
      
      // Write a varint at offset 5
      int offset = 5;
      segment.set(ValueLayout.JAVA_BYTE, offset, (byte) 0x7F);  // value 127
      
      long decoded = DeltaVarIntCodec.readVarLongFromSegment(segment, offset);
      assertEquals(127, decoded);
      assertEquals(1, DeltaVarIntCodec.varintLength(segment, offset));
    }
  }
  
  /**
   * Test reading multiple varints in sequence.
   */
  @Test
  void testReadMultipleVarintsInSequence() {
    // Encode multiple values
    BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
    long[] values = {1, 128, 16384, 100000};
    
    for (long value : values) {
      writeVarLong(bytesOut, value);
    }
    
    // Get the data as byte array
    byte[] data = bytesOut.toByteArray();
    
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment segment = arena.allocate(data.length);
      MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0, data.length);
      
      // Read each value at its offset
      int currentOffset = 0;
      for (long value : values) {
        long decoded = DeltaVarIntCodec.readVarLongFromSegment(segment, currentOffset);
        assertEquals(value, decoded, "Value mismatch at offset " + currentOffset);
        currentOffset += DeltaVarIntCodec.varintLength(segment, currentOffset);
      }
    }
  }
  
  // Helper method to write varint (mirrors the codec's private method)
  private void writeVarLong(BytesOut<?> sink, long value) {
    while ((value & ~0x7FL) != 0) {
      sink.writeByte((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    sink.writeByte((byte) value);
  }
}
