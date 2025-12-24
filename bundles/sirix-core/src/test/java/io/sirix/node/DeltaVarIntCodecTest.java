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
  
  // ==================== DIRECT SEGMENT WRITE TESTS ====================
  
  /**
   * Test that writeVarLong in GrowingMemorySegment produces identical bytes to BytesOut.
   * Covers corner cases: 0, 127 (max 1-byte), 128 (min 2-byte), 16383 (max 2-byte),
   * 16384 (min 3-byte), Long.MAX_VALUE (10 bytes).
   */
  @Test
  void testWriteVarLongInGrowingSegmentMatchesBytesOut() {
    long[] values = {0, 1, 63, 64, 127, 128, 255, 256, 16383, 16384, 100000, 
                     Integer.MAX_VALUE, Long.MAX_VALUE};
    
    for (long value : values) {
      // Encode using BytesOut (original method)
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      writeVarLong(bytesOut, value);
      byte[] expected = bytesOut.toByteArray();
      
      // Encode using GrowingMemorySegment (new method)
      GrowingMemorySegment seg = new GrowingMemorySegment(32);
      int bytesWritten = seg.writeVarLong(value);
      byte[] actual = seg.toByteArray();
      
      assertEquals(expected.length, bytesWritten, 
          "Bytes written mismatch for value: " + value);
      assertEquals(expected.length, actual.length, 
          "Array length mismatch for value: " + value);
      
      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], actual[i], 
            "Byte mismatch at index " + i + " for value: " + value);
      }
    }
  }
  
  /**
   * Test that writeVarLong in PooledGrowingSegment produces identical bytes to BytesOut.
   */
  @Test
  void testWriteVarLongInPooledSegmentMatchesBytesOut() {
    long[] values = {0, 1, 63, 64, 127, 128, 255, 256, 16383, 16384, 100000, 
                     Integer.MAX_VALUE, Long.MAX_VALUE};
    
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pooledBuffer = arena.allocate(64);
      
      for (long value : values) {
        // Encode using BytesOut (original method)
        BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
        writeVarLong(bytesOut, value);
        byte[] expected = bytesOut.toByteArray();
        
        // Encode using PooledGrowingSegment (new method)
        PooledGrowingSegment seg = new PooledGrowingSegment(pooledBuffer);
        int bytesWritten = seg.writeVarLong(value);
        MemorySegment written = seg.getWrittenSlice();
        
        assertEquals(expected.length, bytesWritten, 
            "Bytes written mismatch for value: " + value);
        assertEquals(expected.length, written.byteSize(), 
            "Slice size mismatch for value: " + value);
        
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], written.get(ValueLayout.JAVA_BYTE, i), 
              "Byte mismatch at index " + i + " for value: " + value);
        }
        
        seg.reset();
      }
    }
  }
  
  /**
   * Test encodeDelta with GrowingMemorySegment produces identical bytes to BytesOut version.
   * Corner cases: NULL_KEY, delta=0, delta=±1, delta=±63, delta=±64, large deltas.
   */
  @Test
  void testEncodeDeltaWithGrowingSegmentMatchesBytesOut() {
    long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    long[][] testCases = {
      {100, nullKey},         // NULL_KEY
      {100, 100},             // delta = 0 (self-reference, zigzag=0, +1=1)
      {100, 101},             // delta = +1 (zigzag=2, +1=3)
      {100, 99},              // delta = -1 (zigzag=1, +1=2)
      {100, 163},             // delta = +63 (zigzag=126, +1=127, max 1-byte)
      {100, 164},             // delta = +64 (zigzag=128, +1=129, min 2-byte)
      {100, 37},              // delta = -63 (zigzag=125, +1=126)
      {100, 36},              // delta = -64 (zigzag=127, +1=128, 2 bytes)
      {1000, 10000},          // large positive delta
      {10000, 1000},          // large negative delta
      {0, Long.MAX_VALUE / 2},// very large delta
    };
    
    for (long[] testCase : testCases) {
      long baseKey = testCase[0];
      long targetKey = testCase[1];
      
      // Encode using BytesOut (original method)
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeDelta(bytesOut, targetKey, baseKey);
      byte[] expected = bytesOut.toByteArray();
      
      // Encode using GrowingMemorySegment (new method)
      GrowingMemorySegment seg = new GrowingMemorySegment(32);
      DeltaVarIntCodec.encodeDelta(seg, targetKey, baseKey);
      byte[] actual = seg.toByteArray();
      
      assertEquals(expected.length, actual.length, 
          "Array length mismatch for base=" + baseKey + ", target=" + targetKey);
      
      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], actual[i], 
            "Byte mismatch at index " + i + " for base=" + baseKey + ", target=" + targetKey);
      }
      
      // Verify roundtrip - decode using original method
      BytesIn<?> bytesIn = Bytes.wrapForRead(actual);
      long decoded = DeltaVarIntCodec.decodeDelta(bytesIn, baseKey);
      assertEquals(targetKey, decoded, 
          "Roundtrip decode failed for base=" + baseKey + ", target=" + targetKey);
    }
  }
  
  /**
   * Test encodeDelta with PooledGrowingSegment produces identical bytes.
   */
  @Test
  void testEncodeDeltaWithPooledSegmentMatchesBytesOut() {
    long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    long[][] testCases = {
      {100, nullKey},
      {100, 101},
      {100, 99},
      {1000, 10000},
    };
    
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pooledBuffer = arena.allocate(64);
      
      for (long[] testCase : testCases) {
        long baseKey = testCase[0];
        long targetKey = testCase[1];
        
        // Encode using BytesOut (original method)
        BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
        DeltaVarIntCodec.encodeDelta(bytesOut, targetKey, baseKey);
        byte[] expected = bytesOut.toByteArray();
        
        // Encode using PooledGrowingSegment (new method)
        PooledGrowingSegment seg = new PooledGrowingSegment(pooledBuffer);
        DeltaVarIntCodec.encodeDelta(seg, targetKey, baseKey);
        MemorySegment written = seg.getWrittenSlice();
        
        assertEquals(expected.length, written.byteSize(), 
            "Size mismatch for base=" + baseKey + ", target=" + targetKey);
        
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], written.get(ValueLayout.JAVA_BYTE, i), 
              "Byte mismatch at index " + i);
        }
        
        seg.reset();
      }
    }
  }
  
  /**
   * Test encodeSignedLong with direct segment write matches BytesOut.
   */
  @Test
  void testEncodeSignedLongWithSegmentMatchesBytesOut() {
    long[] values = {0, 1, -1, 63, -64, 64, -65, 8191, -8192, 8192, -8193,
                     Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
    
    for (long value : values) {
      // Encode using BytesOut (original method)
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeSignedLong(bytesOut, value);
      byte[] expected = bytesOut.toByteArray();
      
      // Encode using GrowingMemorySegment (new method)
      GrowingMemorySegment seg = new GrowingMemorySegment(32);
      DeltaVarIntCodec.encodeSignedLong(seg, value);
      byte[] actual = seg.toByteArray();
      
      assertEquals(expected.length, actual.length, 
          "Array length mismatch for value: " + value);
      
      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], actual[i], 
            "Byte mismatch at index " + i + " for value: " + value);
      }
    }
  }
  
  /**
   * Test encodeAbsolute with direct segment write matches BytesOut.
   */
  @Test
  void testEncodeAbsoluteWithSegmentMatchesBytesOut() {
    long[] values = {0, 1, 127, 128, 16383, 16384, Integer.MAX_VALUE, Long.MAX_VALUE};
    
    for (long value : values) {
      // Encode using BytesOut (original method)
      BytesOut<?> bytesOut = Bytes.elasticOffHeapByteBuffer();
      DeltaVarIntCodec.encodeAbsolute(bytesOut, value);
      byte[] expected = bytesOut.toByteArray();
      
      // Encode using GrowingMemorySegment (new method)
      GrowingMemorySegment seg = new GrowingMemorySegment(32);
      DeltaVarIntCodec.encodeAbsolute(seg, value);
      byte[] actual = seg.toByteArray();
      
      assertEquals(expected.length, actual.length, 
          "Array length mismatch for value: " + value);
      
      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], actual[i], 
            "Byte mismatch at index " + i + " for value: " + value);
      }
    }
  }
  
  /**
   * Test that segment grows correctly during writeVarLong when near capacity.
   */
  @Test
  void testWriteVarLongTriggersGrowth() {
    // Start with a very small segment
    GrowingMemorySegment seg = new GrowingMemorySegment(4);
    
    // Write a large value that requires 9 bytes (Long.MAX_VALUE = 2^63-1, ceil(63/7) = 9)
    int bytesWritten = seg.writeVarLong(Long.MAX_VALUE);
    
    assertEquals(9, bytesWritten, "Long.MAX_VALUE should require 9 bytes");
    assertEquals(9, seg.size(), "Segment size should be 9");
    
    // Verify the segment grew (initial was 4, now needs at least 9 + ensureCapacity reserves 10)
    assertTrue(seg.capacity() >= 9, "Segment capacity should have grown to at least 9");
  }
  
  private void assertTrue(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
  
  // ==================== SEGMENT COPY TESTS ====================
  
  /**
   * Test writeSegment in GrowingMemorySegment produces identical bytes to write(byte[]).
   */
  @Test
  void testWriteSegmentInGrowingSegmentMatchesWriteBytes() {
    int[] sizes = {0, 1, 7, 8, 15, 16, 127, 128, 1000};
    
    try (Arena arena = Arena.ofConfined()) {
      for (int size : sizes) {
        // Create source segment with test data
        MemorySegment source = arena.allocate(size);
        for (int i = 0; i < size; i++) {
          source.set(ValueLayout.JAVA_BYTE, i, (byte) (i % 256));
        }
        
        // Write using byte[] (original method)
        GrowingMemorySegment segBytes = new GrowingMemorySegment(32);
        if (size > 0) {
          byte[] srcBytes = new byte[size];
          MemorySegment.copy(source, ValueLayout.JAVA_BYTE, 0, srcBytes, 0, size);
          segBytes.write(srcBytes, 0, size);
        }
        byte[] expected = segBytes.toByteArray();
        
        // Write using writeSegment (new method)
        GrowingMemorySegment segDirect = new GrowingMemorySegment(32);
        segDirect.writeSegment(source, 0, size);
        byte[] actual = segDirect.toByteArray();
        
        assertEquals(expected.length, actual.length, 
            "Array length mismatch for size: " + size);
        
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], actual[i], 
              "Byte mismatch at index " + i + " for size: " + size);
        }
      }
    }
  }
  
  /**
   * Test writeSegment in PooledGrowingSegment produces identical bytes.
   */
  @Test
  void testWriteSegmentInPooledSegmentMatchesWriteBytes() {
    int[] sizes = {0, 1, 16, 128, 1000};
    
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pooledBuffer = arena.allocate(2048);
      
      for (int size : sizes) {
        // Create source segment with test data
        MemorySegment source = arena.allocate(size);
        for (int i = 0; i < size; i++) {
          source.set(ValueLayout.JAVA_BYTE, i, (byte) ((i * 7) % 256));
        }
        
        // Write using write(MemorySegment) (existing method)
        PooledGrowingSegment segExisting = new PooledGrowingSegment(pooledBuffer);
        if (size > 0) {
          segExisting.write(source);
        }
        byte[] expected = toByteArray(segExisting);
        segExisting.reset();
        
        // Write using writeSegment (new method)
        PooledGrowingSegment segDirect = new PooledGrowingSegment(pooledBuffer);
        segDirect.writeSegment(source, 0, size);
        byte[] actual = toByteArray(segDirect);
        segDirect.reset();
        
        assertEquals(expected.length, actual.length, 
            "Array length mismatch for size: " + size);
        
        for (int i = 0; i < expected.length; i++) {
          assertEquals(expected[i], actual[i], 
              "Byte mismatch at index " + i + " for size: " + size);
        }
      }
    }
  }
  
  /**
   * Test writeSegment with non-zero source offset.
   */
  @Test
  void testWriteSegmentWithOffset() {
    try (Arena arena = Arena.ofConfined()) {
      // Create source with known pattern
      MemorySegment source = arena.allocate(100);
      for (int i = 0; i < 100; i++) {
        source.set(ValueLayout.JAVA_BYTE, i, (byte) i);
      }
      
      // Write from offset 20, length 30
      GrowingMemorySegment seg = new GrowingMemorySegment(64);
      seg.writeSegment(source, 20, 30);
      byte[] result = seg.toByteArray();
      
      assertEquals(30, result.length, "Should write exactly 30 bytes");
      
      for (int i = 0; i < 30; i++) {
        assertEquals((byte) (20 + i), result[i], 
            "Byte at index " + i + " should be " + (20 + i));
      }
    }
  }
  
  /**
   * Test writeSegment edge case: zero length is a no-op.
   */
  @Test
  void testWriteSegmentZeroLengthNoOp() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment source = arena.allocate(100);
      
      GrowingMemorySegment seg = new GrowingMemorySegment(64);
      seg.writeByte((byte) 0xAB);  // Write something first
      long posBefore = seg.position();
      
      seg.writeSegment(source, 0, 0);  // Zero length should be no-op
      
      assertEquals(posBefore, seg.position(), "Position should not change for zero-length write");
    }
  }
  
  /**
   * Test writeSegment triggers growth correctly.
   */
  @Test
  void testWriteSegmentTriggersGrowth() {
    try (Arena arena = Arena.ofConfined()) {
      // Create large source
      MemorySegment source = arena.allocate(1000);
      for (int i = 0; i < 1000; i++) {
        source.set(ValueLayout.JAVA_BYTE, i, (byte) i);
      }
      
      // Start with small segment
      GrowingMemorySegment seg = new GrowingMemorySegment(16);
      seg.writeSegment(source, 0, 1000);
      
      assertEquals(1000, seg.size(), "Size should be 1000 after write");
      assertTrue(seg.capacity() >= 1000, "Capacity should have grown to at least 1000");
      
      // Verify data integrity after growth
      byte[] result = seg.toByteArray();
      for (int i = 0; i < 1000; i++) {
        assertEquals((byte) i, result[i], "Data corrupted at index " + i);
      }
    }
  }
  
  private byte[] toByteArray(PooledGrowingSegment seg) {
    long pos = seg.position();
    if (pos == 0) {
      return new byte[0];
    }
    byte[] result = new byte[(int) pos];
    MemorySegment.copy(seg.getCurrentSegment(), ValueLayout.JAVA_BYTE, 0, result, 0, (int) pos);
    return result;
  }
  
  // ==================== BATCH WRITE TESTS ====================
  
  /**
   * Test writeBytes2 produces correct bytes.
   */
  @Test
  void testWriteBytes2() {
    GrowingMemorySegment seg = new GrowingMemorySegment(16);
    seg.writeBytes2((byte) 0xAB, (byte) 0xCD);
    
    byte[] result = seg.toByteArray();
    assertEquals(2, result.length);
    assertEquals((byte) 0xAB, result[0]);
    assertEquals((byte) 0xCD, result[1]);
  }
  
  /**
   * Test writeBytes3 produces correct bytes.
   */
  @Test
  void testWriteBytes3() {
    GrowingMemorySegment seg = new GrowingMemorySegment(16);
    seg.writeBytes3((byte) 0x11, (byte) 0x22, (byte) 0x33);
    
    byte[] result = seg.toByteArray();
    assertEquals(3, result.length);
    assertEquals((byte) 0x11, result[0]);
    assertEquals((byte) 0x22, result[1]);
    assertEquals((byte) 0x33, result[2]);
  }
  
  /**
   * Test writeByteAndInt produces correct bytes.
   */
  @Test
  void testWriteByteAndInt() {
    GrowingMemorySegment seg = new GrowingMemorySegment(16);
    seg.writeByteAndInt((byte) 0x42, 0x12345678);
    
    byte[] result = seg.toByteArray();
    assertEquals(5, result.length);
    assertEquals((byte) 0x42, result[0]);
    
    // Read int back (unaligned, native byte order)
    MemorySegment memResult = MemorySegment.ofArray(result);
    int intValue = memResult.get(ValueLayout.JAVA_INT_UNALIGNED, 1);
    assertEquals(0x12345678, intValue);
  }
  
  /**
   * Test writeByteAndLong produces correct bytes.
   */
  @Test
  void testWriteByteAndLong() {
    GrowingMemorySegment seg = new GrowingMemorySegment(16);
    seg.writeByteAndLong((byte) 0xFF, 0x123456789ABCDEF0L);
    
    byte[] result = seg.toByteArray();
    assertEquals(9, result.length);
    assertEquals((byte) 0xFF, result[0]);
    
    // Read long back (unaligned, native byte order)
    MemorySegment memResult = MemorySegment.ofArray(result);
    long longValue = memResult.get(ValueLayout.JAVA_LONG_UNALIGNED, 1);
    assertEquals(0x123456789ABCDEF0L, longValue);
  }
  
  /**
   * Test batch writes match individual writes.
   */
  @Test
  void testBatchWritesMatchIndividualWrites() {
    // Test writeBytes2 matches two writeByte calls
    GrowingMemorySegment segBatch = new GrowingMemorySegment(16);
    segBatch.writeBytes2((byte) 0x11, (byte) 0x22);
    
    GrowingMemorySegment segIndiv = new GrowingMemorySegment(16);
    segIndiv.writeByte((byte) 0x11);
    segIndiv.writeByte((byte) 0x22);
    
    assertArrayEquals(segIndiv.toByteArray(), segBatch.toByteArray());
    
    // Test writeByteAndInt matches writeByte + writeInt
    GrowingMemorySegment segBatchInt = new GrowingMemorySegment(16);
    segBatchInt.writeByteAndInt((byte) 0x99, 12345);
    
    GrowingMemorySegment segIndivInt = new GrowingMemorySegment(16);
    segIndivInt.writeByte((byte) 0x99);
    segIndivInt.writeInt(12345);
    
    assertArrayEquals(segIndivInt.toByteArray(), segBatchInt.toByteArray());
  }
  
  /**
   * Test batch writes in PooledGrowingSegment.
   */
  @Test
  void testBatchWritesInPooledSegment() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pooledBuffer = arena.allocate(64);
      
      PooledGrowingSegment seg = new PooledGrowingSegment(pooledBuffer);
      seg.writeBytes2((byte) 0xAA, (byte) 0xBB);
      seg.writeBytes3((byte) 0xCC, (byte) 0xDD, (byte) 0xEE);
      seg.writeByteAndInt((byte) 0xFF, 0x11223344);
      seg.writeByteAndLong((byte) 0x00, 0xDEADBEEFCAFEBABEL);
      
      byte[] result = toByteArray(seg);
      assertEquals(2 + 3 + 5 + 9, result.length, "Total length should be 19");
      
      // Verify first bytes
      assertEquals((byte) 0xAA, result[0]);
      assertEquals((byte) 0xBB, result[1]);
      assertEquals((byte) 0xCC, result[2]);
      assertEquals((byte) 0xDD, result[3]);
      assertEquals((byte) 0xEE, result[4]);
      assertEquals((byte) 0xFF, result[5]);
    }
  }
  
  private void assertArrayEquals(byte[] expected, byte[] actual) {
    assertEquals(expected.length, actual.length, "Array lengths differ");
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i], "Byte mismatch at index " + i);
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
