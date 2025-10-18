package io.sirix.node;

import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteOrder;

/**
 * A MemorySegment-based implementation of BytesIn.
 */
public class MemorySegmentBytesIn implements BytesIn<MemorySegment> {
    private final MemorySegment memorySegment;
    private long position;
    
    public MemorySegmentBytesIn(MemorySegment initialSegment) {
        this.memorySegment = initialSegment;
        this.position = 0;
    }
    
    @Override
    public int readInt() {
        int value = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, position);
        position += Integer.BYTES;
        return value;
    }
    
    @Override
    public long readLong() {
        long value = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, position);
        position += Long.BYTES;
        return value;
    }
    
    @Override
    public byte readByte() {
        byte value = memorySegment.get(ValueLayout.JAVA_BYTE, position);
        position += Byte.BYTES;
        return value;
    }
    
    @Override
    public boolean readBoolean() {
        return readByte() != 0;
    }
    
    @Override
    public double readDouble() {
        double value = memorySegment.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, position);
        position += Double.BYTES;
        return value;
    }
    
    @Override
    public float readFloat() {
        float value = memorySegment.get(ValueLayout.JAVA_FLOAT_UNALIGNED, position);
        position += Float.BYTES;
        return value;
    }
    
    @Override
    public short readShort() {
        short value = memorySegment.get(ValueLayout.JAVA_SHORT_UNALIGNED, position);
        position += Short.BYTES;
        return value;
    }
    
    @Override
    public BigInteger readBigInteger() {
        int length = readInt();
        if (length < 0) {
            throw new IllegalStateException("Invalid BigInteger length: " + length);
        }
        byte[] bytes = new byte[length];
        read(bytes);
        return new BigInteger(bytes);
    }
    
    @Override
    public String readUtf8() {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0) {
            throw new IllegalStateException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        read(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
    
    @Override
    public long readStopBit() {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = readByte();
            result |= (long)(b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
    
    @Override
    public void read(byte[] bytes) {
        read(bytes, 0, bytes.length);
    }
    
    @Override
    public void read(byte[] bytes, int offset, int length) {
        // Bulk copy from MemorySegment to byte array (10-50x faster than byte-by-byte)
        MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, position,
                           bytes, offset, length);
        position += length;
    }
    
    @Override
    public long position() {
        return position;
    }
    
    @Override
    public void position(long newPosition) {
        this.position = newPosition;
    }
    
    @Override
    public long remaining() {
        return memorySegment.byteSize() - position;
    }
    
    @Override
    public MemorySegment getSource() {
        return memorySegment;
    }
    
    @Override
    public MemorySegment getUnderlying() {
        // Return a slice starting at the current position
        return memorySegment.asSlice(position);
    }
    
    @Override
    public InputStream inputStream() {
        return new InputStream() {
            private long streamPosition = position;
            
            @Override
            public int read() {
                if (streamPosition >= memorySegment.byteSize()) {
                    return -1;
                }
                byte value = memorySegment.get(ValueLayout.JAVA_BYTE, streamPosition);
                streamPosition++;
                return value & 0xFF;
            }
            
            @Override
            public int read(byte[] b, int off, int len) {
                long remaining = memorySegment.byteSize() - streamPosition;
                if (remaining <= 0) {
                    return -1;
                }
                int toRead = Math.min(len, (int) remaining);
                // Bulk copy instead of byte-by-byte loop
                MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, streamPosition,
                                   b, off, toRead);
                streamPosition += toRead;
                return toRead;
            }
        };
    }
    
    @Override
    public byte[] toByteArray() {
        long remainingBytes = remaining();
        if (remainingBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Too many bytes to convert to array: " + remainingBytes);
        }
        byte[] result = new byte[(int) remainingBytes];
        // Bulk copy instead of byte-by-byte loop (10-50x faster)
        MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, position,
                           result, 0, (int) remainingBytes);
        return result;
    }
}