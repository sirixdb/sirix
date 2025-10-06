package io.sirix.node;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A MemorySegment-based implementation of BytesOut.
 * Uses GrowingMemorySegment for automatic capacity management with off-heap aligned memory.
 * Implements AutoCloseable to properly release off-heap resources.
 */
public class MemorySegmentBytesOut implements BytesOut<MemorySegment> {
    private final GrowingMemorySegment growingSegment;

    public MemorySegmentBytesOut(MemorySegment initialSegment) {
        this.growingSegment = new GrowingMemorySegment(initialSegment);
    }
    
    public MemorySegmentBytesOut(int initialCapacity) {
        this.growingSegment = new GrowingMemorySegment(initialCapacity);
    }
    
    public MemorySegmentBytesOut() {
        this.growingSegment = new GrowingMemorySegment();
    }
    
    /**
     * Create a MemorySegmentBytesOut with a custom Arena.
     * This allows using confined arenas for temporary buffers that can be explicitly freed.
     * 
     * @param arena the arena to use for memory allocation
     * @param initialCapacity the initial capacity in bytes
     */
    public MemorySegmentBytesOut(java.lang.foreign.Arena arena, int initialCapacity) {
        this.growingSegment = new GrowingMemorySegment(arena, initialCapacity);
    }
    
    /**
     * Create a MemorySegmentBytesOut with a custom Arena and default initial capacity.
     * 
     * @param arena the arena to use for memory allocation
     */
    public MemorySegmentBytesOut(java.lang.foreign.Arena arena) {
        this.growingSegment = new GrowingMemorySegment(arena, 1024);
    }

    @Override
    public BytesOut<MemorySegment> writeInt(int value) {
        growingSegment.writeInt(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeLong(long value) {
        growingSegment.writeLong(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeByte(byte value) {
        growingSegment.writeByte(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeBoolean(boolean value) {
        return writeByte(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public BytesOut<MemorySegment> writeDouble(double value) {
        growingSegment.writeDouble(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeFloat(float value) {
        growingSegment.writeFloat(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeShort(short value) {
        growingSegment.writeShort(value);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeBigInteger(BigInteger value) {
        byte[] bytes = value.toByteArray();
        writeInt(bytes.length);
        write(bytes);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeBigDecimal(java.math.BigDecimal value) {
        if (value == null) {
            writeInt(-1);
        } else {
            String stringValue = value.toString();
            writeUtf8(stringValue);
        }
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeUtf8(String value) {
        if (value == null) {
            writeInt(-1);
        } else {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            writeInt(bytes.length);
            write(bytes);
        }
        return this;
    }

    @Override
    public BytesOut<MemorySegment> writeStopBit(long value) {
        // Simple stop-bit encoding implementation
        while ((value & ~0x7FL) != 0) {
            writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeByte((byte) (value & 0x7F));
        return this;
    }

    @Override
    public BytesOut<MemorySegment> write(byte[] bytes) {
        return write(bytes, 0, bytes.length);
    }

    @Override
    public BytesOut<MemorySegment> write(byte[] bytes, int offset, int length) {
        growingSegment.write(bytes, offset, length);
        return this;
    }

    @Override
    public BytesOut<MemorySegment> write(long position, ByteBuffer buffer, int bufferPosition, int length) {
        long oldPos = growingSegment.position();
        growingSegment.setPosition(position);
        
        byte[] temp = new byte[length];
        int oldBufferPos = buffer.position();
        buffer.position(bufferPosition);
        buffer.get(temp);
        buffer.position(oldBufferPos);
        
        write(temp);
        growingSegment.setPosition(oldPos);
        return this;
    }

    @Override
    public long position() {
        return growingSegment.position();
    }

    @Override
    public BytesOut<MemorySegment> position(long newPosition) {
        growingSegment.setPosition(newPosition);
        return this;
    }

    @Override
    public long writePosition() {
        return position();
    }

    @Override
    public BytesOut<MemorySegment> writePosition(long position) {
        return position(position);
    }

    @Override
    public long readLimit() {
        return growingSegment.position();
    }

    @Override
    public byte[] toByteArray() {
        return growingSegment.toByteArray();
    }

    @Override
    public BytesIn<MemorySegment> bytesForRead() {
        return new MemorySegmentBytesIn(growingSegment.getUsedSegment());
    }

    @Override
    public BytesOut<MemorySegment> bytesForWrite() {
        return this;
    }

    @Override
    public BytesOut<MemorySegment> clear() {
        growingSegment.reset();
        return this;
    }

    @Override
    public Object underlyingObject() {
        // Return a ByteBuffer view for compatibility with FileChannelWriter
        // Set native byte order to match FileChannelWriter expectations
        return growingSegment.getUsedSegment().asByteBuffer().order(ByteOrder.nativeOrder());
    }

    @Override
    public MemorySegment getDestination() {
        return growingSegment.getUsedSegment();
    }

    @Override
    public OutputStream outputStream() {
        return new OutputStream() {
            @Override
            public void write(int b) {
                writeByte((byte) b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                MemorySegmentBytesOut.this.write(b, off, len);
            }
        };
    }
    
    /**
     * Close and release the off-heap memory resources.
     * After calling this method, this BytesOut instance can no longer be used.
     */
    @Override
    public void close() {
        growingSegment.close();
    }
}