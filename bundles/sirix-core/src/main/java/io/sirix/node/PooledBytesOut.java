package io.sirix.node;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * A BytesOut implementation backed by a PooledGrowingSegment.
 * 
 * <p>This class is designed for use with {@link io.sirix.io.SerializationBufferPool}
 * to enable efficient buffer reuse during parallel page serialization.
 * Unlike MemorySegmentBytesOut which creates new Arena.ofAuto() instances,
 * this class reuses pooled buffers.</p>
 * 
 * <p>Usage pattern:</p>
 * <pre>{@code
 * var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
 * try {
 *     var bytes = new PooledBytesOut(pooledSeg);
 *     // ... write data ...
 * } finally {
 *     SerializationBufferPool.INSTANCE.release(pooledSeg);
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 */
public final class PooledBytesOut implements BytesOut<MemorySegment> {
    
    private final PooledGrowingSegment segment;
    
    /**
     * Create a new PooledBytesOut wrapping a PooledGrowingSegment.
     * 
     * @param segment the pooled segment to write to
     */
    public PooledBytesOut(PooledGrowingSegment segment) {
        this.segment = segment;
    }
    
    /**
     * Create a new PooledBytesOut with its own heap-backed segment.
     * Uses heap memory for efficient GC reclamation.
     * 
     * @param initialCapacity the initial capacity in bytes
     */
    public PooledBytesOut(int initialCapacity) {
        // Use heap-backed segment - no Arena needed, GC handles cleanup
        MemorySegment buffer = MemorySegment.ofArray(new byte[initialCapacity]);
        this.segment = new PooledGrowingSegment(buffer);
    }
    
    @Override
    public BytesOut<MemorySegment> writeInt(int value) {
        segment.writeInt(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeLong(long value) {
        segment.writeLong(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeByte(byte value) {
        segment.writeByte(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeBoolean(boolean value) {
        segment.writeBoolean(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeDouble(double value) {
        segment.writeDouble(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeFloat(float value) {
        segment.writeFloat(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeShort(short value) {
        segment.writeShort(value);
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
    public BytesOut<MemorySegment> writeBigDecimal(BigDecimal value) {
        if (value == null) {
            writeInt(-1);
        } else {
            writeUtf8(value.toString());
        }
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeUtf8(String value) {
        segment.writeUtf8(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> writeStopBit(long value) {
        // OPTIMIZED: Delegate to PooledGrowingSegment.writeVarLong which does single capacity check
        segment.writeVarLong(value);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> write(byte[] bytes) {
        segment.write(bytes);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> write(byte[] bytes, int offset, int length) {
        segment.write(bytes, offset, length);
        return this;
    }
    
    /**
     * Write the contents of a MemorySegment.
     * 
     * @param source the segment to copy from
     * @return this BytesOut for chaining
     */
    public BytesOut<MemorySegment> write(MemorySegment source) {
        segment.write(source);
        return this;
    }
    
    /**
     * Write data from a MemorySegment at a specific offset without intermediate byte[] allocation.
     * This overrides the default implementation in BytesOut to use direct segment copy.
     *
     * @param source the source segment to copy from
     * @param sourceOffset the offset in the source segment
     * @param length the number of bytes to copy
     * @return this BytesOut for chaining
     */
    @Override
    public BytesOut<MemorySegment> writeSegment(MemorySegment source, long sourceOffset, long length) {
        segment.writeSegment(source, sourceOffset, length);
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> write(long position, ByteBuffer buffer, int bufferPosition, int length) {
        long oldPos = segment.position();
        segment.position(position);
        
        byte[] temp = new byte[length];
        int oldBufferPos = buffer.position();
        buffer.position(bufferPosition);
        buffer.get(temp);
        buffer.position(oldBufferPos);
        
        write(temp);
        segment.position(oldPos);
        return this;
    }
    
    @Override
    public long position() {
        return segment.position();
    }
    
    @Override
    public BytesOut<MemorySegment> position(long newPosition) {
        segment.position(newPosition);
        return this;
    }
    
    @Override
    public long writePosition() {
        return segment.position();
    }
    
    @Override
    public BytesOut<MemorySegment> writePosition(long position) {
        segment.position(position);
        return this;
    }
    
    @Override
    public long readLimit() {
        return segment.position();
    }
    
    @Override
    public byte[] toByteArray() {
        long pos = segment.position();
        if (pos == 0) {
            return new byte[0];
        }
        byte[] result = new byte[(int) pos];
        MemorySegment.copy(segment.getCurrentSegment(), ValueLayout.JAVA_BYTE, 0, result, 0, (int) pos);
        return result;
    }
    
    @Override
    public BytesIn<MemorySegment> bytesForRead() {
        return new MemorySegmentBytesIn(segment.getWrittenSlice());
    }
    
    @Override
    public BytesOut<MemorySegment> bytesForWrite() {
        return this;
    }
    
    @Override
    public BytesOut<MemorySegment> clear() {
        segment.clear();
        return this;
    }
    
    @Override
    public Object underlyingObject() {
        return segment.getCurrentSegment();
    }
    
    @Override
    public MemorySegment getDestination() {
        return segment.getCurrentSegment();
    }
    
    /**
     * Get the underlying PooledGrowingSegment.
     * 
     * @return the pooled segment
     */
    public PooledGrowingSegment getPooledSegment() {
        return segment;
    }
    
    @Override
    public OutputStream outputStream() {
        return new OutputStream() {
            @Override
            public void write(int b) {
                segment.writeByte((byte) b);
            }
            
            @Override
            public void write(byte[] b, int off, int len) {
                segment.write(b, off, len);
            }
        };
    }
    
    @Override
    public BytesIn<MemorySegment> asBytesIn() {
        return new MemorySegmentBytesIn(segment.getWrittenSlice());
    }
    
    /**
     * Close is a no-op for PooledBytesOut.
     * The underlying segment is managed by the SerializationBufferPool.
     */
    @Override
    public void close() {
        // No-op: pool manages segment lifecycle
    }
}

