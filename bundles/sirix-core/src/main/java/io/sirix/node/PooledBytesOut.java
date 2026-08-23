package io.sirix.node;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import net.openhft.hashing.LongHashFunction;

/**
 * A BytesOut implementation backed by a PooledGrowingSegment.
 * 
 * <p>
 * This class is designed for use with {@link io.sirix.io.SerializationBufferPool} to enable
 * efficient buffer reuse during parallel page serialization. Unlike MemorySegmentBytesOut which
 * creates new Arena.ofAuto() instances, this class reuses pooled buffers.
 * </p>
 * 
 * <p>
 * Usage pattern:
 * </p>
 * 
 * <pre>{@code
 * var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
 * try {
 *   var bytes = new PooledBytesOut(pooledSeg);
 *   // ... write data ...
 * } finally {
 *   SerializationBufferPool.INSTANCE.release(pooledSeg);
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 */
public final class PooledBytesOut implements BytesOut<MemorySegment> {

  /**
   * Ownership contract for an empty byte-handler pipeline's identity result.
   *
   * <p>
   * The default requires the page serializer to retain an owned copy, because this writer's pooled
   * segment is reused as soon as serialization returns. The alternate policy is deliberately explicit
   * and narrow: its caller must copy the exact written prefix into owned storage before returning
   * this writer to the pool.
   * </p>
   */
  public enum IdentityCachePolicy {
    RETAIN_OWNED_COPY, CALLER_COPIES_BEFORE_RELEASE
  }

  private final PooledGrowingSegment segment;

  private final IdentityCachePolicy identityCachePolicy;

  /**
   * Create a new PooledBytesOut wrapping a PooledGrowingSegment.
   * 
   * @param segment the pooled segment to write to
   */
  public PooledBytesOut(PooledGrowingSegment segment) {
    this(segment, IdentityCachePolicy.RETAIN_OWNED_COPY);
  }

  /**
   * Create a pooled writer with an explicit identity-cache ownership contract.
   *
   * @param segment the pooled segment to write to
   * @param identityCachePolicy ownership policy for an empty byte-handler pipeline's result
   */
  public PooledBytesOut(final PooledGrowingSegment segment, final IdentityCachePolicy identityCachePolicy) {
    this.segment = Objects.requireNonNull(segment);
    this.identityCachePolicy = Objects.requireNonNull(identityCachePolicy);
  }

  /**
   * Create a new PooledBytesOut with its own heap-backed segment. Uses heap memory for efficient GC
   * reclamation.
   * 
   * @param initialCapacity the initial capacity in bytes
   */
  public PooledBytesOut(int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("initialCapacity must be non-negative: " + initialCapacity);
    }
    // Use heap-backed segment - no Arena needed, GC handles cleanup
    MemorySegment buffer = MemorySegment.ofArray(new byte[initialCapacity]);
    this.segment = new PooledGrowingSegment(buffer);
    this.identityCachePolicy = IdentityCachePolicy.RETAIN_OWNED_COPY;
  }

  /**
   * Return the empty-pipeline identity-cache ownership policy for this invocation.
   *
   * @return the explicit identity-cache policy
   */
  public IdentityCachePolicy identityCachePolicy() {
    return identityCachePolicy;
  }

  @Override
  public boolean retainsEmptyPipelineIdentityCache() {
    return identityCachePolicy == IdentityCachePolicy.RETAIN_OWNED_COPY;
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
   * Write data from a MemorySegment at a specific offset without intermediate byte[] allocation. This
   * overrides the default implementation in BytesOut to use direct segment copy.
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
  public long hashDirect(LongHashFunction hashFunction) {
    final long len = segment.position();
    if (len == 0) {
      return hashFunction.hashBytes(new byte[0]);
    }
    final MemorySegment seg = segment.getCurrentSegment();
    final Object heapBase = seg.heapBase().orElse(null);
    if (heapBase instanceof byte[] backingArray) {
      // Heap-backed: hash directly from backing array — zero allocation
      return hashFunction.hashBytes(backingArray, 0, (int) len);
    }
    // Native segment: hash directly from native address — zero allocation
    return hashFunction.hashMemory(seg.address(), len);
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
   * Close is a no-op for PooledBytesOut. The underlying segment is managed by the
   * SerializationBufferPool.
   */
  @Override
  public void close() {
    // No-op: pool manages segment lifecycle
  }
}
