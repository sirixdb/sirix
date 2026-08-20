package io.sirix.node;

import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import net.openhft.hashing.LongHashFunction;

/**
 * A MemorySegment-based implementation of BytesOut. Uses GrowingMemorySegment for automatic
 * capacity management with off-heap aligned memory. Implements AutoCloseable to properly release
 * off-heap resources.
 */
public class MemorySegmentBytesOut implements BytesOut<MemorySegment> {
  private final GrowingMemorySegment growingSegment;

  private final boolean retainEmptyPipelineIdentityCache;

  /** Backing-segment identity for {@link #readableByteBuffer()}. */
  private MemorySegment readableByteBufferSegment;

  /**
   * Reusable file-I/O view of the capacity-sized backing segment. The logical limit is reset on
   * every access; this object is replaced only when growth swaps the backing segment.
   */
  private ByteBuffer readableByteBuffer;

  public MemorySegmentBytesOut(MemorySegment initialSegment) {
    this.growingSegment = new GrowingMemorySegment(initialSegment);
    this.retainEmptyPipelineIdentityCache = true;
  }

  public MemorySegmentBytesOut(int initialCapacity) {
    this(initialCapacity, true);
  }

  public MemorySegmentBytesOut() {
    this.growingSegment = new GrowingMemorySegment();
    this.retainEmptyPipelineIdentityCache = true;
  }

  private MemorySegmentBytesOut(final int initialCapacity, final boolean retainEmptyPipelineIdentityCache) {
    this.growingSegment = new GrowingMemorySegment(initialCapacity);
    this.retainEmptyPipelineIdentityCache = retainEmptyPipelineIdentityCache;
  }

  /**
   * Create reusable scratch whose written prefix is consumed before the next clear or write.
   *
   * <p>An empty byte-handler pipeline is an identity operation, so the page serializer must not
   * allocate and retain a second copy for this sink. The caller owns the strict synchronous-lifetime
   * contract expressed by this factory.</p>
   *
   * @param initialCapacity initial scratch capacity in bytes
   * @return a synchronous non-retaining scratch writer
   */
  public static MemorySegmentBytesOut synchronousScratch(final int initialCapacity) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("initialCapacity must be non-negative: " + initialCapacity);
    }
    return new MemorySegmentBytesOut(initialCapacity, false);
  }

  /**
   * Create a MemorySegmentBytesOut with a custom Arena. This allows using confined arenas for
   * temporary buffers that can be explicitly freed.
   * 
   * @param arena the arena to use for memory allocation
   * @param initialCapacity the initial capacity in bytes
   */
  public MemorySegmentBytesOut(Arena arena, int initialCapacity) {
    this.growingSegment = new GrowingMemorySegment(arena, initialCapacity);
    this.retainEmptyPipelineIdentityCache = true;
  }

  /**
   * Create a MemorySegmentBytesOut with a custom Arena and default initial capacity.
   * 
   * @param arena the arena to use for memory allocation
   */
  public MemorySegmentBytesOut(Arena arena) {
    this.growingSegment = new GrowingMemorySegment(arena, 1024);
    this.retainEmptyPipelineIdentityCache = true;
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
    return writeByte(value
        ? (byte) 1
        : (byte) 0);
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

  /**
   * Write the contents of a MemorySegment into this BytesOut without creating an intermediate byte
   * array. This is used by zero-copy compression paths to avoid an extra heap allocation when the
   * compressed data is already represented as a MemorySegment.
   *
   * @param segment the segment to copy from
   * @return this BytesOut for chaining
   */
  public BytesOut<MemorySegment> write(MemorySegment segment) {
    growingSegment.writeSegment(segment, 0, segment.byteSize());
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
    growingSegment.writeSegment(source, sourceOffset, length);
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
  public long hashDirect(LongHashFunction hashFunction) {
    final byte[] backingArray = growingSegment.getBackingArrayUnsafe();
    if (backingArray != null) {
      return hashFunction.hashBytes(backingArray, 0, growingSegment.getUsedSize());
    }
    // Off-heap: hash directly from native address — no legacy ByteBuffer
    final MemorySegment seg = growingSegment.getUsedSegment();
    return hashFunction.hashMemory(seg.address(), seg.byteSize());
  }

  @Override
  public BytesIn<MemorySegment> bytesForRead() {
    return new MemorySegmentBytesIn(growingSegment.getUsedSegment());
  }

  @Override
  public boolean retainsEmptyPipelineIdentityCache() {
    return retainEmptyPipelineIdentityCache;
  }

  @Override
  public BytesOut<MemorySegment> bytesForWrite() {
    return this;
  }

  @Override
  public BytesOut<MemorySegment> clear() {
    growingSegment.reset();
    // A failed write may grow the backing segment and clear this writer before file I/O asks for a
    // readable view. Do not let the cached wrapper retain that replaced backing array until some
    // later flush; ordinary clears on the same base keep the reusable wrapper.
    if (readableByteBufferSegment != null && readableByteBufferSegment != growingSegment.getSegment()) {
      readableByteBuffer = null;
      readableByteBufferSegment = null;
    }
    return this;
  }

  @Override
  public Object underlyingObject() {
    return growingSegment.getUsedSegment();
  }

  @Override
  public MemorySegment getDestination() {
    return growingSegment.getUsedSegment();
  }

  /**
   * Zero-allocation accessor returning the {@code base} segment backing this writer plus the
   * {@linkplain #position() current write position}. Together they describe exactly the same
   * byte range as {@link #getDestination()} but without requesting an exact bounded view.
   * {@code getDestination()} caches recurring small views, but a previously unseen length still
   * needs one {@code MemorySegment.asSlice(0, position)} wrapper.
   *
   * <p>Use when the caller is going to copy out / hand off / persist the written bytes via an
   * API that takes a {@code (segment, offset, length)} triple — e.g.
   * {@link io.sirix.page.KeyValueLeafPage#setSlotDirect(MemorySegment, long, int, int)} or
   * {@link MemorySegment#copy(MemorySegment, long, MemorySegment, long, long)}. The returned
   * segment instance is the live, growable backing segment — its {@code byteSize()} is the
   * full capacity, NOT the used length. Callers MUST honor {@link #position()} as the upper
   * bound and MUST NOT cache the segment across writes (a subsequent grow may swap the
   * backing segment).</p>
   *
   * @return the underlying growable segment (full capacity, position-agnostic)
   */
  public MemorySegment baseSegment() {
    return growingSegment.getSegment();
  }

  /**
   * Return an ephemeral {@link ByteBuffer} view over exactly the written prefix without allocating
   * an exact {@link MemorySegment} slice on each call.
   *
   * <p>The returned object is owned by this writer and reused. Each call resets its position to
   * zero and its limit to {@link #writePosition()}; callers must consume it synchronously and must
   * not retain it, mutate the bytes, or use it concurrently with this writer. A backing-segment
   * growth invalidates the cached view and creates one replacement on the next call. Distinct
   * {@code MemorySegmentBytesOut} instances therefore retain independent views for foreground and
   * background append buffers.</p>
   *
   * @return reusable buffer positioned at zero with an exact logical limit
   * @throws IndexOutOfBoundsException if the logical write position is outside the backing segment
   */
  public ByteBuffer readableByteBuffer() {
    final MemorySegment baseSegment = growingSegment.getSegment();
    final long writtenLength = growingSegment.position();
    if (writtenLength < 0L || writtenLength > baseSegment.byteSize()) {
      throw new IndexOutOfBoundsException(
          "Write position " + writtenLength + " is outside segment capacity " + baseSegment.byteSize());
    }

    if (readableByteBufferSegment != baseSegment) {
      readableByteBufferSegment = baseSegment;
      readableByteBuffer = baseSegment.asByteBuffer();
    }

    final ByteBuffer buffer = readableByteBuffer;
    buffer.clear();
    buffer.limit(Math.toIntExact(writtenLength));
    return buffer;
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
   * Close and release the off-heap memory resources. After calling this method, this BytesOut
   * instance can no longer be used.
   */
  @Override
  public void close() {
    readableByteBuffer = null;
    readableByteBufferSegment = null;
    growingSegment.close();
  }

  /**
   * Currently allocated capacity in bytes (not the write position). Used by the buffer pool to
   * decide whether a buffer is small enough to keep, so one pathological commit's grown segment is
   * released rather than pinned for the process's lifetime.
   *
   * @return the segment's allocated capacity
   */
  public long capacity() {
    return growingSegment.capacity();
  }
}
