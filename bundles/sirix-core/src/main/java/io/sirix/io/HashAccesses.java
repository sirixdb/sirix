/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import net.openhft.hashing.Access;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * {@link Access} implementations that feed openhft's hash functions WITHOUT
 * {@code sun.misc.Unsafe}.
 *
 * <p>
 * The library's default {@code Access.unsafe()} routes every read through
 * {@code sun.misc.Unsafe.getLong/getByte}, and since JDK 25 each such call pays
 * {@code Unsafe.beforeMemoryAccess()} — measured at up to ~10% of a bulk load's CPU on
 * page-checksum verification alone. These accesses read through byte-array view {@link VarHandle}s
 * and the {@link MemorySegment} API instead, which JIT-compile to the same plain loads without the
 * deprecation check.
 *
 * <p>
 * HASH-VALUE STABILITY: the hash function itself is unchanged — only the memory access strategy
 * differs. Both accesses declare {@link ByteOrder#LITTLE_ENDIAN} and return little-endian loads,
 * exactly what {@code Access.unsafe()} yields on every platform sirix ships on, so hashes of
 * identical bytes are bit-identical to those already persisted. {@code HashAccessesEquivalenceTest}
 * pins that equivalence against the library's own default access.
 *
 * <p>
 * The {@code MemorySegment} access serves native and heap segments uniformly; bounds are enforced
 * by the segment itself.
 */
public final class HashAccesses {

  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle SHORT_LE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

  private static final ValueLayout.OfLong SEGMENT_LONG_LE =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfInt SEGMENT_INT_LE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  private static final ValueLayout.OfShort SEGMENT_SHORT_LE =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Little-endian byte-array access; offsets are byte offsets into the array. */
  public static final Access<byte[]> BYTES = new ByteArrayAccess();

  /** Little-endian access over a {@link MemorySegment}, native or heap alike. */
  public static final Access<MemorySegment> SEGMENT = new SegmentAccess();

  private HashAccesses() {
    throw new AssertionError("no instances");
  }

  private static final class ByteArrayAccess extends Access<byte[]> {
    @Override
    public long getLong(final byte[] input, final long offset) {
      return (long) LONG_LE.get(input, (int) offset);
    }

    @Override
    public long getUnsignedInt(final byte[] input, final long offset) {
      return getInt(input, offset) & 0xFFFFFFFFL;
    }

    @Override
    public int getInt(final byte[] input, final long offset) {
      return (int) INT_LE.get(input, (int) offset);
    }

    @Override
    public int getUnsignedShort(final byte[] input, final long offset) {
      return getShort(input, offset) & 0xFFFF;
    }

    @Override
    public int getShort(final byte[] input, final long offset) {
      return (short) SHORT_LE.get(input, (int) offset);
    }

    @Override
    public int getUnsignedByte(final byte[] input, final long offset) {
      return input[(int) offset] & 0xFF;
    }

    @Override
    public int getByte(final byte[] input, final long offset) {
      return input[(int) offset];
    }

    @Override
    public ByteOrder byteOrder(final byte[] input) {
      return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    protected Access<byte[]> reverseAccess() {
      return REVERSED_BYTES;
    }
  }

  private static final class SegmentAccess extends Access<MemorySegment> {
    @Override
    public long getLong(final MemorySegment input, final long offset) {
      return input.get(SEGMENT_LONG_LE, offset);
    }

    @Override
    public long getUnsignedInt(final MemorySegment input, final long offset) {
      return getInt(input, offset) & 0xFFFFFFFFL;
    }

    @Override
    public int getInt(final MemorySegment input, final long offset) {
      return input.get(SEGMENT_INT_LE, offset);
    }

    @Override
    public int getUnsignedShort(final MemorySegment input, final long offset) {
      return getShort(input, offset) & 0xFFFF;
    }

    @Override
    public int getShort(final MemorySegment input, final long offset) {
      return input.get(SEGMENT_SHORT_LE, offset);
    }

    @Override
    public int getUnsignedByte(final MemorySegment input, final long offset) {
      return input.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
    }

    @Override
    public int getByte(final MemorySegment input, final long offset) {
      return input.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public ByteOrder byteOrder(final MemorySegment input) {
      return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    protected Access<MemorySegment> reverseAccess() {
      return REVERSED_SEGMENT;
    }
  }

  /**
   * Big-endian mirrors. The hash algorithms only consult the reverse access when an input declares
   * the OPPOSITE byte order, which the little-endian primaries above never do — these exist to honor
   * the {@link Access} contract rather than to be exercised.
   */
  private static final Access<byte[]> REVERSED_BYTES = new ReversedByteArrayAccess();

  private static final Access<MemorySegment> REVERSED_SEGMENT = new ReversedSegmentAccess();

  private static final class ReversedByteArrayAccess extends Access<byte[]> {
    @Override
    public long getLong(final byte[] input, final long offset) {
      return Long.reverseBytes((long) LONG_LE.get(input, (int) offset));
    }

    @Override
    public int getInt(final byte[] input, final long offset) {
      return Integer.reverseBytes((int) INT_LE.get(input, (int) offset));
    }

    @Override
    public int getShort(final byte[] input, final long offset) {
      return Short.reverseBytes((short) SHORT_LE.get(input, (int) offset));
    }

    @Override
    public int getByte(final byte[] input, final long offset) {
      return input[(int) offset];
    }

    @Override
    public ByteOrder byteOrder(final byte[] input) {
      return ByteOrder.BIG_ENDIAN;
    }

    @Override
    protected Access<byte[]> reverseAccess() {
      return BYTES;
    }
  }

  private static final class ReversedSegmentAccess extends Access<MemorySegment> {
    @Override
    public long getLong(final MemorySegment input, final long offset) {
      return Long.reverseBytes(input.get(SEGMENT_LONG_LE, offset));
    }

    @Override
    public int getInt(final MemorySegment input, final long offset) {
      return Integer.reverseBytes(input.get(SEGMENT_INT_LE, offset));
    }

    @Override
    public int getShort(final MemorySegment input, final long offset) {
      return Short.reverseBytes(input.get(SEGMENT_SHORT_LE, offset));
    }

    @Override
    public int getByte(final MemorySegment input, final long offset) {
      return input.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public ByteOrder byteOrder(final MemorySegment input) {
      return ByteOrder.BIG_ENDIAN;
    }

    @Override
    protected Access<MemorySegment> reverseAccess() {
      return SEGMENT;
    }
  }
}
