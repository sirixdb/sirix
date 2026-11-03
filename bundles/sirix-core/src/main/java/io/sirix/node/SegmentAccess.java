/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Unaligned scalar reads with constant, typed handles for the storage format's byte orders, plus
 * aligned native-order accessors for process-private slabs.
 *
 * <p>
 * Keeping each handle at its access site lets ahead-of-time compilers specialize the access without
 * interpreting the layout's method-handle chain. These are plain reads: the handles retain the
 * foreign-memory API's bounds, scope and thread checks and do not provide synchronization. Only
 * immutable access metadata is initialized at native-image build time; no segment, allocation,
 * resource, or runtime configuration is captured.
 */
public final class SegmentAccess {
  private static final VarHandle BYTE = ValueLayout.JAVA_BYTE.varHandle();
  private static final VarHandle SHORT_LE = LE.SHORT.varHandle();
  private static final VarHandle INT_LE = LE.INT.varHandle();
  private static final VarHandle LONG_LE = LE.LONG.varHandle();
  private static final VarHandle FLOAT_LE = LE.FLOAT.varHandle();
  private static final VarHandle DOUBLE_LE = LE.DOUBLE.varHandle();
  private static final VarHandle SHORT_BE =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).varHandle();
  private static final VarHandle INT_BE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).varHandle();
  private static final VarHandle LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN).varHandle();
  /** Aligned, native-order handles for process-private slabs; never used on persisted bytes. */
  private static final VarHandle LONG_NATIVE = ValueLayout.JAVA_LONG.varHandle();
  private static final VarHandle INT_NATIVE = ValueLayout.JAVA_INT.varHandle();

  private SegmentAccess() {}

  /** Read an 8-byte-aligned native-order long from a process-private slab. */
  public static long getLong(final MemorySegment segment, final long offset) {
    return (long) LONG_NATIVE.get(segment, offset);
  }

  /** Write an 8-byte-aligned native-order long into a process-private slab. */
  public static void putLong(final MemorySegment segment, final long offset, final long value) {
    LONG_NATIVE.set(segment, offset, value);
  }

  /** Read a 4-byte-aligned native-order int from a process-private slab. */
  public static int getInt(final MemorySegment segment, final long offset) {
    return (int) INT_NATIVE.get(segment, offset);
  }

  /** Write a 4-byte-aligned native-order int into a process-private slab. */
  public static void putInt(final MemorySegment segment, final long offset, final int value) {
    INT_NATIVE.set(segment, offset, value);
  }

  /** Read one byte at the given byte offset. */
  public static byte getByte(final MemorySegment segment, final long offset) {
    return (byte) BYTE.get(segment, offset);
  }

  /** Read an unaligned little-endian short. */
  public static short getShortLE(final MemorySegment segment, final long offset) {
    return (short) SHORT_LE.get(segment, offset);
  }

  /** Read an unaligned little-endian int. */
  public static int getIntLE(final MemorySegment segment, final long offset) {
    return (int) INT_LE.get(segment, offset);
  }

  /** Read an unaligned little-endian long. */
  public static long getLongLE(final MemorySegment segment, final long offset) {
    return (long) LONG_LE.get(segment, offset);
  }

  /** Read an unaligned little-endian float. */
  public static float getFloatLE(final MemorySegment segment, final long offset) {
    return (float) FLOAT_LE.get(segment, offset);
  }

  /** Read an unaligned little-endian double. */
  public static double getDoubleLE(final MemorySegment segment, final long offset) {
    return (double) DOUBLE_LE.get(segment, offset);
  }

  /** Read an unaligned big-endian short, as used by ordered keys. */
  public static short getShortBE(final MemorySegment segment, final long offset) {
    return (short) SHORT_BE.get(segment, offset);
  }

  /** Read an unaligned big-endian int, as used by ordered keys. */
  public static int getIntBE(final MemorySegment segment, final long offset) {
    return (int) INT_BE.get(segment, offset);
  }

  /** Read an unaligned big-endian long, as used by ordered keys. */
  public static long getLongBE(final MemorySegment segment, final long offset) {
    return (long) LONG_BE.get(segment, offset);
  }
}
