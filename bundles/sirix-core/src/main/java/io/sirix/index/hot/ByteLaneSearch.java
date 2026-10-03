/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** Allocation-free byte-key searches when all live lanes fit in one machine word. */
public final class ByteLaneSearch {
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
  private static final long LOW_BITS = 0x7F7F7F7F7F7F7F7FL;
  private static final long HIGH_BITS = 0x8080808080808080L;
  private static final long REPEAT_BYTE = 0x0101010101010101L;

  private ByteLaneSearch() {}

  /** Matching lanes in {@code entries[0,count)}; storage must include eight readable bytes. */
  public static int equal(final byte[] entries, final int count, final byte key) {
    checkCount(count);
    return zeroLanes((long) LONG_LE.get(entries, 0) ^ (key & 255L) * REPEAT_BYTE) & ((1 << count) - 1);
  }

  /** Lanes whose bits are a subset of {@code key}; storage must include eight readable bytes. */
  public static int subset(final byte[] entries, final int count, final byte key) {
    checkCount(count);
    return zeroLanes((long) LONG_LE.get(entries, 0) & (~key & 255L) * REPEAT_BYTE) & ((1 << count) - 1);
  }

  private static void checkCount(final int count) {
    if (count < 0 || count > Long.BYTES)
      throw new IllegalArgumentException("byte-lane count must be in [0,8]");
  }

  private static int zeroLanes(final long mismatches) {
    // Each low-seven-bit lane adds at most 127 + 127: no carry crosses a byte boundary.
    // Unlike the usual subtract-one zero-byte test, this yields an exact mask even for 00,01.
    final long zeroHighBits = ~(((mismatches & LOW_BITS) + LOW_BITS) | mismatches) & HIGH_BITS;
    return (int) Long.compress(zeroHighBits, HIGH_BITS);
  }
}
