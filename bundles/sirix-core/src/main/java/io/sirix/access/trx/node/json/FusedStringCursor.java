/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

/**
 * Internal cursor capability for copying a fused object's semantic UTF-8 string value into
 * caller-owned storage.
 *
 * <p>
 * The copy/decode completes synchronously while the cursor is bound. No page-backed segment, slice,
 * or array escapes through this API. Callers that receive {@link #UNAVAILABLE} must preserve
 * behavior by falling back to the public value API.
 * </p>
 */
public interface FusedStringCursor {

  /** The current cursor cannot provide a fused string through this internal capability. */
  int UNAVAILABLE = -1;

  /**
   * Copy the current fused object's decoded semantic UTF-8 bytes into {@code valueOut} at offset
   * zero.
   *
   * <p>
   * A non-negative result is the number of bytes written. A result less than {@link #UNAVAILABLE}
   * means the destination is too small; {@link #requiredCapacity(int)} returns the capacity with
   * which the caller must retry. The destination is left untouched in both negative-result cases.
   * </p>
   *
   * @param valueOut caller-owned destination
   * @return bytes written, {@link #UNAVAILABLE}, or an encoded retry capacity
   */
  int readFusedStringUtf8(byte[] valueOut);

  /** Encode a required retry capacity without reserving another sentinel integer. */
  static int insufficientCapacity(final int requiredCapacity) {
    if (requiredCapacity <= 0) {
      throw new IllegalArgumentException("requiredCapacity must be positive: " + requiredCapacity);
    }
    return ~requiredCapacity;
  }

  /** Decode a retry capacity returned by {@link #readFusedStringUtf8(byte[])}. */
  static int requiredCapacity(final int result) {
    if (result >= UNAVAILABLE) {
      throw new IllegalArgumentException("result does not encode a retry capacity: " + result);
    }
    return ~result;
  }
}
