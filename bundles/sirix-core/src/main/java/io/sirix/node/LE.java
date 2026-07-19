/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Little-endian pinned {@link ValueLayout}s for every multi-byte scalar that touches the on-disk
 * format.
 *
 * <p>The storage format is defined as little-endian (see {@code docs/DISK_FORMAT.md}). Using the
 * native-order {@code ValueLayout.JAVA_*} constants made the encoding an accident of the host
 * CPU — correct on x86/ARM-LE, silently byte-swapped on a big-endian host. Every serializer and
 * deserializer must use these pinned layouts instead. On little-endian hosts they are
 * bit-identical (and JIT-identical) to the native-order constants, so the pin costs nothing.
 *
 * <p>The {@code _UNALIGNED}-derived constants carry no alignment constraint and are the default
 * choice for heap/stream offsets; the aligned variants exist for slot-structured access where the
 * layout guarantees natural alignment.
 */
public final class LE {

  /** Unaligned little-endian {@code short}. */
  public static final ValueLayout.OfShort SHORT =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Unaligned little-endian {@code int}. */
  public static final ValueLayout.OfInt INT =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Unaligned little-endian {@code long}. */
  public static final ValueLayout.OfLong LONG =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Unaligned little-endian {@code float}. */
  public static final ValueLayout.OfFloat FLOAT =
      ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Unaligned little-endian {@code double}. */
  public static final ValueLayout.OfDouble DOUBLE =
      ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Naturally aligned little-endian {@code int}. */
  public static final ValueLayout.OfInt INT_ALIGNED =
      ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Naturally aligned little-endian {@code long}. */
  public static final ValueLayout.OfLong LONG_ALIGNED =
      ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Naturally aligned little-endian {@code float}. */
  public static final ValueLayout.OfFloat FLOAT_ALIGNED =
      ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);

  /** Naturally aligned little-endian {@code double}. */
  public static final ValueLayout.OfDouble DOUBLE_ALIGNED =
      ValueLayout.JAVA_DOUBLE.withOrder(ByteOrder.LITTLE_ENDIAN);

  private LE() {
  }
}
