/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Sequential little-endian cursor over a region payload.
 *
 * <p>Region headers are a run of fixed-width fields followed by a few parallel arrays, and every
 * codec in this package used to walk them with a {@link java.nio.ByteBuffer} wrapped around a
 * {@code byte[]}. Payloads are now native {@link MemorySegment}s — see {@link ColumnLoad} for why —
 * and this is the same cursor against that backing: one mutable position, pinned little-endian
 * reads, no allocation per field.
 *
 * <p>Not thread-safe and not meant to be. A cursor is created on the stack, walked once to fill a
 * header object, and discarded; the header it produces is what gets shared.
 */
public final class RegionReader {

  private final MemorySegment segment;
  private long position;

  public RegionReader(final MemorySegment segment) {
    this(segment, 0L);
  }

  public RegionReader(final MemorySegment segment, final long position) {
    if (segment == null) {
      throw new IllegalArgumentException("segment");
    }
    if (position < 0 || position > segment.byteSize()) {
      throw new IllegalArgumentException(
          "position=" + position + " outside [0, " + segment.byteSize() + "]");
    }
    this.segment = segment;
    this.position = position;
  }

  /** Current byte offset into the payload. */
  public int position() {
    return (int) position;
  }

  /** Move the cursor to an absolute byte offset. */
  public RegionReader position(final long newPosition) {
    if (newPosition < 0 || newPosition > segment.byteSize()) {
      throw new IllegalArgumentException(
          "position=" + newPosition + " outside [0, " + segment.byteSize() + "]");
    }
    this.position = newPosition;
    return this;
  }

  /** Advance without reading. */
  public RegionReader skip(final long bytes) {
    return position(position + bytes);
  }

  public byte readByte() {
    final byte value = segment.get(ValueLayout.JAVA_BYTE, position);
    position += Byte.BYTES;
    return value;
  }

  public short readShort() {
    final short value = segment.get(LE.SHORT, position);
    position += Short.BYTES;
    return value;
  }

  public int readInt() {
    final int value = segment.get(LE.INT, position);
    position += Integer.BYTES;
    return value;
  }

  public long readLong() {
    final long value = segment.get(LE.LONG, position);
    position += Long.BYTES;
    return value;
  }

  /** Fill {@code dst[0..n)} with consecutive little-endian ints. */
  public void readInts(final int[] dst, final int n) {
    for (int i = 0; i < n; i++) {
      dst[i] = readInt();
    }
  }

  /** Fill {@code dst[0..n)} with consecutive little-endian longs. */
  public void readLongs(final long[] dst, final int n) {
    for (int i = 0; i < n; i++) {
      dst[i] = readLong();
    }
  }

  /** The payload this cursor walks. */
  public MemorySegment segment() {
    return segment;
  }
}
