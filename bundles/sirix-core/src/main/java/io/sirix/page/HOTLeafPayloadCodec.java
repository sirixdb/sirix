/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchUse;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.Utils;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * The kind-12 slot-payload frame; no codec state survives into a warm leaf.
 *
 * <p>
 * Writers store a payload raw or as LZ4. Readers also decode zero-run frames, which an earlier
 * writer of this format produced, so that every frame ever written stays readable.
 */
public final class HOTLeafPayloadCodec {
  public static final byte FLAG_COMPRESSED = 0x02;
  public static final byte ALLOWED_FLAGS = FLAG_COMPRESSED | HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS;

  /** Codec id of a zero-run frame: read, never written. */
  static final int CODEC_ZERO_RUN = 0;

  /** Codec id of an LZ4 block. */
  static final int CODEC_LZ4 = 1;

  /** Bytes an encoded payload adds to a page: its stored length (i32) and its codec id (u8). */
  static final int FRAME_OVERHEAD = Integer.BYTES + 1;

  /**
   * An LZ4 payload is kept only when it saves, its framing included, at least one part in this many
   * of the raw payload. Anything less is written raw.
   *
   * <p>
   * Derivation. A cold leaf read pays for the decode and is repaid by the bytes it does not read.
   * Measured on the 8,192 leaves of the comparison store's cold probe (58.74 MB of payload, mean
   * 7,169 bytes), through the ordinary reader, ten fresh JVMs per decoder:
   * <ul>
   * <li>native LZ4 decodes at 0.59 ns per payload byte and a raw copy costs 0.10 ns, so decoding adds
   * 0.49 ns per payload byte;</li>
   * <li>the store's device reads a byte it would otherwise have skipped in 1.56 ns (640 MB/s for the
   * bytes a smaller store no longer reads).</li>
   * </ul>
   * LZ4 therefore breaks even when it saves 0.49 / 1.56 = 32 % of the payload, and a third is the
   * nearest rule that needs no arithmetic beyond a multiply. On that sample it keeps LZ4 for 5,407
   * leaves and saves 34.96 MB, against 35.68 MB for "any saving beyond the framing": 2 % of the gain
   * is given up so that no encoded leaf reads slower cold than its raw form where the native decoder
   * exists. Faster storage raises the break-even and slower storage lowers it; one third is this
   * device's, not a universal constant.
   *
   * <p>
   * Zero-run is not offered. On the same leaves it decodes at 2.0 ns per byte, 3.3 times the cost of
   * native LZ4 and 1.5 times that of the Java LZ4 decoder, saves 37 % where LZ4 saves 61 %, and adds
   * less than 0.01 MB where LZ4 exists. A zero-run leaf reads slower cold than a raw one. The table
   * and its protocol are in {@code docs/DISK_FORMAT.md}.
   */
  static final int LZ4_MIN_SAVING_DIVISOR = 3;

  private static final int MAX_ENCODED = ZeroRunByteCodec.maxEncodedSize(HOTLeafPage.DEFAULT_SIZE);
  // Thread-lifetime codec scratch: leases release native storage in finally; cursors reuse the
  // bounded writer/image inputs. Per-call removal would allocate buffers on every cold page.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<State> STATE = ThreadLocal.withInitial(State::new);

  private HOTLeafPayloadCodec() {}

  private static final class State {
    private final byte[] input = new byte[MAX_ENCODED];
    private final FFILz4Compressor lz4 = new FFILz4Compressor();
    private final ScratchLease nativeBytes = new ScratchLease();
    private final ScratchLease compressedBytes = new ScratchLease();
    private byte[] headerBytes;
    private ByteArrayBytesIn header;
    private byte[] imageBytes;
    private ByteArrayBytesIn image;
  }

  /** Read dimensions and leave the cursor at the slot payload, preserving offsets on the wire. */
  private static int seekSlots(final BytesIn<?> source) {
    Utils.getVarLong(source);
    source.readInt();
    source.readByte();
    source.skip(Short.toUnsignedInt(source.readShort()));
    final int count = source.readInt() & Integer.MAX_VALUE;
    final int used = source.readInt();
    if (count > HOTLeafPage.MAX_ENTRIES || used < 0 || used > HOTLeafPage.DEFAULT_SIZE) {
      throw new IllegalStateException("Invalid HOT codec dimensions");
    }
    source.skip((long) count * Integer.BYTES);
    return used;
  }

  /**
   * Write a packed raw V0 image with its slot payload as LZ4 when {@link #keepsLz4} says the saving
   * repays the decode, and unchanged otherwise. A host without native LZ4 writes every page raw.
   */
  static void writePage(final BytesOut<?> sink, final byte[] raw, final int length) {
    final State state = STATE.get();
    Objects.checkFromIndexSize(0, length, raw.length);
    if (state.headerBytes != raw) {
      state.headerBytes = raw;
      state.header = new ByteArrayBytesIn(raw);
    }
    state.header.position(1);
    PageKind.readVersionAndFlagsAllowing(state.header, HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS);
    final int used = seekSlots(state.header);
    final int slots = (int) state.header.position();
    if (used == 0 || !FFILz4Compressor.isNativeAvailable()) {
      sink.write(raw, 0, length);
      return;
    }
    final ScratchLease lease = state.nativeBytes;
    final ScratchLease compressed = state.compressedBytes;
    try {
      final int bound = state.lz4.compressBound(used);
      lease.acquire(used, ScratchUse.CODEC);
      compressed.acquire(bound, ScratchUse.CODEC);
      final MemorySegment scratch = lease.segment();
      MemorySegment.copy(raw, slots, scratch, ValueLayout.JAVA_BYTE, 0, used);
      final int stored = state.lz4.compressSegmentBounded(scratch, used, compressed.segment(), bound);
      if (stored <= 0) {
        throw new IllegalStateException("HOT LZ4 encode failed");
      }
      if (!keepsLz4(used, stored)) {
        sink.write(raw, 0, length);
        return;
      }
      sink.write(raw, 0, 2);
      sink.writeByte((byte) (raw[2] | FLAG_COMPRESSED));
      sink.write(raw, 3, slots - 3);
      sink.writeInt(stored);
      sink.writeByte((byte) CODEC_LZ4);
      sink.writeSegment(compressed.segment(), 0, stored);
      sink.write(raw, slots + used, length - slots - used);
    } finally {
      try {
        compressed.close();
      } finally {
        lease.close();
      }
    }
  }

  /**
   * Whether an LZ4 block of {@code stored} bytes replaces {@code used} bytes of raw payload: the
   * bytes saved after the frame's own {@link #FRAME_OVERHEAD} must be at least one part in
   * {@link #LZ4_MIN_SAVING_DIVISOR} of the payload.
   */
  static boolean keepsLz4(final int used, final int stored) {
    return (long) (used - stored - FRAME_OVERHEAD) * LZ4_MIN_SAVING_DIVISOR >= used;
  }

  /** Decode exactly one bounded payload directly into its final destination. */
  public static void decode(final BytesIn<?> source, final MemorySegment destination, final int used) {
    if (used < 0 || used > HOTLeafPage.DEFAULT_SIZE || used > destination.byteSize() || !destination.isNative()) {
      throw new IllegalStateException("Invalid HOT decoded payload length");
    }
    final int stored = source.readInt();
    final int codec = source.readByte() & 255;
    if (stored <= 0 || stored > MAX_ENCODED || stored > source.remaining() || codec > CODEC_LZ4) {
      throw new IllegalStateException("Invalid HOT encoded payload frame");
    }
    final State state = STATE.get();
    final int produced;
    if (codec == CODEC_ZERO_RUN) {
      source.read(state.input, 0, stored);
      produced = ZeroRunByteCodec.decode(state.input, 0, stored, destination, 0, used);
    } else if (source instanceof MemorySegmentBytesIn nativeSource && nativeSource.getSource().isNative()) {
      produced =
          state.lz4.decompressSegmentRange(nativeSource.getSource(), source.position(), stored, destination, 0, used);
      source.skip(stored);
    } else {
      source.read(state.input, 0, stored);
      final ScratchLease lease = state.nativeBytes.acquire(stored, ScratchUse.CODEC);
      try {
        final MemorySegment input = lease.segment();
        MemorySegment.copy(state.input, 0, input, ValueLayout.JAVA_BYTE, 0, stored);
        produced = state.lz4.decompressSegmentRange(input, 0, stored, destination, 0, used);
      } finally {
        lease.close();
      }
    }
    if (produced != used) {
      throw new IllegalStateException("HOT codec decoded length mismatch");
    }
  }

  /** Heap images stay on array accessors; mixing heap FFM receivers slows native warm lookups. */
  static void decodeHeap(final BytesIn<?> source, final byte[] destination, final int offset, final int used) {
    Objects.checkFromIndexSize(offset, used, destination.length);
    if (used > HOTLeafPage.DEFAULT_SIZE) {
      throw new IllegalStateException("Invalid HOT decoded payload length");
    }
    final int stored = source.readInt();
    final int codec = source.readByte() & 255;
    if (stored <= 0 || stored > MAX_ENCODED || stored > source.remaining() || codec > CODEC_LZ4) {
      throw new IllegalStateException("Invalid HOT encoded payload frame");
    }
    final State state = STATE.get();
    source.read(state.input, 0, stored);
    final int produced;
    if (codec == CODEC_ZERO_RUN) {
      // Bound native output to this payload before copying into the larger retained image.
      final ScratchLease lease = state.nativeBytes.acquire(Math.max(used, 1), ScratchUse.CODEC);
      try {
        final MemorySegment output = lease.segment();
        produced = ZeroRunByteCodec.decode(state.input, 0, stored, output, 0, used);
        if (produced == used) {
          MemorySegment.copy(output, ValueLayout.JAVA_BYTE, 0, destination, offset, used);
        }
      } finally {
        lease.close();
      }
    } else {
      final ScratchLease lease = state.nativeBytes.acquire((long) stored + used, ScratchUse.CODEC);
      try {
        final MemorySegment scratch = lease.segment();
        MemorySegment.copy(state.input, 0, scratch, ValueLayout.JAVA_BYTE, 0, stored);
        produced = state.lz4.decompressSegmentRange(scratch, 0, stored, scratch, stored, used);
        if (produced == used) {
          MemorySegment.copy(scratch, ValueLayout.JAVA_BYTE, stored, destination, offset, used);
        }
      } finally {
        lease.close();
      }
    }
    if (produced != used) {
      throw new IllegalStateException("HOT codec decoded length mismatch");
    }
  }

  /** Expand an encoded frame before immutable image publication; raw frames bypass this codec. */
  public static byte[] expandImage(final byte[] bytes, final int start) {
    return expandImage(bytes, start, bytes.length);
  }

  /** Expand a logical frame in reusable input scratch; bytes beyond length are never part of it. */
  public static byte[] expandImage(final byte[] bytes, final int start, final int length) {
    Objects.checkFromIndexSize(0, length, bytes.length);
    Objects.checkFromIndexSize(start, 2, length);
    final State state = STATE.get();
    if (state.imageBytes != bytes) {
      state.imageBytes = bytes;
      state.image = new ByteArrayBytesIn(bytes);
    }
    final ByteArrayBytesIn source = state.image;
    source.position(start);
    final byte flags = PageKind.readVersionAndFlagsAllowing(source, ALLOWED_FLAGS);
    if ((flags & FLAG_COMPRESSED) == 0) {
      throw new IllegalArgumentException("HOT image has no encoded payload");
    }
    final int used = seekSlots(source);
    final int slots = (int) source.position();
    Objects.checkFromIndexSize(slots, 5, length);
    final int stored = source.readInt();
    // Bound the frame before sizing its owned image; decodeHeap validates the codec payload once.
    Objects.checkFromIndexSize(slots + 5, stored, length);
    final int tail = slots + 5 + stored;
    final byte[] raw = new byte[length - stored - 5 + used];
    System.arraycopy(bytes, 0, raw, 0, slots);
    raw[start + 1] &= (byte) ~FLAG_COMPRESSED;
    source.position(slots);
    decodeHeap(source, raw, slots, used);
    System.arraycopy(bytes, tail, raw, slots + used, length - tail);
    return raw;
  }
}
