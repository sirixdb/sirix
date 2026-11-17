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
import java.util.Arrays;

/** The kind-12 slot-payload frame; no codec state survives into a warm leaf. */
public final class HOTLeafPayloadCodec {
  public static final byte FLAG_COMPRESSED = 0x02;
  public static final byte ALLOWED_FLAGS = FLAG_COMPRESSED | HOTLeafPage.FLAG_OVERFLOW_PAGE_REFS;
  private static final int MAX_ENCODED = ZeroRunByteCodec.maxEncodedSize(HOTLeafPage.DEFAULT_SIZE);
  // Thread-lifetime codec scratch: leases release native storage in finally; cursors reuse the
  // bounded writer/image inputs. Per-call removal would allocate buffers on every cold page.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<State> STATE = ThreadLocal.withInitial(State::new);

  private HOTLeafPayloadCodec() {}

  private static final class State {
    private final byte[] zeroRun = new byte[MAX_ENCODED];
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

  /** Offer a packed raw V0 image to both codecs, keeping raw on ties with framing included. */
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
    if (used < 6) {
      sink.write(raw, 0, length);
      return;
    }
    final int zeroLength = ZeroRunByteCodec.encode(raw, slots, used, state.zeroRun, 0);
    int stored = zeroLength;
    int codec = 0;
    final ScratchLease lease = state.nativeBytes;
    final ScratchLease compressed = state.compressedBytes;
    try {
      if (FFILz4Compressor.isNativeAvailable()) {
        final int bound = state.lz4.compressBound(used);
        lease.acquire(used, ScratchUse.CODEC);
        compressed.acquire(bound, ScratchUse.CODEC);
        final MemorySegment scratch = lease.segment();
        MemorySegment.copy(raw, slots, scratch, ValueLayout.JAVA_BYTE, 0, used);
        final int lz4Length = state.lz4.compressSegmentBounded(scratch, used, compressed.segment(), bound);
        if (lz4Length <= 0) {
          throw new IllegalStateException("HOT LZ4 encode failed");
        }
        if (lz4Length < stored - PageKind.PAYLOAD_COMPRESSION_MARGIN) {
          stored = lz4Length;
          codec = 1;
        }
      }
      if (stored >= used - PageKind.PAYLOAD_COMPRESSION_MARGIN) {
        sink.write(raw, 0, length);
        return;
      }
      sink.write(raw, 0, 2);
      sink.writeByte((byte) (raw[2] | FLAG_COMPRESSED));
      sink.write(raw, 3, slots - 3);
      sink.writeInt(stored);
      sink.writeByte((byte) codec);
      if (codec == 0) {
        sink.write(state.zeroRun, 0, stored);
      } else {
        sink.writeSegment(compressed.segment(), 0, stored);
      }
      sink.write(raw, slots + used, length - slots - used);
    } finally {
      try {
        compressed.close();
      } finally {
        lease.close();
      }
    }
  }

  /** Decode exactly one bounded payload directly into its final destination. */
  public static void decode(final BytesIn<?> source, final MemorySegment destination, final int used) {
    if (used < 0 || used > HOTLeafPage.DEFAULT_SIZE || used > destination.byteSize() || !destination.isNative()) {
      throw new IllegalStateException("Invalid HOT decoded payload length");
    }
    final int stored = source.readInt();
    final int codec = source.readByte() & 255;
    if (stored <= 0 || stored > MAX_ENCODED || stored > source.remaining() || codec > 1) {
      throw new IllegalStateException("Invalid HOT encoded payload frame");
    }
    final State state = STATE.get();
    final int produced;
    if (codec == 0) {
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
    if (stored <= 0 || stored > MAX_ENCODED || stored > source.remaining() || codec > 1) {
      throw new IllegalStateException("Invalid HOT encoded payload frame");
    }
    final State state = STATE.get();
    source.read(state.input, 0, stored);
    final int produced;
    if (codec == 0) {
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

  /** Expand once before immutable projection-image publication; warm accessors retain raw layout. */
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
      return length == bytes.length
          ? bytes
          : Arrays.copyOf(bytes, length);
    }
    final int used = seekSlots(source);
    final int slots = (int) source.position();
    if (slots > length - 5) {
      throw new IllegalStateException("Truncated HOT encoded image header");
    }
    final int stored = source.readInt();
    if (stored <= 0 || stored > MAX_ENCODED || (long) slots + 5 + stored > length) {
      throw new IllegalStateException("Invalid HOT encoded image length");
    }
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
