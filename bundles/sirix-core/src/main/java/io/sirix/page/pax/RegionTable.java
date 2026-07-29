package io.sirix.page.pax;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.SirixLZ77Codec;

import java.lang.foreign.MemorySegment;

/**
 * Length-prefixed list of PAX regions appended to a {@link io.sirix.page.KeyValueLeafPage}.
 *
 * <p>Wire format:
 * <pre>
 * int regionCount
 * regionCount × { byte kind, int size, byte[size] payload }
 * </pre>
 *
 * <p>Each region holds payload-type-segregated data (numeric values, string dictionary
 * entries, struct pointers, DeweyIDs) so scan operators can read a contiguous buffer
 * without per-slot varint decode. The table itself is deliberately simple — per-region
 * encoding lives inside the payload bytes, so adding a new encoding doesn't require a
 * further format bump.
 *
 * <h2>HFT-grade access</h2>
 * Regions are kept in a fixed-size {@code byte[KIND_COUNT][]} slotted by kind ordinal.
 * {@link #payload(byte)} is a single array read with no branching, no linear scan, no
 * boxing, no per-call allocation. The on-read allocation at {@link #read(BytesIn)} is
 * bounded by region count (≤ {@link #KIND_COUNT}) and is only paid once per page load.
 *
 * <p>This class is the Phase-1 scaffold: the table round-trips cleanly but is empty
 * on writes produced by the current codebase. Later tasks populate it with number,
 * string, struct, and DeweyID regions.
 */
public final class RegionTable {

  public static final byte KIND_NUMBER = 0;
  public static final byte KIND_STRING = 1;
  public static final byte KIND_STRUCT = 2;
  public static final byte KIND_DEWEYID = 3;
  public static final byte KIND_OBJECT_KEY_NAMEKEY = 4;
  /**
   * BooleanRegion — packed-bit column for OBJECT_BOOLEAN_VALUE slots, tag-grouped
   * by parent OBJECT_KEY nameKey / pathNodeKey. Enables the columnar filter path
   * for predicates like {@code $u.active} that currently drop back to OBJECT_KEY
   * slot traversal (see task #48 ColumnarScanExecutor).
   */
  public static final byte KIND_BOOLEAN = 5;

  /**
   * HashRegion — per-page column of record hash values, sparse bitmap-indexed.
   * Replaces the per-record 8-byte hash field in the slotted-page heap. For
   * resources configured {@link io.sirix.access.trx.node.HashType#NONE} the
   * region is simply absent and every record's hash resolves to 0 via the
   * sentinel offset-table entry written by the node writers. Saves ~2.4 GB /
   * 100M-record shred on the bench workload.
   */
  public static final byte KIND_HASH = 6;

  /**
   * StructuralPointersRegion — per-page FOR+RLE+delta-of-delta columns for
   * {@code parentKey}, {@code leftSiblingKey}, {@code rightSiblingKey}, and
   * {@code firstChildKey}. In preorder-shred workloads these columns degenerate
   * to monotonic sequences (+1 for right-sibling runs, -N for parent within a
   * subtree), compressing to near-zero bits per record. Replaces the four
   * per-record delta-varint fields in the heap.
   */
  public static final byte KIND_STRUCT_POINTERS = 7;

  /** Size of the fixed-slot storage. Bump when a new region kind is introduced. */
  public static final int KIND_COUNT = 8;

  /** Sentinel empty payload used in place of {@code null} to avoid a per-slot nullcheck on the hot read path. */
  private static final byte[] EMPTY = new byte[0];

  /**
   * Region payload slotted by kind ordinal. Index = {@link #KIND_NUMBER},
   * {@link #KIND_STRING}, etc. Entries are {@link #EMPTY} when the region is absent.
   */
  private final byte[][] payloads = new byte[KIND_COUNT][];

  /** Live count — number of region slots whose payload is non-empty. */
  private int liveCount;

  public RegionTable() {
    // payloads start as null[]; keep null semantics to distinguish "absent" from "empty bytes".
  }

  /** Returns the payload bytes for {@code kind}, or {@code null} when absent. O(1). */
  public byte[] payload(final byte kind) {
    return payloads[kind];
  }

  /** Installs a payload for the given region kind. Pass {@code null} to clear. */
  public void set(final byte kind, final byte[] payload) {
    final byte[] prev = payloads[kind];
    if (prev == null && payload != null) {
      liveCount++;
    } else if (prev != null && payload == null) {
      liveCount--;
    }
    payloads[kind] = payload;
  }

  public boolean isEmpty() {
    return liveCount == 0;
  }

  public int size() {
    return liveCount;
  }

  /** Wire codec ids for region payloads. Matches the heap body's id for LZ77 (3). */
  private static final byte PAYLOAD_RAW = 0;
  private static final byte PAYLOAD_LZ77 = 3;

  /**
   * Payloads below this size skip the compression attempt: the codec's own framing plus the
   * extra length int eat any conceivable saving, and the attempt itself is not free.
   */
  private static final int MIN_COMPRESS_BYTES = 64;

  /**
   * Per-thread scratch for the encode attempt, sized to the LZ77 worst case and grown on demand.
   */
  private static final ThreadLocal<byte[]> ENCODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  /**
   * Per-thread scratch the decoder writes into before the exact-size payload copy. Oversized by
   * {@link #DECODE_TAIL_SLACK} because the native LZ77 decoder's hot loop uses 16-byte wildcopy
   * stores and requires that much tail room to take its fast path.
   */
  private static final ThreadLocal<byte[]> DECODE_SCRATCH =
      ThreadLocal.withInitial(() -> new byte[64 * 1024]);

  private static final int DECODE_TAIL_SLACK = 16;

  /**
   * Write the table, compressing each payload that pays for it.
   *
   * <p>The regions are the single largest section of a JSON database — with strings stored once
   * (heap value elision), the string region alone was 77% of all page bytes, written raw. Text
   * compresses well, and the scans that make the regions worth having never see these wire
   * bytes: {@link #read} decompresses into the in-memory payload arrays exactly once per page
   * load, so the SIMD paths keep scanning raw bytes. Each payload elects its own codec — raw
   * when compression does not strictly win — so incompressible regions cost one length int and
   * nothing else.
   */
  public void write(final BytesOut<?> sink) {
    sink.writeInt(liveCount);
    if (liveCount == 0) {
      return;
    }
    for (int kind = 0; kind < KIND_COUNT; kind++) {
      final byte[] p = payloads[kind];
      if (p == null) {
        continue;
      }
      sink.writeByte((byte) kind);
      if (p.length >= MIN_COMPRESS_BYTES) {
        final int bound = SirixLZ77Codec.maxEncodedSize(p.length);
        byte[] out = ENCODE_SCRATCH.get();
        if (out.length < bound) {
          out = new byte[Math.max(bound, out.length * 2)];
          ENCODE_SCRATCH.set(out);
        }
        final int encodedLen =
            SirixLZ77Codec.encode(MemorySegment.ofArray(p), 0L, p.length, out, 0);
        if (encodedLen > 0 && encodedLen < p.length) {
          sink.writeByte(PAYLOAD_LZ77);
          sink.writeInt(p.length);
          sink.writeInt(encodedLen);
          sink.write(out, 0, encodedLen);
          continue;
        }
      }
      sink.writeByte(PAYLOAD_RAW);
      sink.writeInt(p.length);
      if (p.length > 0) {
        sink.write(p);
      }
    }
  }

  public static RegionTable read(final BytesIn<?> source) {
    final int count = source.readInt();
    final RegionTable t = new RegionTable();
    if (count == 0) {
      return t;
    }
    for (int i = 0; i < count; i++) {
      final byte kind = source.readByte();
      final byte codec = source.readByte();
      final int rawLen = source.readInt();
      final byte[] payload;
      if (codec == PAYLOAD_RAW) {
        payload = rawLen == 0 ? EMPTY : new byte[rawLen];
        if (rawLen > 0) {
          source.read(payload);
        }
      } else if (codec == PAYLOAD_LZ77) {
        final int encodedLen = source.readInt();
        if (encodedLen <= 0 || rawLen <= 0) {
          throw new IllegalStateException("region kind " + kind + " declares a compressed payload"
              + " with rawLen=" + rawLen + " encodedLen=" + encodedLen);
        }
        byte[] in = ENCODE_SCRATCH.get();
        if (in.length < encodedLen) {
          in = new byte[Math.max(encodedLen, in.length * 2)];
          ENCODE_SCRATCH.set(in);
        }
        source.read(in, 0, encodedLen);
        byte[] scratch = DECODE_SCRATCH.get();
        if (scratch.length < rawLen + DECODE_TAIL_SLACK) {
          scratch = new byte[Math.max(rawLen + DECODE_TAIL_SLACK, scratch.length * 2)];
          DECODE_SCRATCH.set(scratch);
        }
        final int decoded =
            SirixLZ77Codec.decode(in, 0, encodedLen, MemorySegment.ofArray(scratch), 0L);
        if (decoded != rawLen) {
          throw new IllegalStateException("region kind " + kind + " decompressed to " + decoded
              + " bytes, expected " + rawLen);
        }
        payload = new byte[rawLen];
        System.arraycopy(scratch, 0, payload, 0, rawLen);
      } else {
        throw new IllegalStateException("region kind " + kind + " has unknown codec " + codec);
      }
      if (kind >= 0 && kind < KIND_COUNT) {
        t.set(kind, payload);
      }
      // Unknown region kinds are silently skipped (forward-compat).
    }
    return t;
  }
}
