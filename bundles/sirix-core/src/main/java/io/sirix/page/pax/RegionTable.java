package io.sirix.page.pax;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;

/**
 * Length-prefixed list of PAX regions appended to a {@link io.sirix.page.KeyValueLeafPage}
 * in {@link io.sirix.BinaryEncodingVersion#V1}.
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

  /** Size of the fixed-slot storage. Bump when a new region kind is introduced. */
  public static final int KIND_COUNT = 4;

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
      final int size = source.readInt();
      final byte[] payload = size == 0 ? EMPTY : new byte[size];
      if (size > 0) {
        source.read(payload);
      }
      if (kind >= 0 && kind < KIND_COUNT) {
        t.set(kind, payload);
      }
      // Unknown region kinds are silently skipped (forward-compat).
    }
    return t;
  }
}
