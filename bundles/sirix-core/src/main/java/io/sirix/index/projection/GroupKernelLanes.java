/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * The unchecked lane access of the aggregation kernels, and nothing else's: it is never used for
 * persisted page decoding and there is no public way to reach it.
 *
 * <p>
 * <b>Records.</b> A kernel does not address lanes of a chunk. It asks the chunk's
 * {@link GroupBuffer} for a <em>record</em> ({@link GroupBuffer#record},
 * {@link GroupBuffer#accumulator}), or its table for the record of a handle
 * ({@link NumericGroupAggTable#accumulatorAt}): the resolution checks once that the memory is live
 * and that the complete footprint lies inside it, and answers a {@code long} naming that footprint.
 * Every method here takes such a record and a lane <em>relative to it</em>, so lane {@code 0} is
 * the footprint's first lane and a kernel that stays below the lane count it asked for cannot leave
 * its record. The record is a primitive, so a row allocates nothing. A record of a table's chunk
 * stops being valid when the table grows or is released, so a kernel resolves it again after every
 * acquisition; a record of a block that is never handed back stays valid for the scan and is
 * resolved once.
 *
 * <p>
 * <b>Addresses.</b> Outside debug mode a record is the address of its first lane, read and written
 * through one process-wide segment that spans the address space. That segment bounds nothing, which
 * is the point: the bound was established by the record's resolution, and the per-access check of a
 * bounded segment is what the group tables cannot afford (it cannot be hoisted from a probe at a
 * data-dependent address). The remaining check of the spanning segment is vacuous and is folded
 * away by masking the address with {@link #ADDRESS_MASK}, which every allocation is validated
 * against when its buffer is created: the mask is the identity on a valid address and tells the
 * compiler the range it could not otherwise know.
 *
 * <p>
 * <b>Held records.</b> A kernel whose record never moves while it runs (the plain kernels fold
 * every row into one missing-key block) uses the {@code held} operations. They read and write the
 * same lanes; what differs is the code HotSpot compiles for a base that stays the same across a row
 * loop, and {@link #heldAt} says how.
 *
 * <p>
 * <b>Lifetime.</b> A record does not keep its memory alive. The kernel that resolved it keeps the
 * {@link GroupBuffer} or the table it came from reachable until its last access
 * ({@link java.lang.ref.Reference#reachabilityFence}); both hold the workspace whose regions the
 * memory lives in.
 *
 * <p>
 * <b>Debug mode</b> is selected by {@code -Dsirix.aggregation.debug=true} at JVM startup, or at
 * native-image BUILD time: this class is initialized when the image is built, so a production image
 * contains no debug branch. In debug mode a record is not an address but the name of a bounded view
 * of exactly the resolved footprint ({@link GroupDebugRecords}), and every access is checked
 * against that view and against the generation of the allocation it was cut from.
 *
 * <p>
 * <b>Method size.</b> Every method here stays within the 35 bytecodes HotSpot inlines at any call
 * site it has seen executed. A larger method is inlined only where the call was frequent when the
 * loop around it was compiled, and a fold's rarely taken arm, or a table's second probe while the
 * table is still empty, is not: the access would stay a call for as long as the compiled loop
 * lives. {@code GroupAccessorSizeTest} holds the limit.
 */
final class GroupKernelLanes {

  /** Bounded record views and generation checks on every lane access; fixed when the class loads. */
  static final boolean DEBUG = Boolean.getBoolean("sirix.aggregation.debug");

  /**
   * The lane-aligned addresses aggregation memory may have. {@link GroupBuffer} refuses an allocation
   * outside it, so masking a valid lane address changes nothing; it leaves room above the mask for
   * the largest record-relative lane offset an {@code int} lane can express.
   */
  static final long ADDRESS_MASK = 0x3FFF_FFFF_FFFF_FFF8L;

  /**
   * The two halves of {@link #ADDRESS_MASK} a held record's address is added up from. The low half is
   * the part of an address that fits a signed 32-bit offset, which is what makes the compiler add it
   * to a pointer instead of converting the whole sum; see {@link #heldAt}.
   */
  static final long ADDRESS_LOW = 0x7FFF_FFF8L;
  static final long ADDRESS_HIGH = ADDRESS_MASK & ~ADDRESS_LOW;

  /**
   * The segment spanning the address space, and the lane handle. The group table's probes use the two
   * directly (see {@link NumericGroupAggTable}): a probe loop must not contain a call that a compiler
   * may decline to inline.
   */
  static final MemorySegment ADDRESS_SPACE = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);
  static final VarHandle LANE = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();

  private GroupKernelLanes() {}

  /** Lane {@code lane} of {@code record}. */
  static long get(final long record, final int lane) {
    if (DEBUG) {
      return GroupDebugRecords.get(record, lane);
    }
    return (long) LANE.get(ADDRESS_SPACE, at(record, lane));
  }

  /** Store {@code value} in lane {@code lane} of {@code record}. */
  static void set(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.set(record, lane, value);
      return;
    }
    LANE.set(ADDRESS_SPACE, at(record, lane), value);
  }

  /** Add {@code delta} to lane {@code lane} of {@code record} (wrapping, like {@code +=}). */
  static void add(final long record, final int lane, final long delta) {
    if (DEBUG) {
      GroupDebugRecords.add(record, lane, delta);
      return;
    }
    addAt(at(record, lane), delta);
  }

  /** Add one to lane {@code lane} of {@code record}. */
  static void increment(final long record, final int lane) {
    add(record, lane, 1L);
  }

  /**
   * Lower lane {@code lane} of {@code record} to {@code value}. Called for every value, so the rare
   * store stays inside an operation the row loop inlines instead of behind a cold call of its own.
   */
  static void min(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.min(record, lane, value);
      return;
    }
    minAt(at(record, lane), value);
  }

  /** Raise lane {@code lane} of {@code record} to {@code value}; see {@link #min}. */
  static void max(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.max(record, lane, value);
      return;
    }
    maxAt(at(record, lane), value);
  }

  /** {@link #get} for a record the kernel holds across its row loop; see {@link #heldAt}. */
  static long heldGet(final long record, final int lane) {
    if (DEBUG) {
      return GroupDebugRecords.get(record, lane);
    }
    return (long) LANE.get(ADDRESS_SPACE, heldAt(record, lane));
  }

  /** {@link #set} for a record the kernel holds across its row loop; see {@link #heldAt}. */
  static void heldSet(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.set(record, lane, value);
      return;
    }
    LANE.set(ADDRESS_SPACE, heldAt(record, lane), value);
  }

  /** {@link #increment} for a record the kernel holds across its row loop; see {@link #heldAt}. */
  static void heldIncrement(final long record, final int lane) {
    if (DEBUG) {
      GroupDebugRecords.add(record, lane, 1L);
      return;
    }
    addAt(heldAt(record, lane), 1L);
  }

  /** {@link #min} for a record the kernel holds across its row loop; see {@link #heldAt}. */
  static void heldMin(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.min(record, lane, value);
      return;
    }
    minAt(heldAt(record, lane), value);
  }

  /** {@link #max} for a record the kernel holds across its row loop; see {@link #heldAt}. */
  static void heldMax(final long record, final int lane, final long value) {
    if (DEBUG) {
      GroupDebugRecords.max(record, lane, value);
      return;
    }
    maxAt(heldAt(record, lane), value);
  }

  /**
   * The lane at {@code address}, which is not a record: an address a {@link GroupBuffer} derived from
   * its own validated bounds for one checked access. Never reached in debug mode, whose callers read
   * through the bounded buffer instead. Not masked: the spanning segment's own check then runs beside
   * the load rather than ahead of it on the address.
   */
  static long load(final long address) {
    return (long) LANE.get(ADDRESS_SPACE, address);
  }

  /** Store {@code value} at {@code address}; see {@link #load}. */
  static void store(final long address, final long value) {
    LANE.set(ADDRESS_SPACE, address, value);
  }

  /**
   * The address of lane {@code lane} of {@code record}. The mask bounds the record and the unsigned
   * widening bounds the lane, so the sum provably lies inside the spanning segment and its check is
   * compiled away; a negative lane is a kernel defect that debug mode reports.
   */
  private static long at(final long record, final int lane) {
    return (record & ADDRESS_MASK) + (Integer.toUnsignedLong(lane) << 3);
  }

  /**
   * {@link #at} for a record its kernel holds across a row loop: the same address, added up from the
   * record's two masked halves.
   *
   * <p>
   * Masked whole, a record's pointer is the bare conversion of one {@code long}. HotSpot's C2 regards
   * such a value as free to recompute and charges its uses half, so when a loop holds more values
   * than there are registers, that pointer is the first to lose its register although nothing in the
   * loop is used more often, and it is then fetched again in front of every lane access. The
   * four-column plain fold ran 196 instead of 186 instructions a row that way, in three of four JVM
   * runs of the same bytecode. Because the low half fits a 32-bit offset, C2 converts the high half
   * alone and adds the low one to the pointer; that addition of two registers is charged in full, and
   * the pointer kept its register in every run measured.
   *
   * <p>
   * It is not the form for every record. It costs a record that changes with each row one further
   * mask and addition per row, and it makes a kernel's compiled code about a hundred bytes larger,
   * which is enough to take a kernel past the size C2 still inlines into a row loop
   * ({@code -XX:InlineSmallCode}): the one-column sums fold then ran 110 instead of 51 instructions a
   * row. So only the kernels that hold one record for their whole run use it.
   */
  private static long heldAt(final long record, final int lane) {
    return (record & ADDRESS_HIGH) + (record & ADDRESS_LOW) + (Integer.toUnsignedLong(lane) << 3);
  }

  /**
   * Add {@code delta} to the lane at {@code at}: a record's lane address, or a buffer's checked one.
   */
  static void addAt(final long at, final long delta) {
    LANE.set(ADDRESS_SPACE, at, (long) LANE.get(ADDRESS_SPACE, at) + delta);
  }

  private static void minAt(final long at, final long value) {
    if (value < (long) LANE.get(ADDRESS_SPACE, at)) {
      LANE.set(ADDRESS_SPACE, at, value);
    }
  }

  private static void maxAt(final long at, final long value) {
    if (value > (long) LANE.get(ADDRESS_SPACE, at)) {
      LANE.set(ADDRESS_SPACE, at, value);
    }
  }
}
