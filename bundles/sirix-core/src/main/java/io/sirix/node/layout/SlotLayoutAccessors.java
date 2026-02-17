package io.sirix.node.layout;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Primitive-only accessors for fixed-size slot layouts backed by {@link MemorySegment}.
 *
 * <p>
 * This utility is designed for hot paths: no boxing, no temporary object allocation.
 *
 * <p>
 * Each accessor has two overloads:
 * <ul>
 * <li>Without {@code baseOffset} — reads/writes relative to the start of the segment (legacy
 * slice-based API).</li>
 * <li>With {@code long baseOffset} — reads/writes at {@code baseOffset + fieldOffset}, allowing
 * callers to pass the full {@code slotMemory} plus an offset instead of allocating an
 * {@code asSlice()} per call.</li>
 * </ul>
 */
public final class SlotLayoutAccessors {
  private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
  private static final ValueLayout.OfInt INT_LAYOUT = ValueLayout.JAVA_INT_UNALIGNED;
  private static final ValueLayout.OfByte BYTE_LAYOUT = ValueLayout.JAVA_BYTE;

  private SlotLayoutAccessors() {}

  // ── readLongField ──────────────────────────────────────────────────

  public static long readLongField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(LONG_LAYOUT, baseOffset + offset);
  }

  public static long readLongField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field) {
    return readLongField(slot, 0L, layout, field);
  }

  // ── writeLongField ─────────────────────────────────────────────────

  public static void writeLongField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field, final long value) {
    final int offset = requiredOffset(layout, field);
    slot.set(LONG_LAYOUT, baseOffset + offset, value);
  }

  public static void writeLongField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field,
      final long value) {
    writeLongField(slot, 0L, layout, field, value);
  }

  // ── readIntField ───────────────────────────────────────────────────

  public static int readIntField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(INT_LAYOUT, baseOffset + offset);
  }

  public static int readIntField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field) {
    return readIntField(slot, 0L, layout, field);
  }

  // ── writeIntField ──────────────────────────────────────────────────

  public static void writeIntField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field, final int value) {
    final int offset = requiredOffset(layout, field);
    slot.set(INT_LAYOUT, baseOffset + offset, value);
  }

  public static void writeIntField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field,
      final int value) {
    writeIntField(slot, 0L, layout, field, value);
  }

  // ── readBooleanField ───────────────────────────────────────────────

  public static boolean readBooleanField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(BYTE_LAYOUT, baseOffset + offset) != 0;
  }

  public static boolean readBooleanField(final MemorySegment slot, final NodeKindLayout layout,
      final StructuralField field) {
    return readBooleanField(slot, 0L, layout, field);
  }

  // ── writeBooleanField ──────────────────────────────────────────────

  public static void writeBooleanField(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final StructuralField field, final boolean value) {
    final int offset = requiredOffset(layout, field);
    slot.set(BYTE_LAYOUT, baseOffset + offset, value
        ? (byte) 1
        : (byte) 0);
  }

  public static void writeBooleanField(final MemorySegment slot, final NodeKindLayout layout,
      final StructuralField field, final boolean value) {
    writeBooleanField(slot, 0L, layout, field, value);
  }

  // ── readPayloadPointer ─────────────────────────────────────────────

  public static long readPayloadPointer(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(LONG_LAYOUT, baseOffset + payloadRef.pointerOffset());
  }

  public static long readPayloadPointer(final MemorySegment slot, final NodeKindLayout layout,
      final int payloadRefIndex) {
    return readPayloadPointer(slot, 0L, layout, payloadRefIndex);
  }

  // ── readPayloadLength ──────────────────────────────────────────────

  public static int readPayloadLength(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(INT_LAYOUT, baseOffset + payloadRef.lengthOffset());
  }

  public static int readPayloadLength(final MemorySegment slot, final NodeKindLayout layout,
      final int payloadRefIndex) {
    return readPayloadLength(slot, 0L, layout, payloadRefIndex);
  }

  // ── readPayloadFlags ───────────────────────────────────────────────

  public static int readPayloadFlags(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(INT_LAYOUT, baseOffset + payloadRef.flagsOffset());
  }

  public static int readPayloadFlags(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex) {
    return readPayloadFlags(slot, 0L, layout, payloadRefIndex);
  }

  // ── writePayloadRef ────────────────────────────────────────────────

  public static void writePayloadRef(final MemorySegment slot, final long baseOffset, final NodeKindLayout layout,
      final int payloadRefIndex, final long pointer, final int length, final int flags) {
    if (length < 0) {
      throw new IllegalArgumentException("length must be >= 0");
    }
    if (flags < 0) {
      throw new IllegalArgumentException("flags must be >= 0");
    }
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    slot.set(LONG_LAYOUT, baseOffset + payloadRef.pointerOffset(), pointer);
    slot.set(INT_LAYOUT, baseOffset + payloadRef.lengthOffset(), length);
    slot.set(INT_LAYOUT, baseOffset + payloadRef.flagsOffset(), flags);
  }

  public static void writePayloadRef(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex,
      final long pointer, final int length, final int flags) {
    writePayloadRef(slot, 0L, layout, payloadRefIndex, pointer, length, flags);
  }

  // ── unchecked hot-path variants ─────────────────────────────────────
  // These skip null checks and bounds validation for maximum performance.
  // Only use when layout and field are known to be valid (e.g., from pre-computed static finals).

  public static long readLongFieldUnchecked(final MemorySegment slot, final long baseOffset,
      final NodeKindLayout layout, final StructuralField field) {
    return slot.get(LONG_LAYOUT, baseOffset + layout.offsetUnchecked(field));
  }

  public static int readIntFieldUnchecked(final MemorySegment slot, final long baseOffset,
      final NodeKindLayout layout, final StructuralField field) {
    return slot.get(INT_LAYOUT, baseOffset + layout.offsetUnchecked(field));
  }

  public static boolean readBooleanFieldUnchecked(final MemorySegment slot, final long baseOffset,
      final NodeKindLayout layout, final StructuralField field) {
    return slot.get(BYTE_LAYOUT, baseOffset + layout.offsetUnchecked(field)) != 0;
  }

  // ── helpers ────────────────────────────────────────────────────────

  private static int requiredOffset(final NodeKindLayout layout, final StructuralField field) {
    final int offset = Objects.requireNonNull(layout, "layout must not be null")
                              .offsetOfOrMinusOne(Objects.requireNonNull(field, "field must not be null"));
    if (offset < 0) {
      throw new IllegalArgumentException("Field " + field + " is not part of layout for " + layout.nodeKind());
    }
    return offset;
  }

  private static PayloadRef payloadRef(final NodeKindLayout layout, final int payloadRefIndex) {
    Objects.requireNonNull(layout, "layout must not be null");
    if (payloadRefIndex < 0 || payloadRefIndex >= layout.payloadRefCount()) {
      throw new IllegalArgumentException("Invalid payloadRefIndex " + payloadRefIndex + " for " + layout.nodeKind()
          + ", count=" + layout.payloadRefCount());
    }
    return layout.payloadRef(payloadRefIndex);
  }
}
