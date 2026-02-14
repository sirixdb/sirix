package io.sirix.node.layout;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Primitive-only accessors for fixed-size slot layouts backed by {@link MemorySegment}.
 *
 * <p>This utility is designed for hot paths: no boxing, no temporary object allocation.
 */
public final class SlotLayoutAccessors {
  private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
  private static final ValueLayout.OfInt INT_LAYOUT = ValueLayout.JAVA_INT_UNALIGNED;
  private static final ValueLayout.OfByte BYTE_LAYOUT = ValueLayout.JAVA_BYTE;

  private SlotLayoutAccessors() {
  }

  public static long readLongField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(LONG_LAYOUT, offset);
  }

  public static void writeLongField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field,
      final long value) {
    final int offset = requiredOffset(layout, field);
    slot.set(LONG_LAYOUT, offset, value);
  }

  public static int readIntField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(INT_LAYOUT, offset);
  }

  public static void writeIntField(final MemorySegment slot, final NodeKindLayout layout, final StructuralField field,
      final int value) {
    final int offset = requiredOffset(layout, field);
    slot.set(INT_LAYOUT, offset, value);
  }

  public static boolean readBooleanField(final MemorySegment slot, final NodeKindLayout layout,
      final StructuralField field) {
    final int offset = requiredOffset(layout, field);
    return slot.get(BYTE_LAYOUT, offset) != 0;
  }

  public static void writeBooleanField(final MemorySegment slot, final NodeKindLayout layout,
      final StructuralField field, final boolean value) {
    final int offset = requiredOffset(layout, field);
    slot.set(BYTE_LAYOUT, offset, value ? (byte) 1 : (byte) 0);
  }

  public static long readPayloadPointer(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(LONG_LAYOUT, payloadRef.pointerOffset());
  }

  public static int readPayloadLength(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(INT_LAYOUT, payloadRef.lengthOffset());
  }

  public static int readPayloadFlags(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex) {
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    return slot.get(INT_LAYOUT, payloadRef.flagsOffset());
  }

  public static void writePayloadRef(final MemorySegment slot, final NodeKindLayout layout, final int payloadRefIndex,
      final long pointer, final int length, final int flags) {
    if (length < 0) {
      throw new IllegalArgumentException("length must be >= 0");
    }
    if (flags < 0) {
      throw new IllegalArgumentException("flags must be >= 0");
    }
    final PayloadRef payloadRef = payloadRef(layout, payloadRefIndex);
    slot.set(LONG_LAYOUT, payloadRef.pointerOffset(), pointer);
    slot.set(INT_LAYOUT, payloadRef.lengthOffset(), length);
    slot.set(INT_LAYOUT, payloadRef.flagsOffset(), flags);
  }

  private static int requiredOffset(final NodeKindLayout layout, final StructuralField field) {
    final int offset = Objects.requireNonNull(layout, "layout must not be null")
                              .offsetOfOrMinusOne(Objects.requireNonNull(field, "field must not be null"));
    if (offset < 0) {
      throw new IllegalArgumentException(
          "Field " + field + " is not part of layout for " + layout.nodeKind());
    }
    return offset;
  }

  private static PayloadRef payloadRef(final NodeKindLayout layout, final int payloadRefIndex) {
    Objects.requireNonNull(layout, "layout must not be null");
    if (payloadRefIndex < 0 || payloadRefIndex >= layout.payloadRefCount()) {
      throw new IllegalArgumentException(
          "Invalid payloadRefIndex " + payloadRefIndex + " for " + layout.nodeKind() + ", count=" + layout.payloadRefCount());
    }
    return layout.payloadRef(payloadRefIndex);
  }
}
