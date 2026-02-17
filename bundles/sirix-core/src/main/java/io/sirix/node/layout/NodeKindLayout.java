package io.sirix.node.layout;

import io.sirix.node.NodeKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Fixed-slot layout contract for a {@link NodeKind}.
 */
public final class NodeKindLayout {
  private static final int UNSUPPORTED_FIELD_OFFSET = -1;
  private static final int FIELD_COUNT = StructuralField.values().length;

  private final NodeKind nodeKind;
  private final int fixedSlotSizeInBytes;
  private final boolean fixedSlotSupported;
  private final int[] offsets;
  private final PayloadRef[] payloadRefs;

  private NodeKindLayout(final NodeKind nodeKind, final int fixedSlotSizeInBytes,
      final boolean fixedSlotSupported, final int[] offsets, final PayloadRef[] payloadRefs) {
    this.nodeKind = Objects.requireNonNull(nodeKind);
    this.fixedSlotSizeInBytes = fixedSlotSizeInBytes;
    this.fixedSlotSupported = fixedSlotSupported;
    this.offsets = Objects.requireNonNull(offsets).clone();
    this.payloadRefs = Objects.requireNonNull(payloadRefs).clone();
  }

  public static NodeKindLayout unsupported(final NodeKind nodeKind) {
    final int[] unsupportedOffsets = new int[FIELD_COUNT];
    Arrays.fill(unsupportedOffsets, UNSUPPORTED_FIELD_OFFSET);
    return new NodeKindLayout(nodeKind, 0, false, unsupportedOffsets, new PayloadRef[0]);
  }

  public static Builder builder(final NodeKind nodeKind) {
    return new Builder(nodeKind);
  }

  public NodeKind nodeKind() {
    return nodeKind;
  }

  public int fixedSlotSizeInBytes() {
    return fixedSlotSizeInBytes;
  }

  public boolean isFixedSlotSupported() {
    return fixedSlotSupported;
  }

  public int offsetOfOrMinusOne(final StructuralField field) {
    return offsets[Objects.requireNonNull(field, "field must not be null").ordinal()];
  }

  public boolean hasField(final StructuralField field) {
    return offsetOfOrMinusOne(field) != UNSUPPORTED_FIELD_OFFSET;
  }

  public int payloadRefCount() {
    return payloadRefs.length;
  }

  public PayloadRef payloadRef(final int index) {
    return payloadRefs[index];
  }

  public static final class Builder {
    private final NodeKind nodeKind;
    private final int[] offsets = new int[FIELD_COUNT];
    private final List<PayloadRef> payloadRefs = new ArrayList<>();
    private int cursor;

    private Builder(final NodeKind nodeKind) {
      this.nodeKind = Objects.requireNonNull(nodeKind);
      Arrays.fill(offsets, UNSUPPORTED_FIELD_OFFSET);
    }

    public Builder addField(final StructuralField field) {
      Objects.requireNonNull(field, "field must not be null");
      if (offsets[field.ordinal()] != UNSUPPORTED_FIELD_OFFSET) {
        throw new IllegalArgumentException("Field already registered: " + field);
      }
      offsets[field.ordinal()] = cursor;
      cursor += field.widthInBytes();
      return this;
    }

    public Builder addPadding(final int bytes) {
      if (bytes < 0) {
        throw new IllegalArgumentException("Padding bytes must be >= 0");
      }
      cursor += bytes;
      return this;
    }

    public Builder addPayloadRef(final String name, final PayloadRefKind kind) {
      final int pointerOffset = cursor;
      final PayloadRef payloadRef =
          new PayloadRef(name, kind, pointerOffset, pointerOffset + PayloadRef.POINTER_WIDTH_BYTES,
              pointerOffset + PayloadRef.POINTER_WIDTH_BYTES + PayloadRef.LENGTH_WIDTH_BYTES);
      payloadRefs.add(payloadRef);
      cursor += PayloadRef.TOTAL_WIDTH_BYTES;
      return this;
    }

    public NodeKindLayout build() {
      return new NodeKindLayout(nodeKind, cursor, true, offsets, payloadRefs.toArray(PayloadRef[]::new));
    }
  }
}
