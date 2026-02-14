package io.sirix.node.layout;

import java.util.Objects;

/**
 * Pointer metadata embedded in a fixed slot for out-of-line payload blocks.
 */
public record PayloadRef(
    String name,
    PayloadRefKind kind,
    int pointerOffset,
    int lengthOffset,
    int flagsOffset) {

  public static final int POINTER_WIDTH_BYTES = Long.BYTES;
  public static final int LENGTH_WIDTH_BYTES = Integer.BYTES;
  public static final int FLAGS_WIDTH_BYTES = Integer.BYTES;
  public static final int TOTAL_WIDTH_BYTES = POINTER_WIDTH_BYTES + LENGTH_WIDTH_BYTES + FLAGS_WIDTH_BYTES;

  public PayloadRef {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(kind, "kind must not be null");
    if (pointerOffset < 0) {
      throw new IllegalArgumentException("pointerOffset must be >= 0");
    }
    if (lengthOffset != pointerOffset + POINTER_WIDTH_BYTES) {
      throw new IllegalArgumentException("lengthOffset must directly follow pointerOffset");
    }
    if (flagsOffset != lengthOffset + LENGTH_WIDTH_BYTES) {
      throw new IllegalArgumentException("flagsOffset must directly follow lengthOffset");
    }
  }
}
