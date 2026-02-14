package io.sirix.node.layout;

/**
 * Structural fields that are modeled as fixed-width columns in the in-memory slot layout.
 */
public enum StructuralField {
  PARENT_KEY(Long.BYTES),
  RIGHT_SIBLING_KEY(Long.BYTES),
  LEFT_SIBLING_KEY(Long.BYTES),
  FIRST_CHILD_KEY(Long.BYTES),
  LAST_CHILD_KEY(Long.BYTES),
  PATH_NODE_KEY(Long.BYTES),
  PREVIOUS_REVISION(Integer.BYTES),
  LAST_MODIFIED_REVISION(Integer.BYTES),
  PREFIX_KEY(Integer.BYTES),
  LOCAL_NAME_KEY(Integer.BYTES),
  URI_KEY(Integer.BYTES),
  NAME_KEY(Integer.BYTES),
  HASH(Long.BYTES),
  CHILD_COUNT(Long.BYTES),
  DESCENDANT_COUNT(Long.BYTES),
  BOOLEAN_VALUE(Byte.BYTES);

  private final int widthInBytes;

  StructuralField(final int widthInBytes) {
    this.widthInBytes = widthInBytes;
  }

  public int widthInBytes() {
    return widthInBytes;
  }
}
