package org.sirix.index;

import org.sirix.page.PageKind;
import org.sirix.page.interfaces.Page;

import java.util.HashMap;
import java.util.Map;

/**
 * The index type.
 *
 * @author Johannes Lichtenberger
 */
public enum IndexType {
  /**
   * Revision index.
   */
  REVISIONS((byte) 0),

  /**
   * Document index.
   */
  DOCUMENT((byte) 1),

  /**
   * Indexes changed nodes of the revision (the subtree roots).
   */
  CHANGED_NODES((byte) 2),

  /**
   * Indexes records to all revisions, where the record has been inserted, modified or deleted.
   */
  RECORD_TO_REVISIONS((byte) 3),

  /**
   * Indexes the path summary.
   */
  PATH_SUMMARY((byte) 4),

  /**
   * Path index.
   */
  PATH((byte) 5),

  /**
   * Content and structure index.
   */
  CAS((byte) 6),

  /**
   * Name index.
   */
  NAME((byte) 7),

  /**
   * DeweyIDs to record-IDs.
   */
  DEWEYID_TO_RECORDID((byte) 8);

  /**
   * Unique ID.
   */
  private final byte id;

  IndexType(byte id) {
    this.id = id;
  }

  public byte getID() {
    return id;
  }

  /**
   * Mapping of keys -> page
   */
  private static final Map<Byte, IndexType> INSTANCEFORID = new HashMap<>();

  static {
    for (final IndexType indexType : values()) {
      INSTANCEFORID.put(indexType.id, indexType);
    }
  }

  /**
   * Public method to get the related index type based on the identifier.
   *
   * @param id the identifier for the index type
   * @return the related index type
   */
  public static IndexType getType(final byte id) {
    final IndexType indexType = INSTANCEFORID.get(id);
    if (indexType == null) {
      throw new IllegalStateException();
    }
    return indexType;
  }
}
