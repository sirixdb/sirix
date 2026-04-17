package io.sirix.index;

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
  DEWEYID_TO_RECORDID((byte) 8),

  /**
   * Vector index for nearest-neighbor search on embeddings.
   */
  VECTOR((byte) 9);

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
   * Direct id-indexed lookup table. IDs are dense [0..9] today; if a sparser
   * id space is ever introduced, this needs to grow OR fall back to a switch.
   * Replaces a {@code HashMap<Byte, IndexType>} that was autoboxing the {@code byte}
   * key on every call ({@code IndexType.getType} showed at ~3% of CPU under
   * Temurin C2 in the 100M-record scan profile).
   */
  private static final IndexType[] BY_ID;

  static {
    final IndexType[] all = values();
    int max = 0;
    for (final IndexType t : all) {
      if (t.id > max) max = t.id;
    }
    BY_ID = new IndexType[max + 1];
    for (final IndexType t : all) {
      BY_ID[t.id] = t;
    }
  }

  /**
   * Public method to get the related index type based on the identifier.
   *
   * @param id the identifier for the index type
   * @return the related index type
   */
  public static IndexType getType(final byte id) {
    if (id < 0 || id >= BY_ID.length) {
      throw new IllegalStateException("Unknown IndexType id: " + id);
    }
    final IndexType indexType = BY_ID[id];
    if (indexType == null) {
      throw new IllegalStateException("Unknown IndexType id: " + id);
    }
    return indexType;
  }
}
