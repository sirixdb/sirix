package io.sirix.page;

import io.sirix.exception.UnsupportedStoreFeatureException;

/**
 * Active store requirements, published atomically with the uber beacon. Bits are unsigned u32
 * values. The only implemented bit is readOnlyCompat {@link #CONDITIONAL_WRITE_METADATA} (FMT-209);
 * reserving any other bit does not make it supported.
 */
public record StoreFeatures(int incompat, int readOnlyCompat, int compat) {
  public static final StoreFeatures NONE = new StoreFeatures(0, 0, 0);

  /**
   * FMT-209 readOnlyCompat bit 0: the store carries the revision-root last-revert entry, so an engine
   * that does not maintain it may read the store but must not write it.
   */
  public static final int CONDITIONAL_WRITE_METADATA = 0x00000001;

  /** Requirements of a store whose revision roots carry the FMT-209 last-revert entry. */
  public static final StoreFeatures CONDITIONAL_WRITE_METADATA_FEATURES =
      new StoreFeatures(0, CONDITIONAL_WRITE_METADATA, 0);

  /** readOnlyCompat bits this engine maintains. */
  private static final int KNOWN_READ_ONLY_COMPAT = CONDITIONAL_WRITE_METADATA;

  public StoreFeatures union(final StoreFeatures added) {
    return new StoreFeatures(incompat | added.incompat, readOnlyCompat | added.readOnlyCompat, compat | added.compat);
  }

  public boolean isEmpty() {
    return (incompat | readOnlyCompat | compat) == 0;
  }

  /**
   * Whether the store carries a readOnlyCompat bit this engine does not maintain, which makes it
   * read-only here: no writes, healing or cleanup. Known bits such as
   * {@link #CONDITIONAL_WRITE_METADATA} do not count.
   */
  public boolean hasUnknownReadOnlyCompat() {
    return (readOnlyCompat & ~KNOWN_READ_ONLY_COMPAT) != 0;
  }

  public void requireReadable() {
    requireKnown("incompat", incompat);
  }

  public void requireWritable() {
    requireReadable();
    requireKnown("readOnlyCompat", readOnlyCompat & ~KNOWN_READ_ONLY_COMPAT);
  }

  /** Maintenance tools must understand even features safe for ordinary readers/writers to ignore. */
  public void requireToolCompatible() {
    requireWritable();
    requireKnown("compat", compat);
  }

  private static void requireKnown(final String word, final int unknown) {
    if (unknown != 0) {
      throw new UnsupportedStoreFeatureException("Unknown store feature bits in " + word + ": 0x"
          + Integer.toHexString(unknown) + "; upgrade to a release supporting these bits before this operation");
    }
  }
}
