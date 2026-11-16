package io.sirix.page;

import io.sirix.exception.UnsupportedStoreFeatureException;

/**
 * Active store requirements, published atomically with the uber beacon. Bits are unsigned u32
 * values. No features are implemented yet: reserving a bit does not make it supported.
 */
public record StoreFeatures(int incompat, int readOnlyCompat, int compat) {
  public static final StoreFeatures NONE = new StoreFeatures(0, 0, 0);

  public StoreFeatures union(final StoreFeatures added) {
    return new StoreFeatures(incompat | added.incompat, readOnlyCompat | added.readOnlyCompat, compat | added.compat);
  }

  public boolean isEmpty() {
    return (incompat | readOnlyCompat | compat) == 0;
  }

  public void requireReadable() {
    requireKnown("incompat", incompat);
  }

  public void requireWritable() {
    requireReadable();
    requireKnown("readOnlyCompat", readOnlyCompat);
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
