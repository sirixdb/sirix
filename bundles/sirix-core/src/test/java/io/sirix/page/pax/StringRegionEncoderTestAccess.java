/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

/** Cross-package white-box access for PageKind's encoder-lifecycle integration tests. */
public final class StringRegionEncoderTestAccess {

  private StringRegionEncoderTestAccess() {
    throw new AssertionError("no instances");
  }

  public static int valueStoreCapacity(final StringRegion.Encoder encoder) {
    return encoder.valueStoreCapacity();
  }

  public static int valueStoreLength(final StringRegion.Encoder encoder) {
    return encoder.valueStoreLength();
  }
}
