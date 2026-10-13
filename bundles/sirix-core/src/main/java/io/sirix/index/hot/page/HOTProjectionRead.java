/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot.page;

import org.jspecify.annotations.Nullable;

/** Detached result of a selective physical read; a null entry is an authoritative leaf miss. */
public record HOTProjectionRead(@Nullable HOTLeafEntry entry, @Nullable HOTIndirectPage indirect,
    @Nullable HOTProjectionImage image) {
  public HOTProjectionRead {
    if (indirect != null && (entry != null || image != null)) {
      throw new IllegalArgumentException("a projection read cannot be both a leaf and an indirect route");
    }
  }
}
