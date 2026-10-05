/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import org.jspecify.annotations.Nullable;

/**
 * One query-owned, verified complete image; never a native page or a globally retained fragment.
 */
public final class HOTProjectionImage {
  public static final int MAX_BYTES = 64 * 1024;
  private final byte[] bytes;

  private HOTProjectionImage(final byte[] bytes) {
    this.bytes = bytes;
  }

  /** The caller verified the entire frame and consumed its HOT-leaf kind byte. */
  static @Nullable HOTProjectionImage copyIfFits(final BytesIn<?> source) {
    final long length = source.remaining();
    if (length > MAX_BYTES) {
      return null;
    }
    final byte[] bytes = new byte[(int) length];
    source.read(bytes);
    return new HOTProjectionImage(bytes);
  }

  public @Nullable HOTLeafEntry select(final byte[] key, final long sideReferenceKey) {
    return PageKind.deserializeHOTProjectionEntry(new ByteArrayBytesIn(bytes), key, sideReferenceKey);
  }

  public long retainedSizeBytes() {
    return 32 + ((16L + bytes.length + 7) & ~7L);
  }
}
