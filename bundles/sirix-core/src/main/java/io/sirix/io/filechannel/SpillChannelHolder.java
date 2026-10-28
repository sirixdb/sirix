/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.filechannel;

import io.sirix.io.IOStorage;
import java.nio.channels.FileChannel;
import org.jspecify.annotations.Nullable;

/**
 * The spill file of a resource's current writer, shared with every reader of that resource so that
 * a page key flagged as spilled ({@link IOStorage#isSpillKey}) can be read back while the
 * transaction that spilled it is still open. One writer at a time owns a resource; it sets the
 * channel when it first spills and clears it when it closes.
 */
public final class SpillChannelHolder {

  private volatile @Nullable FileChannel channel;

  @Nullable
  public FileChannel get() {
    return channel;
  }

  void set(final FileChannel spillChannel) {
    channel = spillChannel;
  }

  void clearIf(final FileChannel spillChannel) {
    if (channel == spillChannel) {
      channel = null;
    }
  }
}
