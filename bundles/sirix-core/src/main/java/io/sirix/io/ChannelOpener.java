/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 * Opens the channel a durability force runs on. Production code uses {@code FileChannel::open};
 * tests pass one that records each force, because no in-process observer can see an fsync
 * otherwise.
 */
@FunctionalInterface
interface ChannelOpener {

  /** The opener of the real filesystem. */
  ChannelOpener FILE_SYSTEM = FileChannel::open;

  FileChannel open(Path path, OpenOption option) throws IOException;
}
