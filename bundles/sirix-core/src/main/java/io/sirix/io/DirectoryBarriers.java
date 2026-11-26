/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import io.sirix.exception.SirixIOException;

import java.io.IOException;
import java.nio.file.Path;

/**
 * One platform's durability primitives behind {@link DurableFiles}, chosen once per process so that
 * no caller branches on the operating system.
 */
interface DirectoryBarriers {

  /**
   * Makes every earlier entry change in {@code directory} durable.
   *
   * @throws SirixIOException naming {@code directory} if the barrier is rejected or fails
   */
  void syncDirectory(Path directory);

  /**
   * Atomically moves {@code source} to {@code target}; durable on return, including the removal of
   * {@code source}'s name from its directory.
   *
   * @param replaceExisting whether an existing {@code target} is replaced; otherwise the move fails
   *        with {@link java.nio.file.FileAlreadyExistsException}
   * @throws IOException if the move itself fails; nothing was moved
   * @throws SirixIOException naming the directory if the move happened but cannot be made durable
   */
  void move(Path source, Path target, boolean replaceExisting) throws IOException;
}
