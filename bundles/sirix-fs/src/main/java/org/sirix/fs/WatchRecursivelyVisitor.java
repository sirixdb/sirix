/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.fs;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.collect.Maps;

/** Watches a directory recursively. */
@NonNull
public final class WatchRecursivelyVisitor extends SimpleFileVisitor<Path> {

  /** {@link WatchService} reference. */
  private final WatchService mWatcher;

  /** Mapping of {@link WatchService} to {@link Path} instances (for deletes). */
  private final Map<WatchKey, Path> mKeys;

  /** Mapping of unique file/directory identifier to {@link Path} instances. */
  private final Map<Object, List<Path>> mIdentifiers;

  /**
   * Private constructor.
   * 
   * @param pWatcher {@link WatchService} reference where each path is registered
   */
  private WatchRecursivelyVisitor(final WatchService pWatcher) {
    mWatcher = pWatcher;
    mKeys = Maps.newHashMap();
    mIdentifiers = Maps.newHashMap();
  }

  /**
   * Get an instance of {@link WatchRecursivelyVisitor}.
   * 
   * @param pWatcher {@link WatchService} reference where each path is registered
   * @return {@link WatchRecursivelyVisitor} reference
   */
  public static WatchRecursivelyVisitor getInstance(final WatchService pWatcher) {
    return new WatchRecursivelyVisitor(requireNonNull(pWatcher));
  }

  /**
   * Each time a directory is going to be visited the directory is registered at the watchservice.
   */
  @Override
  public FileVisitResult preVisitDirectory(final Path pDir, final BasicFileAttributes pAttrs)
      throws IOException {
    requireNonNull(pDir);
    requireNonNull(pAttrs);
    final WatchKey key = pDir.register(mWatcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    mKeys.put(key, pDir);
    if (mIdentifiers.get(pAttrs.fileKey()) == null) {
      mIdentifiers.put(pAttrs.fileKey(), new ArrayList<Path>());
    }
    mIdentifiers.get(pAttrs.fileKey()).add(pDir);
    return FileVisitResult.CONTINUE;
  }

  /**
   * Get mapping of {@link WatchService} to {@link Path} (for deletes).
   * 
   * @return map instance
   */
  public Map<WatchKey, Path> getKeys() {
    return mKeys;
  }

  /**
   * Get mapping of unique Object to {@link Path}.
   * 
   * @return map instance
   */
  public Map<Object, List<Path>> getIdentifiers() {
    return mIdentifiers;
  }
}
