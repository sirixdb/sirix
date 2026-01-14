/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.page;

import io.sirix.cache.PageContainer;
import io.sirix.io.Writer;
import io.sirix.node.BytesOut;
import io.sirix.page.IndirectPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Commit executor that operates exclusively on a frozen {@link CommitSnapshot}.
 * <p>
 * This class performs the actual page serialization and disk writing for an async
 * auto-commit operation. It uses the snapshot's TIL for all page lookups, ensuring
 * complete isolation from the active writer thread.
 * <p>
 * <b>Thread Safety:</b>
 * <p>
 * This class is designed to be used by a single background commit worker thread.
 * All page lookups go through the snapshot, which is immutable after creation.
 * <p>
 * <b>Commit Flow:</b>
 * <ol>
 *   <li>Traverse page tree depth-first starting from RevisionRootPage</li>
 *   <li>For each page, look up from snapshot's identity map</li>
 *   <li>Serialize and write to disk</li>
 *   <li>Record disk offset in snapshot for lazy propagation</li>
 *   <li>Release page resources</li>
 * </ol>
 *
 * @author Johannes Lichtenberger
 */
public final class SnapshotCommitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotCommitter.class);

  /**
   * The frozen snapshot to commit.
   */
  private final CommitSnapshot snapshot;

  /**
   * The storage writer for disk I/O.
   */
  private final Writer storageWriter;

  /**
   * Reusable buffer for serialization.
   */
  private final BytesOut<?> bufferBytes;

  /**
   * Creates a new snapshot committer.
   *
   * @param snapshot the commit snapshot containing frozen TIL
   * @param storageWriter the storage writer for disk I/O
   * @param bufferBytes reusable serialization buffer
   */
  public SnapshotCommitter(
      final CommitSnapshot snapshot,
      final Writer storageWriter,
      final BytesOut<?> bufferBytes) {
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
    this.storageWriter = Objects.requireNonNull(storageWriter, "storageWriter must not be null");
    this.bufferBytes = Objects.requireNonNull(bufferBytes, "bufferBytes must not be null");
  }

  /**
   * Commit a page reference and all its descendants.
   * <p>
   * This method performs a depth-first traversal, committing children before
   * the parent to ensure correct disk layout.
   *
   * @param reference the page reference to commit
   */
  public void commit(final PageReference reference) {
    if (reference == null) {
      return;
    }

    // Look up page from snapshot's identity map
    PageContainer container = snapshot.getByIdentity(reference);

    // Fallback to logKey lookup for cloned RRP refs
    if (container == null) {
      final int logKey = reference.getLogKey();
      if (logKey >= 0 && logKey < snapshot.size()) {
        container = snapshot.getEntry(logKey);
      }
    }

    if (container == null) {
      // Page not in snapshot - already on disk or not modified
      return;
    }

    final Page page = container.getModified();
    if (page == null) {
      return;
    }

    // Recursively commit children first (depth-first)
    commitChildren(page);

    // Serialize and write to disk
    try {
      storageWriter.write(snapshot.resourceConfig(), reference, page, bufferBytes);

      // Record disk offset for lazy propagation to cloned refs
      final int logKey = reference.getLogKey();
      if (logKey >= 0) {
        snapshot.recordDiskOffset(logKey, reference.getKey());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to write page to disk: {}", reference, e);
      throw new RuntimeException("Failed to write page during async commit", e);
    }

    // Clean up - release page resources
    try {
      final Page completePage = container.getComplete();
      if (completePage != null && completePage != page) {
        completePage.close();
      }
      page.close();
    } catch (Exception e) {
      LOGGER.warn("Failed to close page after commit: {}", reference, e);
    }

    // Clear page reference to help GC
    reference.setPage(null);
  }

  /**
   * Commit all children of a page.
   *
   * @param page the page whose children to commit
   */
  private void commitChildren(final Page page) {
    if (page instanceof IndirectPage indirectPage) {
      // IndirectPage has child references
      for (final PageReference childRef : indirectPage.getReferences()) {
        if (childRef != null) {
          commit(childRef);
        }
      }
    }
    // Other page types with children can be added here
    // Currently, KeyValueLeafPage doesn't have child page references
  }

  /**
   * Get the snapshot being committed.
   *
   * @return the commit snapshot
   */
  public CommitSnapshot getSnapshot() {
    return snapshot;
  }
}
