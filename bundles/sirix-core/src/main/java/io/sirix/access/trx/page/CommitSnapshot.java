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

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Objects;

/**
 * Immutable snapshot of TIL state at rotation time for async auto-commit.
 * <p>
 * This class captures the state of the TransactionIntentLog at the moment of rotation,
 * providing an isolated context for the background commit worker to serialize and write
 * pages to disk without interfering with ongoing insert operations.
 * <p>
 * <b>Key Features:</b>
 * <ul>
 *   <li>Frozen TIL entries array for iteration by commit worker</li>
 *   <li>Identity-based lookup map for correct PageReference resolution</li>
 *   <li>Deep-copied RevisionRootPage for isolation from insert thread mutations</li>
 *   <li>Disk offset tracking array for lazy propagation after commit completes</li>
 * </ul>
 * <p>
 * <b>Thread Safety:</b>
 * <ul>
 *   <li>{@link #commitComplete} is volatile for visibility across threads</li>
 *   <li>All other fields are effectively immutable after construction</li>
 *   <li>Disk offset array writes are single-writer (commit worker only)</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class CommitSnapshot {

  // ============================================================================
  // Frozen TIL entries
  // ============================================================================

  /**
   * The frozen TIL entries array. Not modified after construction.
   */
  private final PageContainer[] entries;

  /**
   * The number of valid entries in the array.
   */
  private final int entriesSize;

  /**
   * Identity-based map from PageReference to PageContainer.
   * Uses object identity (==) for lookups to correctly distinguish refs with same logKey.
   */
  private final IdentityHashMap<PageReference, PageContainer> refToContainer;

  // ============================================================================
  // Commit metadata
  // ============================================================================

  /**
   * The UberPage at the time of snapshot.
   */
  private final UberPage uberPage;

  /**
   * Reference to the UberPage for commit.
   */
  private final PageReference uberPageReference;

  /**
   * Deep copy of the RevisionRootPage for isolation from insert thread.
   */
  private final RevisionRootPage revisionRootPage;

  /**
   * The revision number being committed.
   */
  private final int revision;

  /**
   * Optional commit message.
   */
  private final String commitMessage;

  /**
   * Commit timestamp.
   */
  private final long commitTimestamp;

  /**
   * Commit file (for incremental commits).
   */
  private final File commitFile;

  /**
   * Resource configuration.
   */
  private final ResourceConfiguration resourceConfig;

  // ============================================================================
  // Disk offset tracking
  // ============================================================================

  /**
   * Primitive array mapping logKey -> disk offset.
   * Populated during commit as pages are written to disk.
   * Value of -1 means not yet written.
   */
  private final long[] logKeyToDiskOffset;

  /**
   * Flag indicating commit is complete and disk offsets are available.
   * Volatile for visibility across threads.
   */
  private volatile boolean commitComplete = false;

  /**
   * Creates a new commit snapshot from a TIL rotation result.
   *
   * @param rotation the rotation result from TIL
   * @param uberPage the uber page
   * @param uberPageReference reference to the uber page
   * @param revisionRootPage the revision root page (will be deep-copied)
   * @param revision the revision number
   * @param commitMessage optional commit message
   * @param commitTimestamp the commit timestamp
   * @param commitFile the commit file (may be null)
   * @param resourceConfig the resource configuration
   */
  public CommitSnapshot(
      final TransactionIntentLog.RotationResult rotation,
      final UberPage uberPage,
      final PageReference uberPageReference,
      final RevisionRootPage revisionRootPage,
      final int revision,
      final String commitMessage,
      final long commitTimestamp,
      final File commitFile,
      final ResourceConfiguration resourceConfig) {

    Objects.requireNonNull(rotation, "rotation must not be null");
    Objects.requireNonNull(uberPage, "uberPage must not be null");
    Objects.requireNonNull(uberPageReference, "uberPageReference must not be null");
    Objects.requireNonNull(revisionRootPage, "revisionRootPage must not be null");
    Objects.requireNonNull(resourceConfig, "resourceConfig must not be null");

    this.entries = rotation.entries();
    this.entriesSize = rotation.size();
    this.refToContainer = rotation.refToContainer();
    this.uberPage = uberPage;
    this.uberPageReference = uberPageReference;
    // Deep copy to isolate from insert thread mutations
    this.revisionRootPage = new RevisionRootPage(revisionRootPage, revision);
    this.revision = revision;
    this.commitMessage = commitMessage;
    this.commitTimestamp = commitTimestamp;
    this.commitFile = commitFile;
    this.resourceConfig = resourceConfig;

    // Pre-allocate disk offset array with -1 (unset)
    this.logKeyToDiskOffset = new long[entriesSize];
    Arrays.fill(logKeyToDiskOffset, -1L);
  }

  // ============================================================================
  // Snapshot TIL Access
  // ============================================================================

  /**
   * Look up a PageContainer by PageReference identity.
   * <p>
   * This is the primary lookup method for pages in the snapshot. It uses
   * object identity (==) rather than equals() to correctly distinguish
   * PageReferences that may have the same logKey but are different objects.
   *
   * @param ref the page reference to look up
   * @return the page container, or null if not found
   */
  @Nullable
  public PageContainer getByIdentity(final PageReference ref) {
    return refToContainer.get(ref);
  }

  /**
   * Get a page container by logKey index (fallback for cloned RRP refs).
   * <p>
   * This is used as a fallback for PageReferences that were cloned during
   * RevisionRootPage deep copy. Since cloned refs have different identity
   * but same logKey, we fall back to direct array access.
   *
   * @param logKey the log key index
   * @return the page container, or null if out of bounds
   */
  @Nullable
  public PageContainer getEntry(final int logKey) {
    if (logKey >= 0 && logKey < entriesSize) {
      return entries[logKey];
    }
    return null;
  }

  /**
   * Get the number of entries in this snapshot.
   *
   * @return the number of entries
   */
  public int size() {
    return entriesSize;
  }

  /**
   * Get the entries array for iteration.
   * <p>
   * WARNING: This returns the internal array. Do not modify!
   *
   * @return the entries array
   */
  PageContainer[] getEntriesArray() {
    return entries;
  }

  // ============================================================================
  // Disk Offset Tracking
  // ============================================================================

  /**
   * Record the disk offset for a page after it has been written.
   * <p>
   * Called by the commit worker after writing each page to disk.
   * This enables lazy propagation of disk offsets to cloned PageReferences.
   *
   * @param logKey the log key of the written page
   * @param diskOffset the disk offset where the page was written
   */
  public void recordDiskOffset(final int logKey, final long diskOffset) {
    if (logKey >= 0 && logKey < logKeyToDiskOffset.length) {
      logKeyToDiskOffset[logKey] = diskOffset;
    }
  }

  /**
   * Get the disk offset for a page by its logKey.
   * <p>
   * Used by layered lookup to lazily update cloned PageReferences with
   * the correct disk offset after commit completes.
   *
   * @param logKey the log key
   * @return the disk offset, or -1 if not yet written or out of bounds
   */
  public long getDiskOffset(final int logKey) {
    if (logKey >= 0 && logKey < logKeyToDiskOffset.length) {
      return logKeyToDiskOffset[logKey];
    }
    return -1L;
  }

  /**
   * Mark the commit as complete.
   * <p>
   * Called by the commit worker after all pages have been written and
   * fsync'd. After this, the layered lookup will use disk offset mapping
   * for lazy propagation.
   */
  public void markCommitComplete() {
    commitComplete = true;
  }

  /**
   * Check if the commit is complete.
   *
   * @return true if commit is complete and disk offsets are available
   */
  public boolean isCommitComplete() {
    return commitComplete;
  }

  // ============================================================================
  // Getters
  // ============================================================================

  public UberPage uberPage() {
    return uberPage;
  }

  public PageReference uberPageReference() {
    return uberPageReference;
  }

  public RevisionRootPage revisionRootPage() {
    return revisionRootPage;
  }

  public int revision() {
    return revision;
  }

  public String commitMessage() {
    return commitMessage;
  }

  public long commitTimestamp() {
    return commitTimestamp;
  }

  public File commitFile() {
    return commitFile;
  }

  public ResourceConfiguration resourceConfig() {
    return resourceConfig;
  }
}
