/*
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

package io.sirix.api;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixThreadedException;
import io.sirix.exception.SirixUsageException;
import io.sirix.node.interfaces.Node;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.cache.Cache;
import io.sirix.cache.RBIndexKey;
import io.sirix.index.path.summary.PathSummaryReader;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Each resource is bound to a {@code ResourceSession}. Reader-only transactions and the single
 * read/write transaction can then be started from this instance. There can only be one write
 * transaction at a time. However, multiple read-only transactions can coexist concurrently.
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * @author Johannes Lichtenberger
 */
public interface ResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AutoCloseable {

  /**
   * Get the resource path.
   *
   * @return the resource path
   */
  Path getResourcePath();

  /**
   * Get the history, that is the metadata informations about the revisions.
   *
   * @return the history
   */
  List<RevisionInfo> getHistory();

  /**
   * Get the history, that is the metadata informations about the revisions.
   *
   * @param revisions number of revision informations to retrieve starting with the most recent
   * @return the history
   */
  List<RevisionInfo> getHistory(int revisions);

  /**
   * Get the history, that is the metadata informations about the revisions.
   *
   * <p>The bounds are inclusive and may be passed in either order (and may be equal); results
   * are returned newest-first.
   *
   * @param fromRevision one bound of the revision range (must be positive)
   * @param toRevision the other bound of the revision range (must be positive)
   * @return the history
   */
  List<RevisionInfo> getHistory(int fromRevision, int toRevision);

  /**
   * Get the commit timestamps (epoch milliseconds) of all revisions, newest-first.
   *
   * <p>This is a fast path for callers that only need the timestamps of the history (not the
   * commit author or message). Unlike {@link #getHistory()}, it is served entirely from the
   * in-memory revision index: no {@link StorageEngineReader} is opened, no
   * {@code RevisionRootPage} is read, and no per-revision asynchronous work is scheduled.
   * Covers revisions {@code [1, mostRecentRevision]} (revision {@code 0} is the empty
   * bootstrap, mirroring {@link #getHistory()}).
   *
   * @return the commit timestamps as epoch milliseconds, ordered most-recent revision first
   *         (empty if the resource has no committed revisions yet)
   */
  long[] getHistoryTimestamps();

  /**
   * Get the commit timestamps (epoch milliseconds) for an inclusive revision range,
   * newest-first.
   *
   * <p>The bounds are inclusive and may be passed in either order (and may be equal). Like
   * {@link #getHistoryTimestamps()} this reads only from the in-memory revision index.
   *
   * @param fromRevision one bound of the revision range (must be positive)
   * @param toRevision   the other bound of the revision range (must be positive)
   * @return the commit timestamps as epoch milliseconds, ordered most-recent revision first
   */
  long[] getHistoryTimestamps(int fromRevision, int toRevision);

  /**
   * Get the ascending list of revisions in which the record with the given {@code nodeKey} was
   * created or modified.
   *
   * <p>This is served by a single lookup of the {@code RECORD_TO_REVISIONS} node-history index
   * (one lightweight storage reader, no full node transaction). It is the basis for value-history
   * scans that only need to read the revisions in which a value actually changed (the value is
   * unchanged between consecutive entries), rather than every revision.
   *
   * @param nodeKey the stable node key of the record
   * @return the change revisions in ascending order; an empty array when the resource was created
   *         without {@code storeNodeHistory} or the record has no recorded history
   */
  int[] getRecordChangeRevisions(long nodeKey);

  /**
   * Scan the value history of the record with the given {@code nodeKey}, invoking {@code visitor}
   * once per revision in which the record exists.
   *
   * <p>When the resource keeps a node-history index ({@code storeNodeHistory}), only the revisions
   * in which the record actually changed are read (see {@link #getRecordChangeRevisions(long)}) —
   * the value is unchanged in between, so a consumer can treat each visited revision as the start
   * of a run that lasts until the next visited revision. Without the index, every revision is
   * scanned. Either way each revision is read through a lightweight storage reader rather than a
   * full read-only node transaction, avoiding the per-revision transaction-construction cost.
   *
   * <p>Revisions in which the record does not exist (before creation, or after deletion) are
   * skipped. The {@link io.sirix.node.interfaces.DataRecord} handed to the visitor is valid only
   * for the duration of the callback.
   *
   * @param nodeKey the stable node key of the record
   * @param visitor the callback invoked for each existing historical version (must not be null)
   */
  void scanRecordHistory(long nodeKey, RecordHistoryVisitor visitor);

  /**
   * Get the single node writer if available, wrapped in an {@link Optional}.
   *
   * @return The single node writer if available.
   */
  Optional<W> getNodeTrx();

  /**
   * Number of currently-open read-only or read-write node transactions on this session.
   * Useful for diagnostics and for asserting the absence of resource leaks in tests.
   *
   * @return the number of open node transactions, including the single writer if any
   */
  int activeTrxCount();

  /**
   * Begin a new {@link StorageEngineReader}.
   *
   * @return new {@link StorageEngineReader} instance
   */
  default StorageEngineReader createStorageEngineReader() {
    return createStorageEngineReader(getMostRecentRevisionNumber());
  }

  /**
   * Begin a new {@link StorageEngineReader}.
   *
   * @param revision revision number
   * @return new {@link StorageEngineReader} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  StorageEngineReader createStorageEngineReader(int revision);

  /**
   * Begin a new {@link StorageEngineWriter}.
   *
   * @return new {@link StorageEngineWriter} instance
   * @throws SirixException if Sirix fails to create a new instance
   */
  default StorageEngineWriter createStorageEngineWriter() {
    return createStorageEngineWriter(getMostRecentRevisionNumber());
  }

  /**
   * Begin a read-only transaction on the latest committed revision.
   *
   * @return instance of a class, which implements the {@link XmlNodeReadOnlyTrx} interface
   * @throws SirixException if can't begin Read Transaction
   */
  default R beginNodeReadOnlyTrx() {
    return beginNodeReadOnlyTrx(getMostRecentRevisionNumber());
  }

  /**
   * Begin a new {@link StorageEngineWriter}.
   *
   * @param revision revision number
   * @return new {@link StorageEngineWriter} instance
   * @throws SirixException if Sirix fails to create a new instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  StorageEngineWriter createStorageEngineWriter(int revision);

  /**
   * Begin a read-only transaction on the given revision number.
   *
   * @param revision revision to read from denoted by the revision number.
   * @return instance of a class, which implements the {@link XmlNodeReadOnlyTrx} interface
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of read-transactions is exceeded for a defined time
   */
  R beginNodeReadOnlyTrx(int revision);

  /**
   * Begin a read-only transaction with the revision, which is closest to the given point in time.
   *
   * @param pointInTime the point in time
   * @return instance of a class, which implements the {@link XmlNodeReadOnlyTrx} interface
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of read-transactions is exceeded for a defined time
   */
  R beginNodeReadOnlyTrx(Instant pointInTime);

  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   */
  W beginNodeTrx(AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   */
  W beginNodeTrx(int maxNodes, AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxTime time after which a commit is issued
   * @param timeUnit unit used for time
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(int maxTime, TimeUnit timeUnit, AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @param maxTime time after which a commit is issued
   * @param timeUnit unit used for time
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(int maxNodes, int maxTime, TimeUnit timeUnit,
      AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   */
  W beginNodeTrx();

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   */
  W beginNodeTrx(int maxNodes);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxTime time after which a commit is issued
   * @param timeUnit unit used for time
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(int maxTime, TimeUnit timeUnit);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodeCount count of node modifications after which a commit is issued
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(int maxNodeCount, int maxTime, TimeUnit timeUnit);

  /**
   * Open the path summary to allow iteration (basically implementation of {@link XmlNodeReadOnlyTrx}.
   *
   * @param revision revision key to read from
   * @return {@link PathSummaryReader} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PathSummaryReader openPathSummary(int revision);

  /**
   * Open the path summary to allow iteration (basically implementation of {@link XmlNodeReadOnlyTrx}.
   *
   * @return {@link PathSummaryReader} instance
   * @throws SirixException if can't open path summary
   */
  default PathSummaryReader openPathSummary() {
    return openPathSummary(getMostRecentRevisionNumber());
  }

  /**
   * Get the revision number, which was committed at the closest time to the given point in time.
   *
   * @param pointInTime the point in time
   * @return the revision number, which was committed at the closest time to the given point in time.
   */
  int getRevisionNumber(Instant pointInTime);

  /**
   * Safely close resource session and immediately release all resources. If there are running
   * transactions, they will automatically be closed.
   * <p>
   * This is an idempotent operation and does nothing if the resource session is already closed.
   *
   * @throws SirixException if can't close
   */
  @Override
  void close();

  /**
   * Test if session is closed. Needed for check against database for creation of a new one.
   *
   * @return if session was closed
   */
  boolean isClosed();

  /**
   * Get the most recently commited revision number.
   *
   * @return most recently commited revision number
   */
  int getMostRecentRevisionNumber();

  /**
   * Returns {@link ResourceConfiguration} that is bound to the session.
   *
   * @return {@link ResourceConfiguration} instance bound to session
   */
  ResourceConfiguration getResourceConfig();

  /**
   * Get the index controller.
   *
   * @param <C> Instance of IndexController
   * @param revision the revision number
   * @return the {@link XmlIndexController} instance
   */
  <C extends IndexController<R, W>> C getRtxIndexController(int revision);

  /**
   * Get the index controller.
   *
   * @param <C> The index Controller
   * @param revision the revision
   * @return the {@link XmlIndexController} instance
   */
  <C extends IndexController<R, W>> C getWtxIndexController(int revision);

  /**
   * Get the node reader with the given ID wrapped in an optional.
   *
   * @param ID The ID of the reader.
   * @return The node reader if available.
   */
  Optional<R> getNodeReadTrxByTrxId(Integer ID);

  /**
   * Determines if this resource session has a running read-write transaction.
   *
   * @return {@code true}, if a running read-write transaction is found, {code false} otherwise
   */
  boolean hasRunningNodeWriteTrx();

  /**
   * Get the user associated with the current resource session.
   *
   * @return the user
   */
  Optional<User> getUser();

  /**
   * Get or create a shared read-only transaction for the calling thread at the
   * given revision. The transaction is cached per thread+revision so that
   * multiple items on the same worker thread share a single cursor.
   *
   * @param revision the revision number to read
   * @return a read-only transaction bound to the current thread and revision
   */
  R getOrCreateSharedReadOnlyTrx(int revision);

  /**
   * Close and remove all shared read-only transactions for the given revision.
   * Called when a thread-safe proxy is closed to free per-worker cursors.
   *
   * @param revision the revision whose shared transactions should be closed
   */
  void closeSharedReadOnlyTrxs(int revision);

  /**
   * Get cache for index-structures.
   *
   * @return the cache
   */
  Cache<RBIndexKey, Node> getIndexCache();
}
