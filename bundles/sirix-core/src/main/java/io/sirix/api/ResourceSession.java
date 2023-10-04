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
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
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
 * Each resource is bound to a {@code ResourceManager}. Reader-only transactions and the single read/write transaction can then be started from
 * this instance. There can only be one write transaction at a time. However, multiple read-only
 * transactions can coexist concurrently.
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
   * @param fromRevision start revision (must be bigger than {@code toRevision})
   * @param toRevision   start revision (must be lower than {@code fromRevision})
   * @return the history
   */
  List<RevisionInfo> getHistory(int fromRevision, int toRevision);

  /**
   * Get the single node writer if available, wrapped in an {@link Optional}.
   *
   * @return The single node writer if available.
   */
  Optional<W> getNodeTrx();

  /**
   * Begin a new {@link PageReadOnlyTrx}.
   *
   * @return new {@link PageReadOnlyTrx} instance
   */
  default PageReadOnlyTrx beginPageReadOnlyTrx() {
    return beginPageReadOnlyTrx(getMostRecentRevisionNumber());
  }

  /**
   * Begin a new {@link PageReadOnlyTrx}.
   *
   * @param revision revision number
   * @return new {@link PageReadOnlyTrx} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PageReadOnlyTrx beginPageReadOnlyTrx(@NonNegative int revision);

  /**
   * Begin a new {@link PageTrx}.
   *
   * @return new {@link PageTrx} instance
   * @throws SirixException if Sirix fails to create a new instance
   */
  default PageTrx beginPageTrx() {
    return beginPageTrx(getMostRecentRevisionNumber());
  }

  /**
   * Begin a new {@link PageTrx}.
   *
   * @param revision revision number
   * @return new {@link PageTrx} instance
   * @throws SirixException           if Sirix fails to create a new instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PageTrx beginPageTrx(@NonNegative int revision);

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
   * Begin a read-only transaction on the given revision number.
   *
   * @param revision revision to read from denoted by the revision number.
   * @return instance of a class, which implements the {@link XmlNodeReadOnlyTrx} interface
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of read-transactions is exceeded for a defined time
   */
  R beginNodeReadOnlyTrx(@NonNegative int revision);

  /**
   * Begin a read-only transaction with the revision, which is closest to the given point in time.
   *
   * @param pointInTime the point in time
   * @return instance of a class, which implements the {@link XmlNodeReadOnlyTrx} interface
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of read-transactions is exceeded for a defined time
   */
  R beginNodeReadOnlyTrx(@NonNull Instant pointInTime);

  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException    if the number of write-transactions is exceeded for a defined time
   */
  W beginNodeTrx(@NonNull AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes         count of node modifications after which a commit is issued
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   */
  W beginNodeTrx(@NonNegative int maxNodes, @NonNull AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxTime          time after which a commit is issued
   * @param timeUnit         unit used for time
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException     if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(@NonNegative int maxTime, @NonNull TimeUnit timeUnit, @NonNull AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes         count of node modifications after which a commit is issued
   * @param maxTime          time after which a commit is issued
   * @param timeUnit         unit used for time
   * @param afterCommitState determines if the transaction keeps running after committing or not
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException     if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(@NonNegative int maxNodes, @NonNegative int maxTime, @NonNull TimeUnit timeUnit, @NonNull AfterCommitState afterCommitState);

  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException    if the number of write-transactions is exceeded for a defined time
   */
  W beginNodeTrx();

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   */
  W beginNodeTrx(@NonNegative int maxNodes);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxTime  time after which a commit is issued
   * @param timeUnit unit used for time
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException     if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(@NonNegative int maxTime, @NonNull TimeUnit timeUnit);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodeCount count of node modifications after which a commit is issued
   * @param timeUnit unit used for time
   * @param maxTime  time after which a commit is issued
   * @return instance of a class, which implements the {@link XmlNodeTrx} interface
   * @throws SirixThreadedException   if the thread is interrupted
   * @throws SirixUsageException      if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException     if {@code timeUnit} is {@code null}
   */
  W beginNodeTrx(@NonNegative int maxNodeCount, @NonNegative int maxTime, @NonNull TimeUnit timeUnit);

  /**
   * Open the path summary to allow iteration (basically implementation of {@link XmlNodeReadOnlyTrx}.
   *
   * @param revision revision key to read from
   * @return {@link PathSummaryReader} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PathSummaryReader openPathSummary(@NonNegative int revision);

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
  int getRevisionNumber(@NonNull Instant pointInTime);

  /**
   * Safely close resource manager and immediately release all resources. If there are running
   * transactions, they will automatically be closed.
   * <p>
   * This is an idempotent operation and does nothing if the resource manager is already closed.
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
   * @param <C>      Instance of IndexController
   * @param revision the revision number
   * @return the {@link XmlIndexController} instance
   */
  <C extends IndexController<R, W>> C getRtxIndexController(int revision);

  /**
   * Get the index controller.
   *
   * @param <C>      The index Controller
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
  Optional<R> getNodeReadTrxByTrxId(Long ID);

  /**
   * Determines if this resource manager has a running read-write transaction.
   *
   * @return {@code true}, if a running read-write transaction is found, {code false} otherwise
   */
  boolean hasRunningNodeWriteTrx();

//  /**
//   * Get a read-only transaction for a specific revision, if it exists.
//   *
//   * @param revision the revision number
//   * @return optional read-only transaction
//   */
//  Optional<R> getNodeReadTrxByRevisionNumber(final int revision);

  /**
   * Get the user associated with the current ressource manager session.
   *
   * @return the user
   */
  Optional<User> getUser();

  /**
   * Get cache for index-structures.
   *
   * @return the cache
   */
  Cache<RBIndexKey, Node> getIndexCache();
}
