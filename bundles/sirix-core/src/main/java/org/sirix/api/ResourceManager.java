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

package org.sirix.api;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.access.IndexController;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * <h1>ResourceManager</h1>
 *
 * <h2>Description</h2>
 *
 * <p>
 * Each resource is bound to a {@code ResourceManager}. Readers/Writers can then be started from
 * this instance. There can only be one {@code XdmNodeWriter} at the time. However, multiple
 * {@code XdmNodeReader}s can coexist concurrently.
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * @author Johannes Lichtenberger
 */
public interface ResourceManager extends AutoCloseable {

  /**
   * Get the {@link Database} this session is bound to.
   *
   * @return {@link Database} this session is bound to
   */
  Database getDatabase();

  /**
   * Get the single node writer if available, wrapped in an {@link Optional}.
   *
   * @return The single node writer if available.
   */
  Optional<XdmNodeWriteTrx> getXdmNodeWriteTrx();

  /**
   * Begin a new {@link PageReadTrx}.
   *
   * @return new {@link PageReadTrx} instance
   */
  PageReadTrx beginPageReadTrx();

  /**
   * Begin a new {@link PageReadTrx}.
   *
   * @param revision revision number
   * @return new {@link PageReadTrx} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PageReadTrx beginPageReadTrx(@Nonnegative int revision);

  /**
   * Begin a new {@link PageWriteTrx}.
   *
   * @param pRevision revision number
   * @return new {@link PageWriteTrx} instance
   * @throws SirixException if Sirix fails to create a new instance
   */
  PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx();

  /**
   * Begin a new {@link PageWriteTrx}.
   *
   * @param revision revision number
   * @return new {@link PageWriteTrx} instance
   * @throws SirixException if Sirix fails to create a new instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx(@Nonnegative int revision);

  /**
   * Begin a read-only transaction on the latest committed revision.
   *
   * @throws SirixException if can't begin Read Transaction
   * @return {@link XdmNodeReadTrx} instance
   */
  XdmNodeReadTrx beginNodeReadTrx() throws SirixException;

  /**
   * Begin a read-only transaction on the given revision number.
   *
   * @param revision revision to read from denoted by the revision number.
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of read-transactions is exceeded for a defined time
   * @return {@link XdmNodeReadTrx} instance
   */
  XdmNodeReadTrx beginNodeReadTrx(@Nonnegative int revision);

  /**
   * Begin a read-only transaction with the revision, which is closest to the given point in time.
   *
   * @param pointInTime the point in time
   * @throws IllegalArgumentException if {@code revision < 0}
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of read-transactions is exceeded for a defined time
   * @return {@link XdmNodeReadTrx} instance
   */
  XdmNodeReadTrx beginNodeReadTrx(@Nonnull Instant pointInTime);

  /**
   * Begin exclusive read/write transaction without auto commit.
   *
   * @param trx the transaction to use
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx();

  /**
   * Get the revision number, which was committed at the closest time to the given point in time.
   *
   * @param pointInTime the point in time
   * @return the revision number, which was committed at the closest time to the given point in
   *         time.
   */
  int getRevisionNumber(@Nonnull Instant pointInTime);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final @Nonnegative int maxNodes);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxTime < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final TimeUnit timeUnit, final int maxTime);

  /**
   * Begin exclusive read/write transaction with auto commit.
   *
   * @param maxNodes count of node modifications after which a commit is issued
   * @param timeUnit unit used for time
   * @param maxTime time after which a commit is issued
   * @throws SirixThreadedException if the thread is interrupted
   * @throws SirixUsageException if the number of write-transactions is exceeded for a defined time
   * @throws IllegalArgumentException if {@code maxNodes < 0}
   * @throws NullPointerException if {@code timeUnit} is {@code null}
   * @return {@link XdmNodeWriteTrx} instance
   */
  XdmNodeWriteTrx beginNodeWriteTrx(final @Nonnegative int maxNodes, final TimeUnit timeUnit,
      final int maxTime);

  /**
   * Open the path summary to allow iteration (basically implementation of {@link XdmNodeReadTrx}.
   *
   * @param revision revision key to read from
   * @return {@link PathSummaryReader} instance
   * @throws IllegalArgumentException if {@code revision < 0}
   */
  PathSummaryReader openPathSummary(@Nonnegative int revision);

  /**
   * Open the path summary to allow iteration (basically implementation of {@link XdmNodeReadTrx}.
   *
   * @return {@link PathSummaryReader} instance
   * @throws SirixException if can't open path summary
   */
  PathSummaryReader openPathSummary();

  /**
   * Safely close resource manager and immediately release all resources. If there are running
   * transactions, they will automatically be closed.
   *
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
   * Returns {@link ResourceConfiguration} that is bound to the session.
   *
   * @return {@link ResourceConfiguration} instance bound to session
   */
  ResourceConfiguration getResourceConfig();

  /**
   * Get the most recently commited revision number.
   *
   * @return most recently commited revision number
   */
  int getMostRecentRevisionNumber();

  /**
   * Get available number of {@link XdmNodeReadTrx}s.
   *
   * @return available number of {@link XdmNodeReadTrx}s
   */
  int getAvailableNodeReadTrx();

  /**
   * Get available number of {@link XdmNodeWriteTrx}s.
   *
   * @return available number of {@link XdmNodeWriteTrx}s
   */
  int getAvailableNodeWriteTrx();

  /**
   * Get the index controller.
   *
   * @return the {@link IndexController} instance
   */
  IndexController getRtxIndexController(int revision);

  /**
   * Get the index controller.
   *
   * @return the {@link IndexController} instance
   */
  IndexController getWtxIndexController(int revision);

  /**
   * Get the node reader with the given ID wrapped in an optional.
   *
   * @param ID The ID of the reader.
   * @return The node reader if available.
   */
  Optional<XdmNodeReadTrx> getXdmNodeReadTrx(long ID);
}
