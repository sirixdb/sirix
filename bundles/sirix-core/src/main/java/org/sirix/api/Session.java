/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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

package org.sirix.api;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;

import org.sirix.access.IndexController;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * <h1>Session</h1>
 * 
 * <h2>Description</h2>
 * 
 * <p>
 * Each <code>Database</code> is bound to multiple instances implementing
 * <code>ISession</code>. Transactions can then be started from this instance.
 * There can only be one <code>IWriteTransaction</code> at the time. However,
 * multiple <code>IReadTransactions</code> can coexist concurrently.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Marc Kramis, Seabix GmbH
 * @author Johannes Lichtenberger
 */
public interface Session extends AutoCloseable {

	/**
	 * Get the {@link Database} this session is bound to.
	 * 
	 * @return {@link Database} this session is bound to
	 */
	Database getDatabase();

	/**
	 * Begin a new {@link PageReadTrx}.
	 * 
	 * @return new {@link PageReadTrx} instance
	 * @throws SirixException
	 *           if Sirix fails to create a new instance
	 */
	PageReadTrx beginPageReadTrx() throws SirixException;

	/**
	 * Begin a new {@link PageReadTrx}.
	 * 
	 * @param revision
	 *          revision number
	 * @return new {@link PageReadTrx} instance
	 * @throws IllegalArgumentException
	 *           if {@code revision < 0}
	 * @throws SirixException
	 *           if Sirix fails to create a new instance
	 */
	PageReadTrx beginPageReadTrx(@Nonnegative int revision) throws SirixException;

	/**
	 * Begin a new {@link PageWriteTrx}.
	 * 
	 * @param pRevision
	 *          revision number
	 * @return new {@link PageWriteTrx} instance
	 * @throws SirixException
	 *           if Sirix fails to create a new instance
	 */
	PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx()
			throws SirixException;

	/**
	 * Begin a new {@link PageWriteTrx}.
	 * 
	 * @param revision
	 *          revision number
	 * @return new {@link PageWriteTrx} instance
	 * @throws SirixException
	 *           if Sirix fails to create a new instance
	 * @throws IllegalArgumentException
	 *           if {@code revision < 0}
	 */
	PageWriteTrx<Long, Record, UnorderedKeyValuePage> beginPageWriteTrx(
			@Nonnegative int revision) throws SirixException;

	/**
	 * Begin a read-only transaction on the latest committed revision.
	 * 
	 * @throws SirixException
	 *           if can't begin Read Transaction
	 * @return {@link NodeReadTrx} instance
	 */
	NodeReadTrx beginNodeReadTrx() throws SirixException;

	/**
	 * Begin a read-only transaction on the given revision number.
	 * 
	 * @param revision
	 *          revision to read from denoted by the revision number.
	 * @throws SirixException
	 *           if can't begin Read Transaction
	 * @throws IllegalArgumentException
	 *           if {@code revision < 0}
	 * @return {@link NodeReadTrx} instance
	 */
	NodeReadTrx beginNodeReadTrx(@Nonnegative int revision) throws SirixException;

	/**
	 * Begin exclusive read/write transaction without auto commit.
	 * 
	 * @throws SirixException
	 *           if can't begin Write Transaction
	 * @return {@link NodeWriteTrx} instance
	 */
	NodeWriteTrx beginNodeWriteTrx() throws SirixException;

	/**
	 * Begin exclusive read/write transaction with auto commit.
	 * 
	 * @param maxNodes
	 *          count of node modifications after which a commit is issued
	 * @param timeUnit
	 *          unit used for time
	 * @param maxTime
	 *          time after which a commit is issued
	 * @throws SirixException
	 *           if can't begin Write Transaction
	 * @throws IllegalArgumentException
	 *           if {@code maxNodes < 0}
	 * @throws NullPointerException
	 *           if {@code timeUnit} is {@code null}
	 * @return {@link NodeWriteTrx} instance
	 */
	NodeWriteTrx beginNodeWriteTrx(final @Nonnegative int maxNodes,
			final TimeUnit timeUnit, final int maxTime) throws SirixException;

	/**
	 * Commit all running {@link NodeWriteTrx}s.
	 * 
	 * @return this session reference
	 * @throws SirixException
	 *           if commiting fails
	 */
	Session commitAll() throws SirixException;

	/**
	 * Open the path summary to allow iteration (basically implementation of
	 * {@link NodeReadTrx}.
	 * 
	 * @param revision
	 *          revision key to read from
	 * @return {@link PathSummaryReader} instance
	 * @throws SirixException
	 *           if can't open path summary
	 * @throws IllegalArgumentException
	 *           if {@code revision < 0}
	 */
	PathSummaryReader openPathSummary(@Nonnegative int revision)
			throws SirixException;

	/**
	 * Open the path summary to allow iteration (basically implementation of
	 * {@link NodeReadTrx}.
	 * 
	 * @return {@link PathSummaryReader} instance
	 * @throws SirixException
	 *           if can't open path summary
	 */
	PathSummaryReader openPathSummary() throws SirixException;

	/**
	 * Safely close session and immediately release all resources. If there are
	 * running transactions, they will automatically be closed.
	 * 
	 * This is an idempotent operation and does nothing if the session is already
	 * closed.
	 * 
	 * @throws SirixException
	 *           if can't close session
	 */
	@Override
	void close() throws SirixException;

	/**
	 * Test if session is closed. Needed for check against database for creation
	 * of a new one.
	 * 
	 * @return if session was closed
	 */
	boolean isClosed();

	/**
	 * Returns user that is bound to the session.
	 * 
	 * @return current session bound user.
	 */
	String getUser();

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
	 * Get available number of {@link NodeReadTrx}s.
	 * 
	 * @return available number of {@link NodeReadTrx}s
	 */
	int getAvailableNodeReadTrx();

	/**
	 * Get available number of {@link NodeWriteTrx}s.
	 * 
	 * @return available number of {@link NodeWriteTrx}s
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
}
