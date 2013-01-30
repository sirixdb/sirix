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
package org.sirix;

import java.io.File;

import org.sirix.TestHelper.PATHS;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;

/**
 * Generating a standard resource within the {@link PATHS#PATH1} path. It also
 * generates a standard resource defined within {@link TestHelper#RESOURCE}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 * 
 */
public class Holder {

	/** {@link Database} implementation. */
	private Database mDatabase;

	/** {@link Session} implementation. */
	private Session mSession;

	/** {@link NodeReadTrx} implementation. */
	private NodeReadTrx mRtx;

	/** {@link NodeWriteTrx} implementation. */
	private NodeWriteTrx mWtx;

	/**
	 * Generate a session with deweyIDs for resources.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateDeweyIDSession() throws SirixException {
		final File file = PATHS.PATH1.getFile();
		final DatabaseConfiguration config = new DatabaseConfiguration(file);
		if (!file.exists()) {
			Databases.createDatabase(config);
		}
		final Database database = Databases.openDatabase(PATHS.PATH1.getFile());
		database.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH1.getConfig()).useDeweyIDs().build());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final Holder holder = new Holder();
		holder.setDatabase(database);
		holder.setSession(session);
		return holder;
	}
	
	/**
	 * Generate a session.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateSession() throws SirixException {
		final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final Holder holder = new Holder();
		holder.setDatabase(database);
		holder.setSession(session);
		return holder;
	}

	/**
	 * Generate a {@link NodeWriteTrx}.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateWtx() throws SirixException {
		final Holder holder = generateSession();
		final NodeWriteTrx wtx = holder.mSession.beginNodeWriteTrx();
		holder.setWtx(wtx);
		return holder;
	}

	/**
	 * Generate a {@link NodeReadTrx}.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateRtx() throws SirixException {
		final Holder holder = generateSession();
		final NodeReadTrx rtx = holder.mSession.beginNodeReadTrx();
		holder.setRtx(rtx);
		return holder;
	}

	/**
	 * Close the database, session, read transaction and/or write transaction.
	 * 
	 * @throws SirixException
	 *           if an error occurs
	 */
	public void close() throws SirixException {
		if (mRtx != null && !mRtx.isClosed()) {
			mRtx.close();
		}
		if (mWtx != null && !mWtx.isClosed()) {
			mWtx.abort();
			mWtx.close();
		}
		if (mSession != null && !mSession.isClosed()) {
			mSession.close();
		}
		if (mDatabase != null) {
			mDatabase.close();
		}
	}

	/**
	 * Get the {@link Database} handle.
	 * 
	 * @return {@link Database} handle
	 */
	public Database getDatabase() {
		return mDatabase;
	}

	/**
	 * Get the {@link Session} handle.
	 * 
	 * @return {@link Session} handle
	 */
	public Session getSession() {
		return mSession;
	}

	/**
	 * Get the {@link NodeReadTrx} handle.
	 * 
	 * @return {@link NodeReadTrx} handle
	 */
	public NodeReadTrx getRtx() {
		return mRtx;
	}

	/**
	 * Get the {@link NodeWriteTrx} handle.
	 * 
	 * @return {@link NodeWriteTrx} handle
	 */
	public NodeWriteTrx getWtx() {
		return mWtx;
	}

	/**
	 * Set the working {@link NodeWriteTrx}.
	 * 
	 * @param pWtx
	 *          {@link NodeWriteTrx} instance
	 */
	private void setWtx(final NodeWriteTrx pWtx) {
		mWtx = pWtx;
	}

	/**
	 * Set the working {@link NodeReadTrx}.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} instance
	 */
	private void setRtx(final NodeReadTrx pRtx) {
		mRtx = pRtx;
	}

	/**
	 * Set the working {@link Session}.
	 * 
	 * @param pRtx
	 *          {@link NodeReadTrx} instance
	 */
	private void setSession(final Session pSession) {
		mSession = pSession;
	}

	/**
	 * Set the working {@link Database}.
	 * 
	 * @param pRtx
	 *          {@link Database} instance
	 */
	private void setDatabase(final Database pDatabase) {
		mDatabase = pDatabase;
	}

}
