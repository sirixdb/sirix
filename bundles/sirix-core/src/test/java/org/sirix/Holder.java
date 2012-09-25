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

import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;

/**
 * Generating a standard resource within the {@link PATHS#PATH1} path. It also
 * generates a standard resource defined within {@link TestHelper#RESOURCE}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class Holder {

	/** {@link IDatabase} implementation. */
	private IDatabase mDatabase;

	/** {@link ISession} implementation. */
	private ISession mSession;

	/** {@link INodeReadTrx} implementation. */
	private INodeReadTrx mRtx;

	/** {@link INodeWriteTrx} implementation. */
	private INodeWriteTrx mWtx;

	/**
	 * Generate a session.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateSession() throws SirixException {
		final IDatabase database = TestHelper.getDatabase(PATHS.PATH1.getFile());
		database.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH1.getConfig()).build());
		final ISession session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final Holder holder = new Holder();
		holder.setDatabase(database);
		holder.setSession(session);
		return holder;
	}

	/**
	 * Generate a {@link INodeWriteTrx}.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateWtx() throws SirixException {
		final Holder holder = generateSession();
		final INodeWriteTrx wtx = holder.mSession.beginNodeWriteTrx();
		holder.setWtx(wtx);
		return holder;
	}

	/**
	 * Generate a {@link INodeReadTrx}.
	 * 
	 * @return this holder instance
	 * @throws SirixException
	 *           if an error occurs
	 */
	public static Holder generateRtx() throws SirixException {
		final Holder holder = generateSession();
		final INodeReadTrx rtx = holder.mSession.beginNodeReadTrx();
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
	 * Get the {@link IDatabase} handle.
	 * 
	 * @return {@link IDatabase} handle
	 */
	public IDatabase getDatabase() {
		return mDatabase;
	}

	/**
	 * Get the {@link ISession} handle.
	 * 
	 * @return {@link ISession} handle
	 */
	public ISession getSession() {
		return mSession;
	}

	/**
	 * Get the {@link INodeReadTrx} handle.
	 * 
	 * @return {@link INodeReadTrx} handle
	 */
	public INodeReadTrx getRtx() {
		return mRtx;
	}

	/**
	 * Get the {@link INodeWriteTrx} handle.
	 * 
	 * @return {@link INodeWriteTrx} handle
	 */
	public INodeWriteTrx getWtx() {
		return mWtx;
	}

	/**
	 * Set the working {@link INodeWriteTrx}.
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} instance
	 */
	private void setWtx(final INodeWriteTrx pWtx) {
		this.mWtx = pWtx;
	}

	/**
	 * Set the working {@link INodeReadTrx}.
	 * 
	 * @param pRtx
	 *          {@link INodeReadTrx} instance
	 */
	private void setRtx(final INodeReadTrx pRtx) {
		this.mRtx = pRtx;
	}

	/**
	 * Set the working {@link ISession}.
	 * 
	 * @param pRtx
	 *          {@link INodeReadTrx} instance
	 */
	private void setSession(final ISession pSession) {
		this.mSession = pSession;
	}

	/**
	 * Set the working {@link IDatabase}.
	 * 
	 * @param pRtx
	 *          {@link IDatabase} instance
	 */
	private void setDatabase(final IDatabase pDatabase) {
		this.mDatabase = pDatabase;
	}

}
