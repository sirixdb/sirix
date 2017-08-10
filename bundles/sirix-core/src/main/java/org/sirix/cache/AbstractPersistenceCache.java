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

package org.sirix.cache;

import java.io.File;

import javax.annotation.Nonnegative;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;

import com.sleepycat.je.Environment;

/**
 * Abstract class for holding all persistence caches. Each instance of this class stores the data in
 * a place related to the {@link DatabaseConfiguration} at a different subfolder.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public abstract class AbstractPersistenceCache<K, V> implements Cache<K, V> {

	/**
	 * Place to store the data.
	 */
	protected final File mPlace;

	/**
	 * Determines if directory has been created.
	 */
	private final boolean mCreated;

	/**
	 * Constructor with the place to store the data.
	 * 
	 * @param file {@link File} which holds the place to store the data
	 * @param revision revision number
	 * @param logType type of log to append to the path of the log
	 */
	protected AbstractPersistenceCache(final File file, final @Nonnegative int revision,
			final String logType) {
		mPlace = new File(
				new File(new File(file, ResourceConfiguration.Paths.TRANSACTION_LOG.getFile().getName()),
						Integer.toString(revision)),
				logType);
		mCreated = mPlace.mkdirs();
	}

	/**
	 * Remove an existing database from the environment if it's not removed.
	 * 
	 * @param dbName database name
	 * @param environment environment handle
	 * @return {@code true} if it removed at least one database, {@code false} otherwise
	 */
	protected boolean removeExistingDatabase(final String dbName, final Environment environment) {
		// Make a database within that environment.
		boolean removed = false;
		if (mPlace.list().length == 0) {
			for (final String name : environment.getDatabaseNames()) {
				if (dbName.equals(name)) {
					environment.removeDatabase(null, dbName);
					environment.close();
					removed = true;
				}
			}
		}
		return removed;
	}

	/**
	 * Determines if the directory is newly created or not.
	 * 
	 * @return {@code true} if it is newly created, {@code false} otherwise
	 */
	public boolean isCreated() {
		return mCreated;
	}

	@Override
	public final void put(final K key, final V page) {
		try {
			putPersistent(key, page);
		} catch (final SirixIOException exc) {
			throw new IllegalStateException(exc);
		}
	}

	@Override
	public final void clear() {
		try {
			clearPersistent();

			// for (final File file : mPlace.listFiles()) {
			// if (!file.delete()) {
			// throw new SirixIOException("Couldn't delete!");
			// }
			// }
			// if (!mPlace.delete()) {
			// throw new SirixIOException("Couldn't delete!");
			// }
		} catch (final SirixIOException e) {
			throw new IllegalStateException(e.getCause());
		}
	}

	@Override
	public final V get(final K pKey) {
		try {
			return getPersistent(pKey);
		} catch (final SirixIOException e) {
			throw new IllegalStateException(e.getCause());
		}
	}

	/**
	 * Clearing a persistent cache.
	 * 
	 * @throws SirixIOException if something odd happens
	 */
	public abstract void clearPersistent() throws SirixIOException;

	/**
	 * Putting a page into a persistent log.
	 * 
	 * @param key to be put
	 * @param page to be put
	 * @throws SirixIOException if something odd happens
	 */
	public abstract void putPersistent(final K key, final V page) throws SirixIOException;

	/**
	 * Getting a NodePage from the persistent cache.
	 * 
	 * @param key to get the page
	 * @return the Nodepage to be fetched
	 * @throws SirixIOException if something odd happens.
	 */
	public abstract V getPersistent(final K key) throws SirixIOException;
}
