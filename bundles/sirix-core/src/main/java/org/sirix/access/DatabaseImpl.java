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

package org.sirix.access;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.Files;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;

/**
 * This class represents one concrete database for enabling several
 * {@link Session} instances.
 * 
 * @see Database
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class DatabaseImpl implements Database {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DatabaseImpl.class));

	/** Unique ID of a resource. */
	private final AtomicLong mResourceID = new AtomicLong();

	/** Central repository of all running sessions. */
	private final ConcurrentMap<File, Set<Session>> mSessions;
	
	/** Read semaphores shared for each resource. */
	private final ConcurrentMap<File, Semaphore> mReadSemaphores;
	
	/** Write semaphores shared for each resource. */
	private final ConcurrentMap<File, Semaphore> mWriteSemaphores;
	
	/** Central repository of all resource-ID/ResourceConfiguration tuples. */
	private final BiMap<Long, String> mResources;

	/** DatabaseConfiguration with fixed settings. */
	private final DatabaseConfiguration mDBConfig;

	/**
	 * Package private constructor.
	 * 
	 * @param dbConfig
	 *          {@link ResourceConfiguration} reference to configure the
	 *          {@link Database}
	 * @throws SirixException
	 *           if something weird happens
	 */
	DatabaseImpl(final DatabaseConfiguration dbConfig) {
		mDBConfig = checkNotNull(dbConfig);
		mSessions = new ConcurrentHashMap<>();
		mResources = Maps.synchronizedBiMap(HashBiMap.<Long, String> create());
		mReadSemaphores = new ConcurrentHashMap<>();
		mWriteSemaphores = new ConcurrentHashMap<>();
	}

	// //////////////////////////////////////////////////////////
	// START Creation/Deletion of Resources /////////////////////
	// //////////////////////////////////////////////////////////

	@Override
	public synchronized boolean createResource(
			final ResourceConfiguration resConfig) throws SirixIOException {
		boolean returnVal = true;
		final File path = new File(new File(mDBConfig.getFile().getAbsoluteFile(),
				DatabaseConfiguration.Paths.DATA.getFile().getName()),
				resConfig.mPath.getName());
		// If file is existing, skip.
		if (path.exists()) {
			return false;
		} else {
			returnVal = path.mkdir();
			if (returnVal) {
				// Creation of the folder structure.
				for (final ResourceConfiguration.Paths paths : ResourceConfiguration.Paths
						.values()) {
					final File toCreate = new File(path, paths.getFile().getName());
					if (paths.isFolder()) {
						returnVal = toCreate.mkdir();
					} else {
						try {
							returnVal = ResourceConfiguration.Paths.INDEXES.getFile()
									.getName().equals(paths.getFile().getName()) ? true
									: toCreate.createNewFile();
						} catch (final IOException e) {
							Files.recursiveRemove(path.toPath());
							throw new SirixIOException(e);
						}
					}
					if (!returnVal) {
						break;
					}
				}
			}
			// Serialization of the config.
			mResourceID.set(mDBConfig.getMaxResourceID());
			ResourceConfiguration.serialize(resConfig.setID(mResourceID
					.getAndIncrement()));
			mDBConfig.setMaximumResourceID(mResourceID.get());
			mResources.forcePut(mResourceID.get(), resConfig.getResource().getName());

			if (returnVal) {
				// If everything was correct so far, initialize storage.
				try {
					try (final Session session = this
							.getSession(new SessionConfiguration.Builder(resConfig
									.getResource().getName()).build());
							final NodeWriteTrx wtx = session.beginNodeWriteTrx();) {
						wtx.commit();
					}
				} catch (final SirixException e) {
					LOGWRAPPER.error(e.getMessage(), e);
					returnVal = false;
				}
			}

			if (!returnVal) {
				// If something was not correct, delete the partly created substructure.
				Files.recursiveRemove(resConfig.mPath.toPath());
			}

			return returnVal;
		}
	}

	@Override
	public synchronized Database truncateResource(final String name) {
		final File resourceFile = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.DATA.getFile().getName()), name);
		// Check that database must be closed beforehand.
		if (!mSessions.containsKey(resourceFile)) {
			// If file is existing and folder is a Sirix-dataplace, delete it.
			if (resourceFile.exists()
					&& ResourceConfiguration.Paths.compareStructure(resourceFile) == 0) {
				// Instantiate the database for deletion.
				try {
					Files.recursiveRemove(resourceFile.toPath());
				} catch (final SirixIOException e) {
					LOGWRAPPER.error(e.getMessage(), e);
				}
			}
		}

		return this;
	}

	// //////////////////////////////////////////////////////////
	// END Creation/Deletion of Resources ///////////////////////
	// //////////////////////////////////////////////////////////

	// //////////////////////////////////////////////////////////
	// START resource name <=> ID handling //////////////////////
	// //////////////////////////////////////////////////////////

	@Override
	public synchronized String getResourceName(final @Nonnegative long id) {
		checkArgument(id >= 0, "pID must be >= 0!");
		return mResources.get(id);
	}

	@Override
	public synchronized long getResourceID(final String name) {
		return mResources.inverse().get(checkNotNull(name));
	}

	// //////////////////////////////////////////////////////////
	// END resource name <=> ID handling ////////////////////////
	// //////////////////////////////////////////////////////////

	// //////////////////////////////////////////////////////////
	// START DB-Operations //////////////////////////////////////
	// //////////////////////////////////////////////////////////

	@Override
	public synchronized Session getSession(final SessionConfiguration sessionConf)
			throws SirixException {
		final File resourceFile = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.DATA.getFile().getName()),
				sessionConf.getResource());
		Set<Session> sessions = mSessions.get(resourceFile);		
		if (sessions == null) {
			sessions = Collections.newSetFromMap(new ConcurrentHashMap<>());
		} else {
			final Optional<Session> optionalSession = sessions.stream().filter(new SessionPredicate(sessionConf)).findFirst();
	
			if (optionalSession.isPresent()) {
				return optionalSession.get();
			}
		}
			
		if (!resourceFile.exists()) {
			throw new SirixUsageException(
					"Resource could not be opened (since it was not created?) at location",
					resourceFile.toString());
		}
		final ResourceConfiguration resourceConfig = ResourceConfiguration
				.deserialize(resourceFile);

		// Resource of session must be associated to this database
		assert resourceConfig.mPath.getParentFile().getParentFile().equals(mDBConfig.getFile());
		if (!mReadSemaphores.containsKey(resourceFile))
			mReadSemaphores.put(resourceFile, new Semaphore(512));
		if (!mWriteSemaphores.containsKey(resourceFile))
			mWriteSemaphores.put(resourceFile, new Semaphore(1));
		final Session session = new SessionImpl(this, resourceConfig, sessionConf);
		sessions.add(session);
		mSessions.put(resourceFile, sessions);
		return session;
	}

	@Override
	public synchronized void close() throws SirixException {
		// Close all sessions.
		for (final Set<Session> sessions : mSessions.values()) {
			for (final Session session : sessions) {
				if (!session.isClosed()) {
					session.close();
				}
			}
		}

		// Remove from database mapping.
		Databases.removeDatabase(mDBConfig.getFile());

		// Remove lock file.
		Files.recursiveRemove(new File(mDBConfig.getFile().getAbsoluteFile(),
				DatabaseConfiguration.Paths.LOCK.getFile().getName()).toPath());
	}

	@Override
	public DatabaseConfiguration getDatabaseConfig() {
		return mDBConfig;
	}

	@Override
	public synchronized boolean existsResource(final String pResourceName) {
		final File resourceFile = new File(new File(mDBConfig.getFile(),
				DatabaseConfiguration.Paths.DATA.getFile().getName()), pResourceName);
		return resourceFile.exists()
				&& ResourceConfiguration.Paths.compareStructure(resourceFile) == 0 ? true
				: false;
	}

	@Override
	public String[] listResources() {
		return new File(mDBConfig.getFile(), DatabaseConfiguration.Paths.DATA
				.getFile().getName()).list();
	}

	@Override
	public synchronized Database commitAll() throws SirixException {
		// Commit all sessions.
		for (final Set<Session> sessions : mSessions.values()) {
			for (final Session session : sessions) {
				if (!session.isClosed()) {
					session.commitAll();
				}
			}
		}
		return this;
	}

	/**
	 * Closing a resource. This callback is necessary due to centralized handling
	 * of all sessions within a database.
	 * 
	 * @param resourceFile
	 *          {@link File} to be closed
	 * @param sessionConfig
	 *          the session configuration
	 * @return {@code true} if close successful, {@code false} otherwise
	 */
	protected boolean removeSession(final File resourceFile, final SessionConfiguration sessionConfig) {
		final Set<Session> sessions = mSessions.get(resourceFile);		
		if (sessions == null || sessions.isEmpty() || sessions.size() == 1) {
			return mSessions.remove(resourceFile) == null ? false : true;
		}
		
		final Optional<Session> optionalSession = sessions.stream().filter(new SessionPredicate(sessionConfig)).findFirst();
		if (optionalSession.isPresent()) {
			return mSessions.get(resourceFile).remove(optionalSession.get());
		}
		return false;
	}

	// //////////////////////////////////////////////////////////
	// END DB-Operations ////////////////////////////////////////
	// //////////////////////////////////////////////////////////

	// //////////////////////////////////////////////////////////
	// START general methods ////////////////////////////////////
	// //////////////////////////////////////////////////////////

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("dbConfig", mDBConfig).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mDBConfig);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof DatabaseImpl) {
			final DatabaseImpl other = (DatabaseImpl) obj;
			return other.mDBConfig.equals(mDBConfig);
		}
		return false;
	}

	// //////////////////////////////////////////////////////////
	// END general methods //////////////////////////////////////
	// //////////////////////////////////////////////////////////
	
	private static class SessionPredicate implements Predicate<Session> {
		
		private final SessionConfiguration mSessionConf;

		private SessionPredicate(final SessionConfiguration sessionConf) {
			mSessionConf = sessionConf;
		}
	
		@Override
		public boolean test(final Session session) {
			return session.getSessionConfiguration().equals(mSessionConf);
		}
	}

	Semaphore getReadSemaphore(File resourceFile) {
		return mReadSemaphores.get(resourceFile);
	}
	
	Semaphore getWriteSemaphore(File resourceFile) {
		return mWriteSemaphores.get(resourceFile);
	}
}
