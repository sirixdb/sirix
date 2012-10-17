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

package org.sirix.gui;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;

import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.Shredder;
import org.sirix.service.xml.shredder.ShredderCommit;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.shredder.XMLUpdateShredder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines how to shred.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
enum Shredding {

	/** Determines normal shredding. */
	NORMAL {
		@Override
		boolean shred(final File pSource, final File pTarget) {
			return shredder(checkNotNull(pSource), checkNotNull(pTarget),
					EType.NORMAL);
		}
	},

	/** Determines update only shredding. */
	UPDATEONLY {
		@Override
		boolean shred(final File pSource, final File pTarget) {
			return shredder(checkNotNull(pSource), checkNotNull(pTarget),
					EType.UPDATE);
		}
	};

	/** Logger. */
	private static final Logger LOGWRAPPER = LoggerFactory
			.getLogger(Shredding.class);

	/**
	 * Shred XML file.
	 * 
	 * @param pSource
	 *          source XML file
	 * @param pTarget
	 *          target folder
	 * @return true if successfully shreddered, false otherwise
	 */
	abstract boolean shred(final File pSource, final File pTarget);

	/** Kind of shredder. */
	private enum EType {
		/** Normal shredder. */
		NORMAL {
			@Override
			Callable<Long> newInstance(final File pSource, final NodeWriteTrx pWtx)
					throws IOException, XMLStreamException, SirixUsageException {
				final XMLEventReader reader = XMLShredder.createFileReader(pSource);
				return new XMLShredder.Builder(pWtx, reader, Insert.ASFIRSTCHILD)
						.includeComments(true).includePIs(true).build();
			}
		},

		/** Update shredder. */
		UPDATE {
			@Override
			Callable<Long> newInstance(final File pSource, final NodeWriteTrx pWtx)
					throws IOException, XMLStreamException, SirixUsageException {
				final XMLEventReader reader = XMLShredder.createFileReader(pSource);
				try {
					return new XMLUpdateShredder(pWtx, reader, Insert.ASFIRSTCHILD,
							pSource, ShredderCommit.COMMIT);
				} catch (final SirixIOException e) {
					throw new IOException(e);
				}
			}
		};

		/**
		 * Get new instance of the appropriate shredder.
		 * 
		 * @param pSource
		 *          source to shredder
		 * @param pWtx
		 *          sirix {@link NodeWriteTrx}
		 * @return instance of appropriate {@link Shredder} implementation
		 * @throws IOException
		 *           if {@code pSource} can't be read
		 * @throws XMLStreamException
		 *           if parser encounters an error
		 * @throws SirixUsageException
		 *           if the shredder isn't used properly
		 */
		abstract Callable<Long> newInstance(final File pSource,
				final NodeWriteTrx pWtx) throws IOException, XMLStreamException,
				SirixUsageException;
	}

	/**
	 * Do the shredding.
	 * 
	 * @param source
	 *          the source file to shredder
	 * @param target
	 *          the database to create/open
	 */
	private static boolean shredder(final File source, final File target,
			final EType pType) {
		assert source != null;
		assert target != null;
		assert pType != null;
		boolean retVal = true;
		try {
			final Database database = setupDatabase(target);
			try (final Session session = database
					.getSession(new SessionConfiguration.Builder("shredded").build());
					final NodeWriteTrx wtx = session.beginNodeWriteTrx();) {
				final ExecutorService executor = Executors.newSingleThreadExecutor();
				executor.submit(pType.newInstance(source, wtx));
				executor.shutdown();
				executor.awaitTermination(5 * 60, TimeUnit.SECONDS);
			}
		} catch (final IOException | XMLStreamException | InterruptedException
				| SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
			retVal = false;
		}
		return retVal;

	}

	/**
	 * Setup a new {@code database/resource}.
	 * 
	 * @param target
	 *          the database to create/open
	 * @return {@link Database} implementation
	 * @throws SirixException
	 *           if something went wrong
	 */
	private static Database setupDatabase(final File target)
			throws SirixException {
		assert target != null;
		final DatabaseConfiguration config = new DatabaseConfiguration(target);
		Databases.truncateDatabase(config);
		Databases.createDatabase(config);
		final Database db = Databases.openDatabase(target);
		db.createResource(new ResourceConfiguration.Builder("shredded", config)
				.useTextCompression(false).build());
		return db;
	}
}
