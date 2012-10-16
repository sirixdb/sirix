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
package org.sirix.diff.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;

import org.slf4j.LoggerFactory;
import org.sirix.access.DatabaseImpl;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.api.NodeWriteTrx;
import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.utils.Files;
import org.sirix.utils.LogWrapper;

/**
 * Import using the FMSE algorithm.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class FMSEImport {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(FMSEImport.class));

	/**
	 * Shredder new revision as temporal resource.
	 * 
	 * @param pResNewRev
	 *          {@link File} reference for new revision (XML resource)
	 * @param pNewRev
	 *          {@link File} reference for shreddered new revision (sirix
	 *          resource)
	 * @throws SirixException
	 *           if sirix fails to shredder the file
	 * @throws IOException
	 *           if file couldn't be read
	 * @throws XMLStreamException
	 *           if XML document isn't well formed
	 * @throws NullPointerException
	 *           if {@code paramResNewRev} or {@code paramNewRev} is {@code null}
	 */
	private void shredder(@Nonnull final File pResNewRev,
			@Nonnull final File pNewRev) throws SirixException, IOException,
			XMLStreamException {
		assert pResNewRev != null;
		assert pNewRev != null;
		final DatabaseConfiguration conf = new DatabaseConfiguration(pNewRev);
		DatabaseImpl.truncateDatabase(conf);
		DatabaseImpl.createDatabase(conf);
		final Database db = DatabaseImpl.openDatabase(pNewRev);
		db.createResource(new ResourceConfiguration.Builder("shredded", conf)
				.build());
		final Session session = db.getSession(new SessionConfiguration.Builder(
				"shredded").build());
		final NodeWriteTrx wtx = session.beginNodeWriteTrx();
		final XMLEventReader reader = XMLShredder.createFileReader(pResNewRev);
		final XMLShredder shredder = new XMLShredder.Builder(wtx, reader,
				Insert.ASFIRSTCHILD).commitAfterwards().build();
		shredder.call();
		wtx.close();
		session.close();
		db.close();
	}

	/**
	 * Import the data.
	 * 
	 * @param pResOldRev
	 *          {@link File} for old revision (sirix resource)
	 * @param pResNewRev
	 *          {@link File} for new revision (XML resource)
	 */
	private void dataImport(@Nonnull final File pResOldRev,
			@Nonnull final File pResNewRev) {

		try {
			final File newRevTarget = new File(new StringBuilder("target")
					.append(File.separator).append(checkNotNull(pResNewRev).getName())
					.toString());
			if (newRevTarget.exists()) {
				Files.recursiveRemove(newRevTarget.toPath());
			}
			shredder(checkNotNull(pResNewRev), newRevTarget);

			final Database databaseOld = DatabaseImpl.openDatabase(pResOldRev);
			final Session sessionOld = databaseOld
					.getSession(new SessionConfiguration.Builder("shredded").build());
			final NodeWriteTrx wtx = sessionOld.beginNodeWriteTrx();

			final Database databaseNew = DatabaseImpl.openDatabase(newRevTarget);
			final Session sessionNew = databaseNew
					.getSession(new SessionConfiguration.Builder("shredded").build());
			final NodeReadTrx rtx = sessionNew.beginNodeReadTrx();
			try (final FMSE fmes = new FMSE()) {
				fmes.diff(wtx, rtx);
			}
			wtx.close();
			rtx.close();
			sessionOld.close();
			sessionNew.close();
			databaseOld.close();
			databaseNew.close();
		} catch (final SirixException | IOException | XMLStreamException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * Main entry point.
	 * 
	 * @param args
	 *          <p>
	 *          arguments:
	 *          </p>
	 *          <ul>
	 *          <li>args[0] - path to resource to update</li>
	 *          <li>args[1] - path to new XML document</li>
	 *          </ul>
	 */
	public static void main(final String[] args) {
		if (args.length < 2 || args.length > 4) {
			throw new IllegalArgumentException(
					"Usage: FSME oldResource newXMLDocument [startNodeKeyOld] [startNodeKeyNew]");
		}

		final File resOldRev = new File(args[0]);
		final File resNewRev = new File(args[1]);

		final FMSEImport fmse = new FMSEImport();
		fmse.dataImport(resOldRev, resNewRev);
	}
}
