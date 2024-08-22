/*
 * Copyright (c) 2023, Sirix
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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
package io.sirix.query.function.xml.io;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.exception.SirixException;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.utils.XmlDocumentCreator;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author Johannes Lichtenberger
 *         <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class OpenRevisionsTest {
	/**
	 * The {@link Holder} instance.
	 */
	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		XmlTestHelper.deleteEverything();
		holder = Holder.generateWtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		XmlTestHelper.closeEverything();
	}

	@Test
	public void test() throws IOException, QueryException {
		XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXdmNodeWriteTrx());
		holder.getXdmNodeWriteTrx().close();

		final Instant revisionTwoTimestamp;

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(1)) {
			revisionTwoTimestamp = rtx.getRevisionTimestamp();
		}

		final ZonedDateTime dateTime = ZonedDateTime.ofInstant(revisionTwoTimestamp, ZoneId.of("UTC"));

		final Path database = XmlTestHelper.PATHS.PATH1.getFile();

		// Initialize query context and store.
		try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
			final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

			final String dbName = database.toString();
			final String resName = XmlTestHelper.RESOURCE;

			final String xq1 = "xml:open-revisions('" + dbName + "','" + resName + "', xs:dateTime(\""
					+ DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime)
					+ "\"), xs:dateTime(\"2200-05-01T00:00:00-00:00\"))";

			final Query query = new Query(SirixCompileChain.createWithNodeStore(store), xq1);
			final Sequence nodes = query.evaluate(ctx);

			try (final Iter iter = nodes.iterate()) {
				Assert.assertNotNull(iter.next());
				Assert.assertNotNull(iter.next());
				Assert.assertNotNull(iter.next());
				Assert.assertNotNull(iter.next());
				Assert.assertNotNull(iter.next());
				Assert.assertNull(iter.next());
			}
		}
	}
}
