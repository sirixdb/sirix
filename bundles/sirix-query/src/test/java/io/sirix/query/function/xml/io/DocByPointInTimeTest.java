/*
 * Copyright (c) 2018, Sirix
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
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;
import io.sirix.utils.XmlDocumentCreator;
import junit.framework.TestCase;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Query;
import io.brackit.query.node.parser.DocumentParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class DocByPointInTimeTest extends TestCase {
  /** The {@link Holder} instance. */
  private Holder holder;

  @BeforeEach
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @AfterEach
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void test() throws QueryException {
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    holder.getXmlNodeTrx().close();

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();

    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = database.toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq1 = "xn:open('" + dbName + "','" + resName + "', xs:dateTime(\"2219-05-01T00:00:00\"))";

      // final String xq1 =
      // "(xs:dateTime(\"2019-05-01T00:00:00-00:00\") - xs:dateTime(\"1970-01-01T00:00:00-00:00\")) div
      // xs:dayTimeDuration('PT0.001S')";

      final Query query = new Query(SirixCompileChain.createWithNodeStore(store), xq1);
      final XmlDBNode node = (XmlDBNode) query.evaluate(ctx);

      assertEquals(5, node.getTrx().getRevisionNumber());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"()", "(), ()", "(), false()", "(), true()",
      "xs:dateTime('2219-05-01T00:00:00Z'), ()", "xs:dateTime('2219-05-01T00:00:00Z'), false()",
      "xs:dateTime('2219-05-01T00:00:00Z'), true()"})
  public void registeredOpenFormsReturnLatestRevision(final String arguments) {
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    holder.getXmlNodeTrx().close();

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      final String query =
          "xn:open('" + database + "','" + XmlTestHelper.RESOURCE + "', " + arguments + ")";
      final XmlDBNode node = (XmlDBNode) new Query(SirixCompileChain.createWithNodeStore(store), query).evaluate(ctx);

      assertEquals(5, node.getTrx().getRevisionNumber());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"()", "false()", "true()"})
  public void datedFourArgumentOpenReturnsHistoricalRevision(final String option) {
    holder.getXmlNodeTrx().close();

    final Instant firstCommit = Instant.parse("2001-01-01T00:00:00Z");
    final Path database = XmlTestHelper.PATHS.PATH2.getFile();
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final XmlDBCollection collection =
          store.create(database.toString(), new DocumentParser("<root/>"), null, firstCommit);
      final XmlResourceSession session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        writer.commit(null, Instant.parse("2002-01-01T00:00:00Z"));
      }

      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      final String query = "xn:open('" + database + "','resource1', xs:dateTime('"
          + firstCommit + "'), " + option + ")";
      final XmlDBNode node = (XmlDBNode) new Query(SirixCompileChain.createWithNodeStore(store), query).evaluate(ctx);

      assertEquals(1, node.getTrx().getRevisionNumber());
    }
  }

  /**
   * A point in time before the resource's first revision answers "the resource did not exist yet",
   * and that answer must not take the resource session down with it.
   * {@link io.sirix.api.Database#beginResourceSession} hands every caller the one cached session for
   * that resource, so closing it here closed every transaction anybody else still held on it.
   */
  @Test
  public void testPointInTimeBeforeFirstRevisionKeepsSharedSessionOpen() throws QueryException {
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    holder.getXmlNodeTrx().close();

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();

    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final XmlDBCollection collection = store.lookup(database.toString());
      final XmlResourceSession session = collection.getDatabase().beginResourceSession(XmlTestHelper.RESOURCE);
      final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();

      try {
        assertNull("a point in time before the first revision must yield no document",
            collection.getDocument(XmlTestHelper.RESOURCE, Instant.parse("2000-01-01T00:00:00Z")));

        assertFalse("the shared resource session was closed", session.isClosed());
        assertFalse("an unrelated open transaction on the shared session was closed", rtx.isClosed());
        assertTrue("the surviving transaction is no longer usable", rtx.moveToDocumentRoot());
      } finally {
        if (!rtx.isClosed()) {
          rtx.close();
        }
      }
    }
  }
}
