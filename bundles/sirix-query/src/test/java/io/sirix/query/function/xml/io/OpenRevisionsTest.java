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
import io.sirix.query.node.XmlDBCollectionImpl;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.node.XmlDBStore;
import io.sirix.utils.XmlDocumentCreator;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.node.parser.DocumentParser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
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
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    holder.getXmlNodeTrx().close();

    final Instant revisionTwoTimestamp;

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx(1)) {
      revisionTwoTimestamp = rtx.getRevisionTimestamp();
    }

    final ZonedDateTime dateTime = ZonedDateTime.ofInstant(revisionTwoTimestamp, ZoneId.of("UTC"));

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();

    // Initialize query context and store.
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);

      final String dbName = database.toString();
      final String resName = XmlTestHelper.RESOURCE;

      final String xq1 = "xn:open-revisions('" + dbName + "','" + resName + "', xs:dateTime(\""
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

  @Test
  public void intervalBeginningBeforeCreationReturnsAllRevisions() {
    assertRevisionInterval("2000-01-01T00:00:00Z", "2219-05-01T00:00:00Z", 1, 5);
  }

  @Test
  public void intervalBeforeCreationReturnsEmptySequence() {
    assertRevisionInterval("1999-01-01T00:00:00Z", "2000-01-01T00:00:00Z", 0, 0);
  }

  @Test
  public void intervalAfterLatestCommitReturnsLatestRevisionOnce() {
    assertRevisionInterval("2218-05-01T00:00:00Z", "2219-05-01T00:00:00Z", 5, 5);
  }

  @Test
  public void intervalUsesOneRevisionCeilingAcrossOneInterveningCommit() {
    assertRevisionIntervalAcrossCommits(1, false);
  }

  @Test
  public void intervalUsesOneRevisionCeilingAcrossTwoInterveningCommits() {
    assertRevisionIntervalAcrossCommits(2, false);
  }

  @Test
  public void intervalUsesLastTimestampTieAcrossInterveningCommit() {
    assertRevisionIntervalAcrossCommits(1, true);
  }

  private void assertRevisionIntervalAcrossCommits(final int commits, final boolean timestampTies) {
    if (!timestampTies) {
      XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    }
    holder.getXmlNodeTrx().close();

    final Instant start = Instant.parse(timestampTies
        ? "2018-05-01T00:00:00Z"
        : "2218-05-01T00:00:00Z");
    final Instant end = timestampTies
        ? start.plusNanos(500_000)
        : Instant.parse("2219-05-01T00:00:00Z");
    final Path database = (timestampTies
        ? XmlTestHelper.PATHS.PATH2
        : XmlTestHelper.PATHS.PATH1).getFile();
    final String resourceName = timestampTies
        ? "resource1"
        : XmlTestHelper.RESOURCE;
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final XmlDBCollection collection = timestampTies
          ? store.create(database.toString(), new DocumentParser("<root/>"), null, start.minusMillis(3))
          : store.lookup(database.toString());
      final var session = collection.getDatabase().beginResourceSession(resourceName);
      if (timestampTies) {
        try (final var writer = session.beginNodeTrx()) {
          for (int revision = 2; revision <= 5; revision++) {
            writer.commit(null, start.minusMillis(Math.max(0, 4 - revision)));
          }
        }
      }
      final var observedSession = spy(session);
      final var observedDatabase = spy(collection.getDatabase());
      doReturn(observedSession).when(observedDatabase).beginResourceSession(resourceName);
      final var observedCollection = new XmlDBCollectionImpl(database.toString(), observedDatabase);
      final XmlDBStore observedStore = mock(XmlDBStore.class, delegatesTo(store));
      doReturn(observedCollection).when(observedStore).lookup(database.toString());
      final int[] endpointResolutions = {0};

      try (final SirixQueryContext context = SirixQueryContext.createWithNodeStore(observedStore);
          final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(observedStore);
          final var writer = session.beginNodeTrx()) {
        final Answer<Integer> endpointResolver = invocation -> {
          final int revision = (int) invocation.callRealMethod();
          final Instant pointInTime = invocation.getArgument(0);
          if (end.equals(pointInTime)) {
            for (int commit = 0; commit < commits; commit++) {
              if (timestampTies) {
                writer.commit(null, start.plusMillis(commit + 1));
              } else {
                writer.commit();
              }
            }
          } else {
            Assert.assertEquals(start, pointInTime);
            Assert.assertEquals(5 + commits, session.getMostRecentRevisionNumber());
          }
          endpointResolutions[0]++;
          return revision;
        };
        doAnswer(endpointResolver).when(observedSession).getRevisionNumber(any(Instant.class));
        doAnswer(endpointResolver).when(observedSession).getRevisionNumber(any(Instant.class), eq(5));

        final String query = "xn:open-revisions('" + database + "','" + resourceName + "', xs:dateTime('" + start
            + "'), xs:dateTime('" + end + "'))";
        final Sequence nodes = new Query(chain, query).evaluate(context);
        Assert.assertNotNull(nodes);
        try (final Iter iter = nodes.iterate()) {
          final XmlDBNode node = (XmlDBNode) iter.next();
          Assert.assertNotNull("the captured interval must include revision 5", node);
          Assert.assertEquals(5, node.getTrx().getRevisionNumber());
          Assert.assertNull(iter.next());
        }
        Assert.assertEquals(2, endpointResolutions[0]);
        Assert.assertEquals(5 + commits, session.getMostRecentRevisionNumber());
        Assert.assertFalse(session.isClosed());
      }
    }
  }

  private void assertRevisionInterval(final String start, final String end, final int firstRevision,
      final int lastRevision) {
    XmlDocumentCreator.createVersionedWithUpdatesAndDeletes(holder.getXmlNodeTrx());
    holder.getXmlNodeTrx().close();

    final Path database = XmlTestHelper.PATHS.PATH1.getFile();
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(database.getParent()).build()) {
      final QueryContext ctx = SirixQueryContext.createWithNodeStore(store);
      final String query = "xn:open-revisions('" + database + "','" + XmlTestHelper.RESOURCE + "', xs:dateTime('"
          + start + "'), xs:dateTime('" + end + "'))";
      final Sequence nodes = new Query(SirixCompileChain.createWithNodeStore(store), query).evaluate(ctx);

      if (firstRevision == 0) {
        Assert.assertNull(nodes);
        return;
      }

      Assert.assertNotNull(nodes);
      try (final Iter iter = nodes.iterate()) {
        for (int revision = firstRevision; revision <= lastRevision; revision++) {
          final XmlDBNode node = (XmlDBNode) iter.next();
          Assert.assertNotNull(node);
          Assert.assertEquals(revision, node.getTrx().getRevisionNumber());
        }
        Assert.assertNull(iter.next());
      }
    }
  }
}
