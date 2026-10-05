package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.access.trx.node.xml.ForwardingXmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.SirixTranslator;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class XmlPathSummaryParallelQueryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"descendant, false", "descendant-or-self, false", "child, false",
      "descendant, true", "descendant-or-self, true", "child, true"})
  void parallelFlworCachesKeepPublicationWithinTheirWorker(final String axis, final boolean storeDeweyIds)
      throws Exception {
    final String children = axis.equals("child")
        ? "<n/>".repeat(SirixTranslator.CHILD_THRESHOLD + 1)
        : "";
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                    .location(directory)
                                                    .storageType(StorageType.FILE_CHANNEL)
                                                    .storeDeweyIds(storeDeweyIds)
                                                    .buildPathSummary(true)
                                                    .build();
        final SirixCompileChain chain = SirixCompileChain.createParallel(store, null);
        final SirixQueryContext emptyContext = SirixQueryContext.createWithNodeStore(store);
        final SirixQueryContext presentContext = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("empty", new DocumentParser("<r><a id='revision'>" + children + "</a></r>")));
      assertNotNull(collection.add("present", new DocumentParser(
          "<r><a id='present'><hit/>" + children + "</a></r>")));
      final XmlResourceSession emptySession = collection.getDatabase().beginResourceSession("empty");
      emptySession.getNodeTrx().ifPresent(XmlNodeTrx::close);
      try (final XmlNodeTrx writer = emptySession.beginNodeTrx()) {
        writer.moveToDocumentRoot();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        writer.insertElementAsFirstChild(new QNm("hit"));
        writer.commit();
      }
      final XmlDBNode empty = contextNode(collection, emptySession, 1);
      final XmlDBNode revised = contextNode(collection, emptySession, 2);
      final XmlResourceSession presentSession = collection.getDatabase().beginResourceSession("present");
      final XmlDBNode present = contextNode(collection, presentSession, 1);
      assertEquals(empty.getTrx().getPathNodeKey(), present.getTrx().getPathNodeKey());
      assertEquals(empty.getTrx().getPathNodeKey(), revised.getTrx().getPathNodeKey());
      if (axis.equals("child")) {
        assertTrue(empty.getTrx().getChildCount() > SirixTranslator.CHILD_THRESHOLD);
        assertTrue(present.getTrx().getChildCount() > SirixTranslator.CHILD_THRESHOLD);
        assertTrue(revised.getTrx().getChildCount() > SirixTranslator.CHILD_THRESHOLD);
      }
      final CountDownLatch emptyComputed = new CountDownLatch(1);
      final CountDownLatch releaseEmpty = new CountDownLatch(1);
      final CountDownLatch emptyFinished = new CountDownLatch(1);
      final XmlDBNode pausedEmpty = pauseFirstMatch(empty, emptyComputed, releaseEmpty);
      final Query query = new Query(chain, "declare variable $starts external; "
          + "for $s in $starts where exists($s/" + axis + "::hit) return string($s/@id)");
      try (final ExecutorService workers = Executors.newFixedThreadPool(2)) {
        final Future<?> emptyResult = workers.submit(() -> {
          try {
            assertResults(query, emptyContext, new Item[] {pausedEmpty}, List.of());
          } finally {
            emptyFinished.countDown();
          }
        });
        try {
          await(emptyComputed);
          final Future<?> presentResult = workers.submit(() -> {
            try {
              assertResults(query, presentContext, new Item[] {present}, List.of("present"));
            } finally {
              releaseEmpty.countDown();
            }
            await(emptyFinished);
            final Item[] repeated = new Item[256];
            Arrays.fill(repeated, present);
            assertResults(query, presentContext, repeated, Collections.nCopies(256, "present"));
          });
          presentResult.get(1, TimeUnit.MINUTES);
          emptyResult.get(1, TimeUnit.MINUTES);
        } finally {
          releaseEmpty.countDown();
          emptyFinished.countDown();
        }
      }
      final Item[] alternating = new Item[768];
      final List<String> expected = new ArrayList<>(512);
      for (int index = 0; index < 256; index++) {
        alternating[index * 3] = present;
        alternating[index * 3 + 1] = empty;
        alternating[index * 3 + 2] = revised;
        expected.add("present");
        expected.add("revision");
      }
      assertResults(query, presentContext, alternating, expected);
      assertResults(query, presentContext, new Item[] {empty}, List.of());
      assertResults(query, presentContext, new Item[] {revised, present}, List.of("revision", "present"));
    }
  }

  private static XmlDBNode contextNode(final XmlDBCollection collection, final XmlResourceSession session,
      final int revision) {
    return new XmlDBNode(new ThreadSafeXmlReadOnlyTrx(session.beginNodeReadOnlyTrx(revision)), collection)
        .getFirstChild().getFirstChild();
  }

  private static XmlDBNode pauseFirstMatch(final XmlDBNode node, final CountDownLatch computed,
      final CountDownLatch release) {
    final XmlNodeReadOnlyTrx delegate = node.getTrx();
    final XmlResourceSession actualSession = delegate.getResourceSession();
    final XmlResourceSession session = mock(XmlResourceSession.class, delegatesTo(actualSession));
    final AtomicBoolean first = new AtomicBoolean(true);
    doAnswer(open -> {
      final PathSummaryReader reader = actualSession.openPathSummary(open.getArgument(0));
      if (!first.compareAndSet(true, false)) {
        return reader;
      }
      final PathSummaryReader controlled = spy(reader);
      doAnswer(match -> {
        final BitSet result = (BitSet) match.callRealMethod();
        assertTrue(result.isEmpty());
        computed.countDown();
        await(release);
        return result;
      }).when(controlled).match(any(QNm.class), anyInt(), eq(NodeKind.ELEMENT));
      return controlled;
    }).when(session).openPathSummary(delegate.getRevisionNumber());
    final ForwardingXmlNodeReadOnlyTrx transaction = new ForwardingXmlNodeReadOnlyTrx() {
      @Override
      public XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
        return delegate;
      }

      @Override
      public XmlResourceSession getResourceSession() {
        return session;
      }
    };
    return new XmlDBNode(transaction, node.getCollection());
  }

  private static void assertResults(final Query query, final SirixQueryContext context, final Item[] starts,
      final List<String> expected) {
    context.bind(new QNm("starts"), new ItemSequence(starts));
    final Sequence result = query.execute(context);
    final List<String> actual = new ArrayList<>();
    if (result != null) {
      try (final Iter items = result.iterate()) {
        for (Item item; (item = items.next()) != null;) {
          actual.add(((Str) item).stringValue());
        }
      }
    }
    assertEquals(expected, actual);
  }

  private static void await(final CountDownLatch latch) {
    try {
      if (!latch.await(1, TimeUnit.MINUTES)) {
        throw new IllegalStateException("Cache interleaving did not reach its next phase");
      }
    } catch (final InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(exception);
    }
  }
}
