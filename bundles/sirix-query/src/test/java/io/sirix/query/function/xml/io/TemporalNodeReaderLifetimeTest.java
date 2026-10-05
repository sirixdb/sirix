package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.type.NodeType;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.stream.node.TemporalSirixNodeStream;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

final class TemporalNodeReaderLifetimeTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void acceptedReaderSurvivesEarlyStreamClose(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("history", "resource1", new DocumentParser("<root/>"));
      final var source = collection.getDocument("resource1").getFirstChild();
      final var session = source.getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        writer.commit();
        writer.commit();
      }
      final NodeType test = mock(NodeType.class);
      when(test.matches(any(XmlDBNode.class))).thenAnswer(invocation -> {
        final XmlDBNode node = invocation.getArgument(0);
        return node.getTrx().getRevisionNumber() == 2;
      });
      final int baseline = session.activeTrxCount();
      final XmlDBNode accepted;
      try (final var stream =
          new TemporalSirixNodeStream(new PrefetchedAllTimeAxis<>(session, source.getTrx()), collection, test)) {
        accepted = (XmlDBNode) stream.next();
        assertEquals(2, accepted.getTrx().getRevisionNumber());
      }
      assertEquals(baseline + 1, session.activeTrxCount(), "only the accepted reader belongs to the consumer");
      assertFalse(accepted.getTrx().isClosed());
      assertEquals("root", accepted.getName().getLocalName());
      accepted.getTrx().close();
      assertEquals(baseline, session.activeTrxCount());
      assertFalse(source.getTrx().isClosed());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void matchingExceptionClosesCandidateAndPrefetchedReaders(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("history", "resource1", new DocumentParser("<root/>"));
      final var source = collection.getDocument("resource1").getFirstChild();
      final var session = source.getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        writer.commit();
        writer.commit();
      }
      final NodeType test = mock(NodeType.class);
      final IllegalStateException failure = new IllegalStateException("matching failed");
      when(test.matches(any(XmlDBNode.class))).thenThrow(failure);
      final int baseline = session.activeTrxCount();
      try (final var stream =
          new TemporalSirixNodeStream(new PrefetchedAllTimeAxis<>(session, source.getTrx()), collection, test)) {
        assertSame(failure, assertThrows(IllegalStateException.class, stream::next));
      }
      assertEquals(baseline, session.activeTrxCount(), "matching failures must release every stream-owned reader");
      assertFalse(source.getTrx().isClosed());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectedTemporalNodesReleaseReadersBeforeSessionClose(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var collection = store.create("history", "resource1",
          new DocumentParser("<root xmlns:p='urn:a' p:flag='a'><p:item>one</p:item></root>"));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        for (int revision = 2; revision <= 4; revision++) {
          writer.commit();
        }
      }
      for (final String axis : List.of("first", "last", "next", "previous", "past", "past-or-self", "future",
          "future-or-self", "all-times")) {
        final boolean backwards = axis.equals("first") || axis.equals("previous") || axis.startsWith("past");
        final var document = collection.getDocument("resource1", backwards
            ? 4
            : 1);
        context.bind(new QNm("document"), document);
        final String prefix = "declare namespace a='urn:a'; declare namespace b='urn:b'; "
            + "declare variable $document external; count(";
        for (final String path : List.of("/root/a:item/" + axis + "::processing-instruction()",
            "/root/a:item/" + axis + "::b:*", "/root/a:item/" + axis + "::*:other",
            "/root/a:item/" + axis + "::element(a:item,xs:string)",
            "/root/@a:flag/" + axis + "::attribute(a:flag,xs:string)", "/" + axis + "::document-node(element(other))",
            "/" + axis + "::document-node(element(root,xs:string))")) {
          final var query = new Query(chain, prefix + "$document" + path + ")");
          final int baseline = session.activeTrxCount();
          for (int repetition = 0; repetition < 3; repetition++) {
            assertEquals("0", query.evaluate(context).toString(), path);
            assertEquals(baseline, session.activeTrxCount(),
                path + " must close rejected readers before session close");
          }
        }
      }
    }
  }
}
