package io.sirix.query.function.xml.io;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.expr.Accessor;
import io.brackit.query.jdm.type.ElementType;
import io.brackit.query.jdm.type.NodeType;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.axis.filter.xml.ElementFilter;
import io.sirix.axis.filter.xml.TemporalXmlNodeReadFilterAxis;
import io.sirix.query.compiler.translator.SirixTranslator;
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
  private static final List<Accessor> TEMPORAL_ACCESSORS =
      List.of(Accessor.FIRST, Accessor.LAST, Accessor.NEXT, Accessor.PREVIOUS, Accessor.PAST, Accessor.PAST_OR_SELF,
          Accessor.FUTURE, Accessor.FUTURE_OR_SELF, Accessor.ALL_TIME);

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void acceptedReaderSurvivesEarlyStreamClose(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("history", "resource1", new DocumentParser("<root/>"));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        for (int revision = 2; revision <= 5; revision++) {
          writer.commit();
        }
      }
      final var source = collection.getDocument("resource1", 3).getFirstChild();
      final int baseline = session.activeTrxCount();
      for (final Accessor accessor : TEMPORAL_ACCESSORS) {
        final int acceptedRevision = switch (accessor.getAxis()) {
          case FIRST, PAST, PAST_OR_SELF -> 1;
          case LAST, FUTURE, FUTURE_OR_SELF -> 5;
          case NEXT -> 4;
          case PREVIOUS, ALL_TIME -> 2;
          default -> throw new AssertionError(accessor);
        };
        final int firstRevision = switch (accessor.getAxis()) {
          case FIRST, ALL_TIME -> 1;
          case LAST -> 5;
          case NEXT, FUTURE -> 4;
          case PREVIOUS, PAST -> 2;
          case PAST_OR_SELF, FUTURE_OR_SELF -> 3;
          default -> throw new AssertionError(accessor);
        };
        final NodeType test = mock(NodeType.class);
        when(test.matches(any(XmlDBNode.class))).thenAnswer(invocation -> {
          final XmlDBNode node = invocation.getArgument(0);
          return node.getTrx().getRevisionNumber() == acceptedRevision;
        });
        for (final NodeType nodeTest : List.of(test, new ElementType(new QNm("root")))) {
          final XmlDBNode accepted;
          try (final var stream = accessor.performStep(source, nodeTest)) {
            accepted = (XmlDBNode) stream.next();
            assertEquals(nodeTest == test
                ? acceptedRevision
                : firstRevision, accepted.getTrx().getRevisionNumber(), accessor.toString());
          }
          assertEquals(baseline + 1, session.activeTrxCount(), "only the accepted reader belongs to the consumer");
          assertFalse(accepted.getTrx().isClosed());
          assertEquals("root", accepted.getName().getLocalName());
          accepted.getTrx().close();
          assertEquals(baseline, session.activeTrxCount(), accessor.toString());
          assertFalse(source.getTrx().isClosed());
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void matchingExceptionClosesCandidateAndPrefetchedReaders(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("history", "resource1", new DocumentParser("<root/>"));
      final var session = collection.getDocument("resource1").getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        writer.commit();
        writer.commit();
      }
      final var source = collection.getDocument("resource1", 2).getFirstChild();
      final NodeType test = mock(NodeType.class);
      final IllegalStateException failure = new IllegalStateException("matching failed");
      when(test.matches(any(XmlDBNode.class))).thenThrow(failure);
      final int baseline = session.activeTrxCount();
      for (final Accessor accessor : TEMPORAL_ACCESSORS) {
        try (final var stream = accessor.performStep(source, test)) {
          assertSame(failure, assertThrows(IllegalStateException.class, stream::next));
        }
        assertEquals(baseline, session.activeTrxCount(),
            accessor + " matching failures must release every stream-owned reader");
        assertFalse(source.getTrx().isClosed());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void simpleFilterExceptionClosesCandidateAndPrefetchedReaders(final VersioningType versioning) {
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build()) {
      final var collection = store.create("history", "resource1", new DocumentParser("<root/>"));
      final var source = collection.getDocument("resource1").getFirstChild();
      final var session = source.getTrx().getResourceSession();
      try (final var writer = session.beginNodeTrx()) {
        writer.commit();
        writer.commit();
      }
      final ElementFilter filter = mock(ElementFilter.class);
      final IllegalStateException failure = new IllegalStateException("filtering failed");
      when(filter.filter()).thenThrow(failure);
      final int baseline = session.activeTrxCount();
      try (final var stream = new TemporalSirixNodeStream(
          new TemporalXmlNodeReadFilterAxis<>(new PrefetchedAllTimeAxis<>(session, source.getTrx()), filter),
          collection)) {
        assertSame(failure, assertThrows(IllegalStateException.class, stream::next));
      }
      assertEquals(baseline, session.activeTrxCount(), "filtering failures must release every stream-owned reader");
      assertFalse(source.getTrx().isClosed());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectedTemporalNodesReleaseReadersBeforeSessionClose(final VersioningType versioning) {
    assertEquals(Boolean.parseBoolean(System.getProperty("org.sirix.xquery.optimize.accessor", "true")),
        SirixTranslator.OPTIMIZE);
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
            "/root/a:item/" + axis + "::text()", "/root/a:item/" + axis + "::element(other)",
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
