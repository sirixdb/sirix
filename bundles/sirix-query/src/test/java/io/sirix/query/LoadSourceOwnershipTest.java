package io.sirix.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.io.URIHandler;
import io.sirix.query.function.xml.io.Load;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** REST-210: retained load sources are owned by the call, including partial acquisition. */
final class LoadSourceOwnershipTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void xmlReleasesAllAcquiredSourcesWithoutMaskingAcquisitionFailure(final boolean brokenClose) {
    final Source first = new Source("<first/>", brokenClose);
    final Source second = new Source("<second/>", brokenClose);
    final IOException missing = new IOException("source unavailable");
    try (var store = BasicXmlDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithNodeStore(store);
        var uris = mockStatic(URIHandler.class)) {
      uris.when(() -> URIHandler.getInputStream("first")).thenReturn(first);
      uris.when(() -> URIHandler.getInputStream("second")).thenReturn(second);
      uris.when(() -> URIHandler.getInputStream("missing")).thenThrow(missing);
      final QueryException failure =
          assertThrows(QueryException.class, () -> new Load(true).execute(null, context, new Sequence[] {new Str("db"),
              null, new ItemSequence(new Str("first"), new Str("second"), new Str("missing"))}));
      assertSame(missing, failure.getCause());
      assertTrue(first.closed);
      assertTrue(second.closed, "one close failure must not prevent release of later sources");
      assertEquals(null, store.lookup("db"), "acquisition failure must not create the target");
      if (brokenClose) {
        assertEquals(1, missing.getSuppressed().length);
        final Throwable cleanup = missing.getSuppressed()[0].getCause();
        assertSame(first.closeFailure, cleanup);
        assertEquals(List.of(second.closeFailure), List.of(cleanup.getSuppressed()));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void xmlReleasesSourcesAfterParsingAndAfterInvalidContent(final boolean invalid) {
    final Source source = new Source(invalid
        ? "<broken"
        : "<loaded/>", false);
    try (var store = BasicXmlDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithNodeStore(store);
        var uris = mockStatic(URIHandler.class)) {
      uris.when(() -> URIHandler.getInputStream("input")).thenReturn(source);
      final Sequence[] args = {new Str("db"), new Str("doc"), new Str("input")};
      if (invalid) {
        assertThrows(QueryException.class, () -> new Load(true).execute(null, context, args));
      } else {
        new Load(true).execute(null, context, args);
        try (var session = store.lookup("db").getDatabase().beginResourceSession("doc");
            var reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveToFirstChild());
          assertEquals("loaded", reader.getName().getLocalName());
        }
      }
      assertTrue(source.closed);
    }
  }

  @Test
  void jsonReleasesEveryAcquiredSourceEvenWhenCloseFails() {
    final Source first = new Source("{}", true);
    final Source second = new Source("[]", true);
    final IOException missing = new IOException("source unavailable");
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var uris = mockStatic(URIHandler.class)) {
      final var collection = store.create("db");
      uris.when(() -> URIHandler.getInputStream("first")).thenReturn(first);
      uris.when(() -> URIHandler.getInputStream("second")).thenReturn(second);
      uris.when(() -> URIHandler.getInputStream("missing")).thenThrow(missing);
      final UncheckedIOException failure = assertThrows(UncheckedIOException.class,
          () -> collection.addAllFromUris(List.of("first", "second", "missing"),
              new ArrayObject(new QNm[0], new Sequence[0])));
      assertSame(missing, failure.getCause());
      assertTrue(first.closed);
      assertTrue(second.closed);
      assertTrue(collection.getDatabase().listResources().isEmpty());
    }
  }

  private static final class Source extends ByteArrayInputStream {
    private final boolean brokenClose;
    private final IOException closeFailure = new IOException("cannot close source");
    private boolean closed;

    private Source(final String content, final boolean brokenClose) {
      super(content.getBytes(StandardCharsets.UTF_8));
      this.brokenClose = brokenClose;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (brokenClose) {
        throw closeFailure;
      }
      super.close();
    }
  }
}
