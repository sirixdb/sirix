package io.sirix.service;

import io.brackit.query.atomic.QNm;
import io.sirix.JsonTestHelper;
import io.sirix.XmlTestHelper;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.xml.serialize.XmlSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.io.Writer;

import static org.junit.Assert.assertThrows;

/**
 * Regression tests for issue #1068: the JSON and XML serializers must propagate I/O failures of
 * the target sink as {@link UncheckedIOException} from {@code call()} instead of swallowing them
 * and completing "successfully" with truncated/absent output.
 */
public final class SerializerIOExceptionPropagationTest {

  /** A {@link Writer} whose write/flush methods always fail. */
  private static final class ThrowingWriter extends Writer {

    @Override
    public void write(final char[] buffer, final int offset, final int length) throws IOException {
      throw new IOException("sink failure (write)");
    }

    @Override
    public void flush() throws IOException {
      throw new IOException("sink failure (flush)");
    }

    @Override
    public void close() {
      // Nothing to close.
    }
  }

  /** An {@link OutputStream} whose write/flush methods always fail. */
  private static final class ThrowingOutputStream extends OutputStream {

    @Override
    public void write(final int b) throws IOException {
      throw new IOException("sink failure (write)");
    }

    @Override
    public void flush() throws IOException {
      throw new IOException("sink failure (flush)");
    }
  }

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testJsonSerializerPropagatesSinkFailure() {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"foo\":\"bar\"}"));
      }

      final JsonSerializer serializer = JsonSerializer.newBuilder(session, new ThrowingWriter()).build();

      // Pre-fix call() completed normally although not a single character reached the sink.
      assertThrows(UncheckedIOException.class, serializer::call);
    }
  }

  @Test
  public void testXmlSerializerPropagatesSinkFailure() {
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertElementAsFirstChild(new QNm("root"));
        wtx.commit();
      }

      final XmlSerializer serializer = XmlSerializer.newBuilder(session, new ThrowingOutputStream()).build();

      // Pre-fix call() completed normally although not a single byte reached the sink.
      assertThrows(UncheckedIOException.class, serializer::call);
    }
  }
}
