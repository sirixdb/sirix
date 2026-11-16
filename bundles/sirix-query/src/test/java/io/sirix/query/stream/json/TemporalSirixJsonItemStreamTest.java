package io.sirix.query.stream.json;

import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.query.json.JsonDBCollection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Contract of {@link TemporalSirixJsonItemStream} beyond the per-revision items, which
 * {@code TemporalNavigationTest} covers end to end: exhaustion yields {@code null} and closing the
 * stream releases the axis (look-ahead axes hold prefetched transactions).
 */
final class TemporalSirixJsonItemStreamTest {

  @Test
  void exhaustedAxisYieldsNull() {
    final EmptyAxis axis = new EmptyAxis();
    final TemporalSirixJsonItemStream stream =
        new TemporalSirixJsonItemStream(axis, mock(JsonDBCollection.class), false);

    assertNull(stream.next());
    assertNull(stream.next());
  }

  @Test
  void closeClosesTheAxis() {
    final EmptyAxis axis = new EmptyAxis();
    final TemporalSirixJsonItemStream stream =
        new TemporalSirixJsonItemStream(axis, mock(JsonDBCollection.class), true);

    stream.close();

    assertEquals(1, axis.closed);
  }

  @Test
  void nullArgumentsAreRejected() {
    final JsonDBCollection collection = mock(JsonDBCollection.class);
    final EmptyAxis axis = new EmptyAxis();
    assertThrows(NullPointerException.class, () -> new TemporalSirixJsonItemStream(null, collection, false));
    assertThrows(NullPointerException.class, () -> new TemporalSirixJsonItemStream(axis, null, false));
  }

  /** An axis over no revisions that counts {@link #close()} calls. */
  private static final class EmptyAxis extends AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> {
    private int closed;

    @Override
    protected JsonNodeReadOnlyTrx computeNext() {
      return endOfData();
    }

    @Override
    public ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> getResourceSession() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      closed++;
    }
  }
}
