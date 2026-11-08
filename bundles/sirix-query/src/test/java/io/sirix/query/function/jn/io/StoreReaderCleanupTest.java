package io.sirix.query.function.jn.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.exception.SirixIOException;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import java.io.IOException;
import java.io.StringReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class StoreReaderCleanupTest {

  /**
   * Java's try-with-resources contract keeps an operation failure primary and suppresses cleanup
   * failures. Exercise jn:store's batch append with boundary failures to reject masking the operation
   * or abandoning later readers after the first failed close.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void batchAppendClosesEveryReaderAndPreservesThePrimaryFailure(final boolean operationFails) {
    final JsonDBStore store = mock(JsonDBStore.class);
    final JsonDBCollection collection = mock(JsonDBCollection.class);
    when(store.lookup("database")).thenReturn(collection);
    final SirixIOException operationFailure = new SirixIOException("append failed");
    if (operationFails) {
      doThrow(operationFailure).when(collection).addAll(anyList(), any());
    }
    final IOException firstCloseFailure = new IOException("first close failed");
    final IOException secondCloseFailure = new IOException("second close failed");
    final TrackingJsonReader first = new TrackingJsonReader(firstCloseFailure);
    final TrackingJsonReader second = new TrackingJsonReader(secondCloseFailure);
    final TrackingJsonReader third = new TrackingJsonReader(null);

    try (var shredder = mockStatic(JsonShredder.class);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      shredder.when(() -> JsonShredder.createStringReader(anyString())).thenReturn(first, second, third);
      final Query query = new Query(chain, "jn:store('database',(),('{}','{}','{}'),false())");
      final QueryException thrown = assertThrows(QueryException.class, () -> query.evaluate(context));

      Throwable primary = thrown;
      while (primary.getCause() != null) {
        primary = primary.getCause();
      }
      assertSame(operationFails
          ? operationFailure
          : firstCloseFailure, primary);
      if (operationFails) {
        assertArrayEquals(new Throwable[] {firstCloseFailure}, operationFailure.getSuppressed());
      }
      assertArrayEquals(new Throwable[] {secondCloseFailure}, firstCloseFailure.getSuppressed());
      assertTrue(first.closed);
      assertTrue(second.closed);
      assertTrue(third.closed);
    }
  }

  private static final class TrackingJsonReader extends JsonReader {
    private final IOException closeFailure;
    private boolean closed;

    private TrackingJsonReader(final IOException closeFailure) {
      super(new StringReader("{}"));
      this.closeFailure = closeFailure;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }
}
