package io.sirix.query.scan;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Ordinary SPI failures retain their cause and internal-error code; only explicit refusal retries.
 */
final class VectorizedSpiFailureTest {
  @Test
  void aggregateStorageFailureRemainsAnInternalQueryError() {
    // The oracle is the SPI error envelope plus an independently injected storage-boundary failure.
    // Returning null or turning the failure into route refusal would violate that contract.
    final JsonResourceSession session = mock(JsonResourceSession.class);
    when(session.getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("records").buildPathSummary(false).buildPathStatistics(false).build());
    final IllegalStateException storageFailure = new IllegalStateException("cannot open revision reader");
    when(session.beginNodeReadOnlyTrx(1)).thenThrow(storageFailure);
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, 1, 1);
    try {
      final QueryException failure =
          assertThrows(QueryException.class, () -> executor.executeAggregate(null, null, "sum", "value"));
      assertEquals(ErrorCode.BIT_DYN_INT_ERROR, failure.getCode());
      assertSame(storageFailure, failure.getCause());
      assertFalse(failure instanceof VectorizedRouteRefusedException);
    } finally {
      executor.close();
    }
  }
}
