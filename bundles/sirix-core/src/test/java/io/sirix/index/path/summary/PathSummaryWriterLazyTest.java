package io.sirix.index.path.summary;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.NodeFactory;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class PathSummaryWriterLazyTest {
  @Test
  void closingAnUnusedSummaryClosesStorageWithoutReadingIt() {
    final StorageEngineWriter storage = mock(StorageEngineWriter.class);
    final JsonResourceSession session = mock(JsonResourceSession.class);
    when(session.getResourceConfig()).thenReturn(
        ResourceConfiguration.newBuilder("data").buildPathSummary(true).buildPathStatistics(true).build());
    final PathSummaryWriter<JsonNodeReadOnlyTrx> writer =
        PathSummaryWriter.createLazy(storage, session, mock(NodeFactory.class), mock(JsonNodeReadOnlyTrx.class));
    assertFalse(writer.isClosed());
    writer.flushPendingStats();
    writer.close();
    when(storage.isClosed()).thenReturn(true);
    assertTrue(writer.isClosed());
    writer.close();
    verify(storage).close();
    assertThrows(IllegalStateException.class, writer::getPathSummary);
    // Storage has no buffer manager or records configured: any eager traversal also fails this test.
  }
}
