package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryExecution;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Expr;
import io.brackit.query.module.Module;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexSnapshot;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.changelog.TemporalAggregateCatchUpBudget;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.page.UberPage;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class DerivedAggregateNonblockingTest {
  @Test
  void closingAttemptsBothSnapshotKindsAndPreservesFailures() {
    final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
    final StorageEngineReader source = mock(StorageEngineReader.class);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    final DerivedAggregateCatchUp catchUp = mock(DerivedAggregateCatchUp.class);
    final RuntimeException first = new IllegalStateException("snapshot close");
    final RuntimeException second = new IllegalStateException("catch-up close");
    doReturn(session).when(source).getResourceSession();
    when(session.openDerivedIndexReader(source)).thenReturn(index);
    when(catchUp.reader()).thenReturn(mock(StorageEngineReader.class));
    doThrow(first).when(index).close();
    doThrow(second).when(catchUp).close();
    try (var aggregates = mockStatic(DerivedAggregateCatchUp.class)) {
      aggregates.when(() -> DerivedAggregateCatchUp.open(any(StorageEngineReader.class),
          any(TemporalAggregateCatchUpBudget.Budget.class))).thenReturn(catchUp);
      final DerivedAggregateReaders readers = new DerivedAggregateReaders();
      assertSame(index, readers.borrow(source));
      assertSame(catchUp, readers.borrowCatchUp(source));
      assertSame(first, assertThrows(RuntimeException.class, readers::close));
      assertArrayEquals(new Throwable[] {second}, first.getSuppressed());
      verify(index).close();
      verify(catchUp).close();
      readers.close();
      assertThrows(IllegalStateException.class, () -> readers.borrow(source));
      assertThrows(IllegalStateException.class, () -> readers.borrowCatchUp(source));
    }
  }

  @Test
  void laggingMappingsStayRetryableWithoutWaitingForTheMaintainer() throws Exception {
    final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
    final DerivedIndexMaintainer.Resource resource = mock(DerivedIndexMaintainer.Resource.class);
    final StorageEngineReader source = mock(StorageEngineReader.class);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    doReturn(session).when(source).getResourceSession();
    when(source.getRevisionNumber()).thenReturn(2);
    when(source.getUberPage()).thenReturn(new UberPage(3));
    when(session.getDerivedResource()).thenReturn(resource);
    when(session.openDerivedIndexReader(source)).thenReturn(null, null, index);
    try (var readers = new DerivedAggregateReaders()) {
      assertNull(readers.borrow(source));
      assertNull(readers.borrow(source));
      assertSame(index, readers.borrow(source));
      assertSame(index, readers.borrow(source));
      verify(session, times(3)).openDerivedIndexReader(source);
      verify(resource, never()).awaitRevision(anyInt(), any(Duration.class));
    }
    verify(index).close();
  }

  @Test
  void executionsCombineEveryViewWithoutCumulativeBudgetDeclines() {
    final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
    final DerivedIndexMaintainer.Resource resource = mock(DerivedIndexMaintainer.Resource.class);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    final JsonResourceSession derivedSession = mock(JsonResourceSession.class, RETURNS_DEEP_STUBS);
    final IndexDef definition = mock(IndexDef.class);
    final IndexDef secondDefinition = mock(IndexDef.class);
    final IndexDef projection = mock(IndexDef.class);
    final TemporalAggregateIndexes.CatchUp tail = mock(TemporalAggregateIndexes.CatchUp.class);
    final StorageEngineReader[] sources = new StorageEngineReader[514];
    doReturn(derivedSession).when(index).getResourceSession();
    when(definition.hasSameDefinition(definition)).thenReturn(true);
    when(secondDefinition.getID()).thenReturn(1);
    when(secondDefinition.hasSameDefinition(secondDefinition)).thenReturn(true);
    when(derivedSession.getRtxIndexController(0).getIndexes().getIndexDef(1, IndexType.TEMPORAL_AGGREGATE)).thenReturn(
        secondDefinition);
    when(derivedSession.getRtxIndexController(0).getIndexes().getIndexDef(0, IndexType.TEMPORAL_AGGREGATE)).thenReturn(
        definition);
    when(session.getDerivedResource()).thenReturn(resource);
    for (int offset = 0; offset < sources.length; offset++) {
      final StorageEngineReader source = mock(StorageEngineReader.class);
      doReturn(session).when(source).getResourceSession();
      when(source.getUberPage()).thenReturn(new UberPage(offset + 3));
      when(source.getRevisionNumber()).thenReturn(offset + 2);
      when(resource.openCatchUpSnapshot(source)).thenReturn(new DerivedIndexSnapshot(index, offset + 1));
      sources[offset] = source;
    }
    final Module module = mock(Module.class);
    final Expr body = mock(Expr.class);
    when(module.getBody()).thenReturn(body);
    final Query query = new Query(module);
    final QueryContext context = mock(QueryContext.class);
    final QueryExecution[] firstExecution = new QueryExecution[1];
    try (var readers = new DerivedAggregateReaders(); var aggregates = mockStatic(TemporalAggregateIndexes.class);
        var prefixes = mockConstruction(DerivedAggregateCatchUp.TailCache.class)) {
      aggregates.when(() -> TemporalAggregateIndexes.catchUp(any(StorageEngineReader.class), anyInt(),
          any(IndexDef.class), any(IndexDef.class), any(TemporalAggregateCatchUpBudget.Budget.class)))
                .thenAnswer(invocation -> {
            final TemporalAggregateCatchUpBudget.Budget budget = invocation.getArgument(4);
            budget.chunk();
            return tail;
          });
      doAnswer(invocation -> {
        firstExecution[0] = QueryExecution.current();
        for (int offset = 0; offset < 512; offset++) {
          final StorageEngineReader source = sources[offset];
          final DerivedAggregateCatchUp view = readers.borrowCatchUp(source);
          assertNotNull(view);
          assertSame(tail, view.tail(source, definition, projection));
        }
        final StorageEngineReader source = sources[512];
        final DerivedAggregateCatchUp view = readers.borrowCatchUp(source);
        assertNotNull(view);
        assertSame(tail, view.tail(source, definition, projection));
        assertSame(tail, view.tail(source, definition, projection));
        assertSame(tail, view.tail(source, secondDefinition, projection));
        return Int32.ONE;
      }).when(body).evaluate(any(), any());
      query.evaluate(context);
      doAnswer(invocation -> {
        final StorageEngineReader source = sources[512];
        final DerivedAggregateCatchUp view = readers.borrowCatchUp(source);
        assertNotNull(view);
        assertSame(tail, view.tail(source, definition, projection));
        return Int32.ONE;
      }).when(body).evaluate(any(), any());
      query.evaluate(context);
      firstExecution[0].run(() -> {
        final StorageEngineReader source = sources[513];
        final DerivedAggregateCatchUp view = readers.borrowCatchUp(source);
        assertNotNull(view);
        assertSame(tail, view.tail(source, definition, projection));
      });
      assertEquals(0, prefixes.constructed().size(), "Lag-one views must not allocate a historical prefix cache");
      aggregates.verify(() -> TemporalAggregateIndexes.catchUpIncrementally(any(StorageEngineReader.class), anyInt(),
          any(IndexDef.class), any(IndexDef.class), any(TemporalAggregateCatchUpBudget.Budget.class),
          any(TemporalAggregateIndexes.CatchUpAccumulator.class)), never());
    }
  }
}
