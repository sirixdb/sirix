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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class DerivedAggregateNonblockingTest {
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
  void executionsShareTheirBudgetAcrossViewsAndRetryCachedDeclines() {
    final AbstractResourceSession<?, ?> session = mock(AbstractResourceSession.class);
    final DerivedIndexMaintainer.Resource resource = mock(DerivedIndexMaintainer.Resource.class);
    final StorageEngineReader index = mock(StorageEngineReader.class);
    final JsonResourceSession derivedSession = mock(JsonResourceSession.class, RETURNS_DEEP_STUBS);
    final IndexDef definition = mock(IndexDef.class);
    final IndexDef secondDefinition = mock(IndexDef.class);
    final IndexDef projection = mock(IndexDef.class);
    final TemporalAggregateIndexes.CatchUp tail = mock(TemporalAggregateIndexes.CatchUp.class);
    final UberPage publication = new UberPage(516);
    final StorageEngineReader[] sources = new StorageEngineReader[514];
    doReturn(derivedSession).when(index).getResourceSession();
    when(definition.hasSameDefinition(definition)).thenReturn(true);
    when(secondDefinition.getID()).thenReturn(1);
    when(secondDefinition.hasSameDefinition(secondDefinition)).thenReturn(true);
    when(derivedSession.getRtxIndexController(0).getIndexes().getIndexDef(1, IndexType.TEMPORAL_AGGREGATE))
        .thenReturn(secondDefinition);
    when(derivedSession.getRtxIndexController(0).getIndexes().getIndexDef(0, IndexType.TEMPORAL_AGGREGATE))
        .thenReturn(definition);
    when(session.getDerivedResource()).thenReturn(resource);
    for (int offset = 0; offset < sources.length; offset++) {
      final StorageEngineReader source = mock(StorageEngineReader.class);
      doReturn(session).when(source).getResourceSession();
      when(source.getUberPage()).thenReturn(publication);
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
    try (var readers = new DerivedAggregateReaders(); var aggregates = mockStatic(TemporalAggregateIndexes.class)) {
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
        assertNull(view.tail(source, definition, projection));
        assertNull(view.tail(source, definition, projection));
        assertNull(view.tail(source, secondDefinition, projection));
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
        assertNull(view.tail(source, definition, projection));
      });
    }
  }
}
