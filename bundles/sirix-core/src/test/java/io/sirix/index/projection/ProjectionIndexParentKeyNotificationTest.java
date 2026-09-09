/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Parent-key propagation coverage for the projection notification hot path. */
final class ProjectionIndexParentKeyNotificationTest {

  private static final long RECORD_KEY = 42L;
  private static final long ARRAY_ROOT_KEY = 7L;
  private static final long ROOT_PCR = 11L;

  @Test
  void parentAwareBulkNotificationDoesNotReadTheChangedRecordBack() throws Exception {
    final Fixture fixture = fixture();

    fixture.listener.listen(IndexController.ChangeType.INSERT, RECORD_KEY, NodeKind.OBJECT, ARRAY_ROOT_KEY, ROOT_PCR,
        null, null);

    verify(fixture.load).observeRecord(RECORD_KEY);
    verifyNoInteractions(fixture.storageEngineWriter);
  }

  @Test
  void legacyPrimitiveNotificationStillReadsTheChangedRecordToRecoverItsParent() throws Exception {
    final Fixture fixture = fixture();
    final ImmutableNode self = mock(ImmutableNode.class);
    when(self.getParentKey()).thenReturn(ARRAY_ROOT_KEY);
    doReturn(self).when(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);

    fixture.listener.listen(IndexController.ChangeType.INSERT, RECORD_KEY, NodeKind.OBJECT, ROOT_PCR, null, null);

    verify(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);
    verify(fixture.load).observeRecord(RECORD_KEY);
  }

  @Test
  void recordReadFailureIsNotReclassifiedAsAnAbsentNode() throws Exception {
    final Fixture fixture = fixture();
    final IllegalStateException sentinel = new IllegalStateException("injected record read failure");
    doThrow(sentinel).when(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> fixture.listener.listen(IndexController.ChangeType.INSERT,
            RECORD_KEY, NodeKind.OBJECT, ROOT_PCR, null, null));

    assertSame(sentinel, failure, "storage failure must reach the transaction's fail-closed boundary");
    // The listener must probe whether its constructor-bound bulk load has finished before routing
    // the notification. The record read then fails before the load can observe or mutate anything.
    verify(fixture.load).isFinished();
    verifyNoMoreInteractions(fixture.load);
  }

  @Test
  void parentAwareListenerDefaultKeepsLegacyImplementationsCompatible() {
    final LegacyPrimitiveListener listener = new LegacyPrimitiveListener();

    listener.listen(IndexController.ChangeType.DELETE, RECORD_KEY, NodeKind.STRING_VALUE, ARRAY_ROOT_KEY, ROOT_PCR,
        null, null);

    assertEquals(1, listener.calls);
    assertEquals(RECORD_KEY, listener.nodeKey);
    assertEquals(ROOT_PCR, listener.pathNodeKey);
  }

  private static Fixture fixture() throws Exception {
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final PathSummaryReader pathSummary = mock(PathSummaryReader.class);
    final LongOpenHashSet rootPcrs = new LongOpenHashSet();
    rootPcrs.add(ROOT_PCR);
    when(pathSummary.getPCRsForPaths(any())).thenReturn(rootPcrs);
    // Seeding walks UP from every matched record-set root to reject an overlapping nested root, so
    // the summary must actually resolve ROOT_PCR. A record-set root directly under the document
    // node has no further ancestor, which is what the empty parent walk models here.
    when(pathSummary.moveTo(ROOT_PCR)).thenReturn(true);
    when(pathSummary.moveToParent()).thenReturn(false);

    final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
    final ProjectionBulkLoad load = mock(ProjectionBulkLoad.class);
    when(load.isArrayRootInstance(ARRAY_ROOT_KEY)).thenReturn(true);
    when(load.currentRecordKey()).thenReturn(-1L);

    // Inject the armed load through the package-private constructor rather than reflecting a field
    // in afterwards. bulkLoad and the FINAL bulkSkipsNamedKinds are correlated and derived together;
    // replacing only the reference left the boolean derived from whatever the global registry held,
    // which is a silent skew between two fields that must agree. Injecting removes that possibility
    // instead of asserting against it.
    final ProjectionIndexChangeListener listener = new ProjectionIndexChangeListener(storageEngineWriter, pathSummary,
        indexDef, mock(NodeReadOnlyTrx.class), load);
    // The constructor derives bulkSkipsNamedKinds by asking the load once. That is construction
    // bookkeeping, not behaviour any test here observes, so the fixture hands back a clean mock —
    // otherwise every verifyNoInteractions would trip on the derivation itself.
    clearInvocations(load);
    return new Fixture(listener, load, storageEngineWriter);
  }

  private record Fixture(ProjectionIndexChangeListener listener, ProjectionBulkLoad load,
      StorageEngineWriter storageEngineWriter) {
  }

  private static final class LegacyPrimitiveListener implements PathNodeKeyChangeListener {

    private int calls;
    private long nodeKey;
    private long pathNodeKey;

    @Override
    public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
      // The test exercises the primitive contract.
    }

    @Override
    public void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
        final long pathNodeKey, final QNm name, final Str value) {
      calls++;
      this.nodeKey = nodeKey;
      this.pathNodeKey = pathNodeKey;
    }
  }
}
