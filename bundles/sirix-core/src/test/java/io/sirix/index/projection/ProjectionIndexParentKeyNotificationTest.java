/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Parent-key propagation coverage for the projection notification hot path.
 *
 * <p>
 * The listener feeds a REAL {@link ProjectionBulkLoad} and reads the transaction's REAL path summary,
 * and every assertion on the load is against its observable state. Both classes are final, so a
 * Mockito mock of either is an inline retransformation of the production class itself; once earlier
 * tests have warmed the listener up, a JIT that inlined the real getter keeps running it and bypasses
 * the mock. Only interfaces are mocked here: the storage writer (the failure-injection seam), the
 * maintenance transaction and the record handed back by a read.
 * </p>
 */
final class ProjectionIndexParentKeyNotificationTest {

  private static final long RECORD_KEY = 42L;
  private static final long ARRAY_ROOT_KEY = 7L;
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
  }

  @AfterEach
  void tearDown() {
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void parentAwareBulkNotificationDoesNotReadTheChangedRecordBack() {
    try (final Fixture fixture = fixture()) {
      fixture.listener.listen(IndexController.ChangeType.INSERT, RECORD_KEY, NodeKind.OBJECT, ARRAY_ROOT_KEY,
          fixture.rootPcr, null, null);

      assertEquals(RECORD_KEY, fixture.load.currentRecordKey(), "the record must be observed by the load");
      verifyNoInteractions(fixture.storageEngineWriter);
    }
  }

  @Test
  void legacyPrimitiveNotificationStillReadsTheChangedRecordToRecoverItsParent() {
    try (final Fixture fixture = fixture()) {
      final ImmutableNode self = mock(ImmutableNode.class);
      when(self.getParentKey()).thenReturn(ARRAY_ROOT_KEY);
      doReturn(self).when(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);

      fixture.listener.listen(IndexController.ChangeType.INSERT, RECORD_KEY, NodeKind.OBJECT, fixture.rootPcr, null,
          null);

      verify(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);
      assertEquals(RECORD_KEY, fixture.load.currentRecordKey(), "the record must be observed by the load");
    }
  }

  @Test
  void recordReadFailureIsNotReclassifiedAsAnAbsentNode() {
    try (final Fixture fixture = fixture()) {
      final IllegalStateException sentinel = new IllegalStateException("injected record read failure");
      doThrow(sentinel).when(fixture.storageEngineWriter).getRecord(RECORD_KEY, IndexType.DOCUMENT, -1);

      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.listener.listen(IndexController.ChangeType.INSERT,
              RECORD_KEY, NodeKind.OBJECT, fixture.rootPcr, null, null));

      assertSame(sentinel, failure, "storage failure must reach the transaction's fail-closed boundary");
      // The record read fails before the load can observe or mutate anything: no record is open, the
      // armed record-set array is unchanged, and the build is still live rather than closed.
      assertEquals(-1L, fixture.load.currentRecordKey(), "a failed read must not open a record");
      assertTrue(fixture.load.isArrayRootInstance(ARRAY_ROOT_KEY), "a failed read must not move the array root");
      assertFalse(fixture.load.isFinished(), "a failed read must not close the build");
      assertEquals(0L, fixture.load.rowsEmitted(), "a failed read must not emit a row");
    }
  }

  @Test
  void parentAwareListenerDefaultKeepsLegacyImplementationsCompatible() {
    final LegacyPrimitiveListener listener = new LegacyPrimitiveListener();

    listener.listen(IndexController.ChangeType.DELETE, RECORD_KEY, NodeKind.STRING_VALUE, ARRAY_ROOT_KEY, 11L, null,
        null);

    assertEquals(1, listener.calls);
    assertEquals(RECORD_KEY, listener.nodeKey);
    assertEquals(11L, listener.pathNodeKey);
  }

  /**
   * A record set {@code /[]} in the path summary, and a real load armed for it whose record-set array
   * instance is {@link #ARRAY_ROOT_KEY}. The listener reads document records only through the mocked
   * writer, so the node keys it is notified about are free to be synthetic.
   */
  private static Fixture fixture() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
    JsonNodeTrx wtx = null;
    ProjectionBulkLoad load = null;
    try {
      wtx = session.beginNodeTrx();
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader("[{\"value\":1}]"), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();

      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);
      final LongSet rootPcrs = wtx.getPathSummary().getPCRsForPaths(Set.of(indexDef.getProjectionRootPath()));
      assertEquals(1, rootPcrs.size(), "the fixture must hold exactly one record-set path class");
      final long rootPcr = rootPcrs.iterator().nextLong();

      load = ProjectionBulkLoad.begin(indexDef, session.getResourceConfig().getResource().toString(),
          wtx.getPathSummary(), wtx.getStorageEngineWriter());
      load.noteArrayRootInstance(ARRAY_ROOT_KEY, wtx);

      final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
      final ProjectionIndexChangeListener listener = new ProjectionIndexChangeListener(storageEngineWriter,
          wtx.getPathSummary(), indexDef, mock(NodeReadOnlyTrx.class), load);
      return new Fixture(session, wtx, listener, load, storageEngineWriter, rootPcr);
    } catch (final RuntimeException | Error failure) {
      try {
        if (load != null) {
          load.abort();
        }
        if (wtx != null) {
          wtx.rollback();
          wtx.close();
        }
      } finally {
        session.close();
      }
      throw failure;
    }
  }

  private record Fixture(JsonResourceSession session, JsonNodeTrx wtx, ProjectionIndexChangeListener listener,
      ProjectionBulkLoad load, StorageEngineWriter storageEngineWriter, long rootPcr) implements AutoCloseable {

    /** Drop the armed build and its uncommitted tombstone; nothing of it outlives the test. */
    @Override
    public void close() {
      try {
        load.abort();
      } finally {
        try {
          wtx.rollback();
          wtx.close();
        } finally {
          session.close();
        }
      }
    }
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
