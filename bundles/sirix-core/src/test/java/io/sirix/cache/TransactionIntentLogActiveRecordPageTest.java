package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class TransactionIntentLogActiveRecordPageTest {
  @Test
  void frozenPinnedStructuralAndCompletePagesAreNeverVisited() {
    try (final TransactionIntentLog log = newLog()) {
      final KeyValueLeafPage earlier = recordPage();
      log.put(new PageReference(), PageContainer.getInstance(earlier, earlier));
      log.snapshot();
      final KeyValueLeafPage active = recordPage();
      log.put(new PageReference(), PageContainer.getInstance(recordPage(), active));
      final HOTLeafPage structural = mock(HOTLeafPage.class);
      log.put(new PageReference(), PageContainer.getInstance(structural, structural));
      final List<KeyValueLeafPage> visited = new ArrayList<>();
      log.forEachActiveRecordPage(visited::add);
      assertEquals(List.of(active), visited);

      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_PROMOTE_TO_TIL);
      log.cleanupSnapshot();
      visited.clear();
      log.forEachActiveRecordPage(visited::add);
      assertEquals(List.of(active), visited, "pinning a frozen page does not make it current-epoch state");
    }
  }

  @Test
  void sidePublicationCanAddAnEntryWithoutVisitingItInTheSamePass() {
    try (final TransactionIntentLog log = newLog()) {
      final KeyValueLeafPage earlier = recordPage();
      log.put(new PageReference(), PageContainer.getInstance(earlier, earlier));
      log.snapshot();
      log.setSnapshotDiskOffset(0, TransactionIntentLog.SNAPSHOT_RETRY_NEXT_EPOCH);
      final KeyValueLeafPage active = recordPage();
      log.put(new PageReference(), PageContainer.getInstance(active, active));
      final List<KeyValueLeafPage> visited = new ArrayList<>();
      log.forEachActiveRecordPage(page -> {
        visited.add(page);
        log.cleanupSnapshot();
      });
      assertEquals(List.of(active), visited);
      visited.clear();
      log.forEachActiveRecordPage(visited::add);
      assertEquals(List.of(active, earlier), visited);
    }
  }

  @Test
  void retiredPagesAreExcludedAndSameSlotReplacementIsCurrent() {
    try (final TransactionIntentLog log = newLog()) {
      final KeyValueLeafPage closed = recordPage();
      when(closed.isClosed()).thenReturn(true);
      log.put(new PageReference(), PageContainer.getInstance(closed, closed));
      final KeyValueLeafPage orphaned = recordPage();
      when(orphaned.isOrphaned()).thenReturn(true);
      log.put(new PageReference(), PageContainer.getInstance(orphaned, orphaned));
      final PageReference reference = new PageReference();
      final KeyValueLeafPage original = recordPage();
      log.put(reference, PageContainer.getInstance(original, original));
      final KeyValueLeafPage replacement = recordPage();
      log.put(reference, PageContainer.getInstance(replacement, replacement));
      final List<KeyValueLeafPage> visited = new ArrayList<>();
      log.forEachActiveRecordPage(visited::add);
      assertEquals(List.of(replacement), visited);
    }
  }

  @Test
  void aVisitorCannotRotateTheStorageEpoch() {
    try (final TransactionIntentLog log = newLog()) {
      final KeyValueLeafPage page = recordPage();
      log.put(new PageReference(), PageContainer.getInstance(page, page));
      assertThrows(IllegalStateException.class, () -> log.forEachActiveRecordPage(ignored -> log.snapshot()));
    }
  }

  private static TransactionIntentLog newLog() {
    return new TransactionIntentLog(mock(BufferManager.class, RETURNS_DEEP_STUBS), 64);
  }

  private static KeyValueLeafPage recordPage() {
    final KeyValueLeafPage page = mock(KeyValueLeafPage.class);
    when(page.getIndexType()).thenReturn(IndexType.NAME);
    return page;
  }
}
