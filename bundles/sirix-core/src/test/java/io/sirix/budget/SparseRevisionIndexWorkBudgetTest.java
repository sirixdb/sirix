package io.sirix.budget;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.io.IOStorage;
import io.sirix.io.RevisionIndex;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * The production decoder/loader must use one bulk read regardless of history size or pruning.
 * Measured at 16/4096 slots: one cold read (512/131072 bytes), zero warm reads. Restoring scalar
 * loading failed both cases with 16/4096 reads. The mocked channel completes each requested read,
 * so this counts metadata read calls independently of filesystem short reads and beacon I/O.
 */
@Isolated
final class SparseRevisionIndexWorkBudgetTest {
  @ParameterizedTest
  @ValueSource(ints = {16, 4096})
  void coldLoadUsesOneBulkReadAndWarmLoadUsesNone(final int count) throws Exception {
    final ByteBuffer records = ByteBuffer.allocate(count * IOStorage.REVISIONS_FILE_RECORD_SIZE)
        .order(ByteOrder.LITTLE_ENDIAN);
    for (int revision = 0; revision < count; revision++) {
      if (revision > 0 && revision < count - 1) {
        IOStorage.writePrunedRevisionRecord(records, revision);
      } else {
        records.putLong(IOStorage.DATA_REGION_START).putLong(revision)
            .putLong(IOStorage.revisionRecordChecksum(IOStorage.DATA_REGION_START, revision, 42)).putLong(42);
      }
    }
    final AtomicInteger reads = new AtomicInteger();
    final AtomicInteger bytes = new AtomicInteger();
    final FileChannel revisions = mock(FileChannel.class);
    when(revisions.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      reads.incrementAndGet();
      final ByteBuffer target = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      final int start = Math.toIntExact(offset - IOStorage.REVISIONS_RECORDS_START);
      final int length = Math.min(target.remaining(), records.capacity() - start);
      target.put(records.array(), start, length);
      bytes.addAndGet(length);
      return length;
    });
    final FileChannelReader reader = spy(new FileChannelReader(mock(FileChannel.class), revisions,
        new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build()));
    final PageReference beacon = new PageReference();
    beacon.setPage(new UberPage(count));
    doReturn(beacon).when(reader).readUberPageReference();
    final IOStorage storage = mock(IOStorage.class, CALLS_REAL_METHODS);
    when(storage.exists()).thenReturn(true);
    when(storage.createReader()).thenReturn(reader);
    final RevisionIndexHolder holder = new RevisionIndexHolder();
    storage.loadRevisionIndex(holder);
    final RevisionIndex index = holder.get();
    assertEquals(count, index.size());
    assertEquals(1, reads.get(), "Cold history must use one revisions-file read, including tombstones");
    assertEquals(count * IOStorage.REVISIONS_FILE_RECORD_SIZE, bytes.get(), "Exactly one history pass");
    assertTrue(index.isPruned(count / 2));
    assertEquals(count / 2, index.findRevision(count / 2));
    storage.loadRevisionIndex(holder);
    assertEquals(1, reads.get(), "Warm load must not read history again");
    if (Boolean.getBoolean("sirix.workBudget.print")) {
      System.out.printf("SPARSE_HISTORY_BUDGET slots=%d coldReads=%d bytes=%d warmReads=0%n",
          count, reads.get(), bytes.get());
    }
  }
}
