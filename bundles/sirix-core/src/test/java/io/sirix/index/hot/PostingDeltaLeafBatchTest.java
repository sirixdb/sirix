package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ValidTimeIndexPage;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class PostingDeltaLeafBatchTest {
  private static final ValidTimeKey KEY = new ValidTimeKey((byte) 0, 1, 2);
  private static final int COMPOSITE_LENGTH = ValidTimeKeySerializer.KEY_BYTES + Integer.BYTES;

  @Test
  void reclaimingAnInvalidatedOwnerCannotReviveAnOlderView() {
    try (final Fixture fixture = new Fixture(false)) {
      final TransactionIntentLog log = fixture.log;
      when(log.claimHOTPostingViewOwner(anyLong(), any())).thenCallRealMethod();
      doAnswer(invocation -> invocation.callRealMethod()).when(log).invalidateHOTPostingViews(anyLong());
      fixture.writer.indexNodeKey(KEY, 1001);
      final long scope = TransactionIntentLog.indexScope(IndexType.VALIDTIME, 0);
      log.invalidateHOTPostingViews(scope);
      log.claimHOTPostingViewOwner(scope, fixture.writer); // an intervening mutation prelude
      fixture.replaceLastBit(1002);
      assertFalse(fixture.writer.remove(KEY, 1001), "the reloaded view no longer contains this posting");
      verify(fixture.storage, never()).markTransactionRollbackOnly(any());
    }
  }

  @Test
  void reusedViewAnswersNoOpsWithoutReadingPagesAgain() {
    try (final Fixture fixture = new Fixture(false)) {
      when(fixture.log.claimHOTPostingViewOwner(anyLong(), any())).thenCallRealMethod();
      fixture.writer.indexNodeKey(KEY, 1001);
      clearInvocations(fixture.storage);
      for (int i = 0; i < 10; i++) {
        fixture.writer.indexNodeKey(KEY, 1001);
        fixture.writer.indexNodeKey(KEY, 2);
        assertFalse(fixture.writer.remove(KEY, 65000));
      }
      verify(fixture.storage, never()).loadHOTPage(any(PageReference.class));
      verify(fixture.storage, never()).loadHOTPageAndGuard(any(PageReference.class));
      verify(fixture.storage, never()).markTransactionRollbackOnly(any());
    }
  }

  @Test
  void tornBatchDiscardsPartialDeltasAndReleasesTheRecoveryGuard() {
    try (final Fixture fixture = new Fixture(false)) {
      fixture.evictDuringRead = true;
      // The final delta already added this bit. Retrying must discard the first partial batch,
      // or its repeated sequence zero would be rejected as a non-contiguous sequence.
      assertDoesNotThrow(() -> fixture.writer.indexNodeKey(KEY, 1001));
      assertTrue(fixture.evictions > 0);
      verify(fixture.storage, times(1)).loadHOTPageAndGuard(fixture.root);
      verify(fixture.storage, never()).markTransactionRollbackOnly(any());
      final HOTLeafPage guarded = fixture.leaves.getLast();
      assertEquals(0, guarded.getGuardCount());
      assertTrue(guarded.isClosed(), "retired recovery leaf must be released when the batch ends");

      fixture.evictDuringRead = false;
      assertDoesNotThrow(() -> fixture.writer.indexNodeKey(KEY, 1001));
      verify(fixture.storage, times(1)).loadHOTPageAndGuard(fixture.root);
      verify(fixture.leaves.getLast(), never()).acquireGuard();
    }
  }

  @Test
  void stableMalformedDeltaPoisonsTheTransactionWithoutLeakingGuards() {
    try (final Fixture fixture = new Fixture(true)) {
      final IllegalArgumentException failure =
          assertThrows(IllegalArgumentException.class, () -> fixture.writer.indexNodeKey(KEY, 1001));
      assertTrue(requireNonNull(failure.getMessage()).contains("exactly one packed chunk bit"));
      verify(fixture.storage).markTransactionRollbackOnly(failure);
      verify(fixture.storage, never()).loadHOTPageAndGuard(fixture.root);
      for (final HOTLeafPage leaf : fixture.leaves) {
        assertEquals(0, leaf.getGuardCount());
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Arena arena = Arena.ofShared();
    private final List<HOTLeafPage> leaves = new ArrayList<>();
    private final StorageEngineWriter storage = mock(StorageEngineWriter.class);
    private final TransactionIntentLog log = mock(TransactionIntentLog.class);
    private final PageReference root = new PageReference().setKey(123L);
    private final HOTIndexWriter<ValidTimeKey> writer;
    private final boolean malformed;
    private boolean evictDuringRead;
    private int evictions;
    private long lastBit = 1001;

    private Fixture(final boolean malformed) {
      this.malformed = malformed;
      when(storage.getLog()).thenReturn(log);
      final ValidTimeIndexPage page = new ValidTimeIndexPage();
      page.setOrCreateReference(0, root);
      when(storage.<ValidTimeIndexPage>prepareSecondaryIndexPage(IndexType.VALIDTIME)).thenReturn(page);
      when(storage.loadHOTPage(root)).thenAnswer(_ -> newLeaf());
      when(storage.loadHOTPageAndGuard(root)).thenAnswer(_ -> {
        final HOTLeafPage leaf = newLeaf();
        assertTrue(leaf.acquireGuard());
        return leaf;
      });
      writer = HOTIndexWriter.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
    }

    private HOTLeafPage newLeaf() {
      final HOTLeafPage leaf = spy(new HOTLeafPage(123L, 1, IndexType.VALIDTIME,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0));
      final byte[] composite = new byte[COMPOSITE_LENGTH];
      ValidTimeKeySerializer.INSTANCE.serialize(KEY, composite, 0);
      HOTKeySerializer.writeChunkIdxBE(composite, ValidTimeKeySerializer.KEY_BYTES, 0);
      final NodeReferences base = new NodeReferences();
      for (int i = 0; i < 400; i++) {
        base.getNodeKeys().addLong(2L * i);
      }
      final byte[] payload = NodeReferencesSerializer.serialize(base);
      assertTrue(payload.length >= PostingDeltas.HOT_CHUNK_BYTES);
      assertTrue(leaf.put(composite, payload));
      final byte[] delta = Arrays.copyOf(composite, COMPOSITE_LENGTH + PostingDeltas.SUFFIX_BYTES);
      for (int seq = 0; seq < 3; seq++) {
        HOTKeySerializer.writeChunkIdxBE(delta, COMPOSITE_LENGTH, PostingDeltas.suffix(seq, seq == 1));
        final NodeReferences bit = new NodeReferences();
        bit.getNodeKeys()
           .addLong(seq == 0
               ? 1000
               : seq == 1
                   ? 0
                   : lastBit);
        if (malformed && seq == 1) {
          bit.getNodeKeys().addLong(2);
        }
        assertTrue(leaf.put(delta, NodeReferencesSerializer.serialize(bit)));
      }
      doAnswer(invocation -> {
        final Object result = invocation.callRealMethod();
        if (evictDuringRead && (int) invocation.getArgument(0) == 2) {
          evictions++;
          leaf.close();
        }
        return result;
      }).when(leaf).readKeyIntBE(anyInt(), anyInt());
      leaves.add(leaf);
      return leaf;
    }

    private void replaceLastBit(final long bit) {
      lastBit = bit;
      final byte[] delta = new byte[COMPOSITE_LENGTH + PostingDeltas.SUFFIX_BYTES];
      ValidTimeKeySerializer.INSTANCE.serialize(KEY, delta, 0);
      HOTKeySerializer.writeChunkIdxBE(delta, ValidTimeKeySerializer.KEY_BYTES, 0);
      HOTKeySerializer.writeChunkIdxBE(delta, COMPOSITE_LENGTH, PostingDeltas.suffix(2, false));
      final NodeReferences posting = new NodeReferences();
      posting.getNodeKeys().addLong(bit);
      final byte[] payload = NodeReferencesSerializer.serialize(posting);
      for (final HOTLeafPage leaf : leaves) {
        assertTrue(leaf.updateValue(leaf.findEntry(delta), payload));
      }
    }

    @Override
    public void close() {
      for (final HOTLeafPage leaf : leaves) {
        leaf.close();
      }
      arena.close();
    }
  }
}
