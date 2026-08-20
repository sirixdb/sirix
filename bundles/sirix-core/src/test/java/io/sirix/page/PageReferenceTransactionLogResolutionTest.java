/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.settings.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PageReferenceTransactionLogResolutionTest {

  @Test
  @DisplayName("A copied log reference resolves after its original is flushed without global history")
  void copiedReferenceResolvesDurableResultFromSharedHandle() {
    final PageReference original = new PageReference();
    original.bindToTransactionLog(17, 4, false);
    final PageReference copy = new PageReference(original);
    final PageReference.TransactionLogReference frozenIdentity = original.transactionLogReference();
    final long hash = 0x0102_0304_0506_0708L;

    assertTrue(PageReference.completeTransactionLogReference(frozenIdentity, 8_192L, hash));
    assertTrue(original.refreshTransactionLogReference());
    assertTrue(copy.refreshTransactionLogReference());

    assertDurable(original, 8_192L, hash, true);
    assertDurable(copy, 8_192L, hash, true);
    assertNull(original.transactionLogReference(), "the owner must not retain a side object after completion");
    assertNull(copy.transactionLogReference(), "a stale copy drops the handle once it has resolved too");
  }

  @Test
  @DisplayName("A stale copy follows a superseding epoch and refuses the outdated flush result")
  void supersededReferenceForwardsBeforeAcceptingDurableResult() {
    final PageReference owner = new PageReference();
    owner.bindToTransactionLog(5, 1, false);
    final PageReference.TransactionLogReference oldIdentity = owner.transactionLogReference();
    final PageReference staleCopy = new PageReference(owner);

    assertTrue(owner.bindToTransactionLog(9, 2, true));
    final PageReference.TransactionLogReference currentIdentity = owner.transactionLogReference();

    assertFalse(PageReference.completeTransactionLogReference(oldIdentity, 111L, 1L),
        "an in-flight flush of the superseded page must not publish its outdated image");
    assertFalse(staleCopy.refreshTransactionLogReference(), "the forwarded target is still in the log");
    assertEquals(9, staleCopy.getLogKey());
    assertEquals(2, staleCopy.getActiveTilGeneration());
    assertEquals(Constants.NULL_ID_LONG, staleCopy.getKey());

    final long currentHash = 0x0908_0706_0504_0302L;
    assertTrue(PageReference.completeTransactionLogReference(currentIdentity, 222L, currentHash));
    assertTrue(owner.refreshTransactionLogReference());
    assertTrue(staleCopy.refreshTransactionLogReference());

    assertDurable(owner, 222L, currentHash, true);
    assertDurable(staleCopy, 222L, currentHash, true);
  }

  @Test
  @DisplayName("The manual trie-growth copy shares the same resolution handle")
  void manuallyPopulatedReferenceCanShareResolution() {
    final PageReference source = new PageReference();
    source.bindToTransactionLog(3, 7, false);

    final PageReference manualCopy = new PageReference();
    manualCopy.setLogKey(source.getLogKey());
    manualCopy.setActiveTilGeneration(source.getActiveTilGeneration());
    manualCopy.shareTransactionLogReference(source);

    assertTrue(PageReference.completeTransactionLogReference(source.transactionLogReference(), 512L));
    assertTrue(manualCopy.refreshTransactionLogReference());
    assertDurable(manualCopy, 512L, 0L, false);
  }

  @Test
  @DisplayName("A pending immutable page publishes without allocating a copied resolution identity")
  void pendingImmutablePagePreservesIdentityUntilForegroundPublication() {
    final byte[] payload = {4, 5, 6};
    final OverflowPage page = new OverflowPage(payload);
    final PageReference reference = new PageReference();
    reference.setPage(page);
    reference.bindPendingPageWrite(page);

    assertTrue(reference.hasPendingPageWrite());
    assertSame(page, reference.getPage());
    assertThrows(IllegalStateException.class, () -> new PageReference(reference),
        "generic CoW must not duplicate a pending reference; HOTLeafPage.copy shares its identity explicitly");

    reference.completePendingPageWrite(4_096L);

    assertFalse(reference.hasPendingPageWrite());
    assertEquals(4_096L, reference.getKey());
    assertNull(reference.getPage());
  }

  @Test
  @DisplayName("A present zero checksum survives copy and transaction-log publication")
  void zeroChecksumHasIndependentPresence() {
    final PageReference owner = new PageReference();
    owner.bindToTransactionLog(6, 3, false);
    final PageReference copy = new PageReference(owner);

    assertTrue(PageReference.completeTransactionLogReference(owner.transactionLogReference(), 1_024L, 0L));
    assertTrue(owner.refreshTransactionLogReference());
    assertTrue(copy.refreshTransactionLogReference());

    assertDurable(owner, 1_024L, 0L, true);
    assertDurable(copy, 1_024L, 0L, true);
  }

  private static void assertDurable(final PageReference reference, final long key, final long hash,
      final boolean hashPresent) {
    assertEquals(key, reference.getKey());
    assertEquals(Constants.NULL_ID_INT, reference.getLogKey());
    assertEquals(-1, reference.getActiveTilGeneration());
    assertEquals(hashPresent, reference.hasHash());
    assertEquals(hash, reference.getHashAsLong());
  }
}
