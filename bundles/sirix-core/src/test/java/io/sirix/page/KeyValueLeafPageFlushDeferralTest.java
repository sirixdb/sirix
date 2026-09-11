package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.Test;

final class KeyValueLeafPageFlushDeferralTest {
  @Test
  void onlyCompletionOfEveryPendingCarrierResetsTheDeferralWindow() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("carrier-progress").build();
    final KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.NAME, config, 1, null, null, false);
    try {
      final PageReference first = pendingCarrier();
      final PageReference second = pendingCarrier();
      page.setPageReference(1L, first);
      page.setPageReference(2L, second);
      page.noteFlushDeferral();
      page.noteFlushDeferral();
      page.resetFlushDeferralsIfCarriersResolved();
      assertEquals(2, page.flushDeferrals(), "unchanged pending writes must retain their retry count");
      first.completePendingPageWrite(100L);
      page.resetFlushDeferralsIfCarriersResolved();
      assertEquals(2, page.flushDeferrals(), "one unresolved carrier still prevents a reset");
      second.completePendingPageWrite(200L);
      page.resetFlushDeferralsIfCarriersResolved();
      assertEquals(0, page.flushDeferrals(), "completed carriers permit a fresh generation of overflow values");

      page.setPageReference(3L, pendingCarrier());
      page.noteFlushDeferral();
      page.resetFlushDeferralsIfCarriersResolved();
      assertEquals(1, page.flushDeferrals(), "the next batch must retain its own deferrals while pending");
    } finally {
      page.close();
    }
  }

  @Test
  void anUnstagedCarrierCannotResetTheDeferralWindow() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("carrier-unresolved").build();
    final KeyValueLeafPage page = new KeyValueLeafPage(0L, IndexType.NAME, config, 1, null, null, false);
    try {
      final PageReference unresolved = new PageReference();
      unresolved.setPage(new OverflowPage(new byte[] {1, 2, 3}));
      page.setPageReference(1L, unresolved);
      page.noteFlushDeferral();
      page.resetFlushDeferralsIfCarriersResolved();
      assertEquals(1, page.flushDeferrals());
    } finally {
      page.close();
    }
  }

  private static PageReference pendingCarrier() {
    final OverflowPage carrier = new OverflowPage(new byte[] {1, 2, 3});
    final PageReference reference = new PageReference();
    reference.setPage(carrier);
    reference.bindPendingPageWrite(carrier);
    return reference;
  }
}
