package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.pax.GlobalStringDictionaries;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link KeyValueLeafPage#deepCopy()} clones every carrier reference through the copy constructor —
 * which REFUSES a reference whose immutable page write is pending in the writer's side-page batch.
 * A carrier staged at bulk adoption must therefore be SHARED by the copy (the HOT leaf CoW rule),
 * so every copy observes the durable key that publication installs on the one handle. Pinned here
 * because no load path reaches the branch today (adopted leaves serialize in place); a future path
 * that stages a carrier of a page the flush lane still deep-copies would otherwise fail inside a
 * flush worker with no test warning.
 */
final class KeyValueLeafPageDeepCopyPendingReferenceTest {

  private static final int REVISION = 1;

  @Test
  @DisplayName("a pending carrier reference is shared by the deep copy; a durable one is cloned")
  void pendingReferencesAreSharedDurableOnesCloned() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("deep-copy-pending").build();
    final KeyValueLeafPage page = new KeyValueLeafPage(7, IndexType.DOCUMENT, config, REVISION, null, null);
    final PageReference pending = new PageReference();
    final OverflowPage carrier = new OverflowPage("a staged overflow carrier".getBytes(StandardCharsets.UTF_8));
    pending.setPage(carrier);
    pending.bindPendingPageWrite(carrier);
    final PageReference durable = new PageReference();
    durable.setKey(4096L);
    page.setPageReference(7L << 10 | 5L, pending);
    page.setPageReference(7L << 10 | 6L, durable);
    KeyValueLeafPage copy = null;
    try {
      copy = page.deepCopy();
      assertSame(pending, copy.getReferencesMap().get(7L << 10 | 5L),
          "a pending carrier must be shared, not cloned — the copy constructor refuses pending references");
      final PageReference clonedDurable = copy.getReferencesMap().get(7L << 10 | 6L);
      assertNotSame(durable, clonedDurable, "a durable reference is still cloned");
      assertEquals(4096L, clonedDurable.getKey());
      assertTrue(pending.hasPendingPageWrite(), "sharing must not disturb the pending marker");
    } finally {
      if (copy != null) {
        copy.retire();
      }
      pending.cancelPendingPageWrite();
      page.retire();
    }
  }

  @Test
  @DisplayName("the deep copy carries the page's dictionary resolver: the copy IS the page")
  void theDictionaryResolverTravelsWithTheCopy() {
    final ResourceConfiguration config = ResourceConfiguration.newBuilder("deep-copy-resolver").build();
    final KeyValueLeafPage page = new KeyValueLeafPage(7, IndexType.DOCUMENT, config, REVISION, null, null);
    final GlobalStringDictionaries resolver = new GlobalStringDictionaries() {
      @Override
      public boolean hasDictionary(final int tag) {
        return true;
      }

      @Override
      public boolean accepts(final int tag, final long dictionaryKey, final int recordedEntryCount) {
        return false;
      }

      @Override
      public int idOf(final int tag, final byte[] value, final int offset, final int length) {
        return ID_ABSENT;
      }

      @Override
      public byte[] valueOf(final int tag, final long dictionaryKey, final int recordedEntryCount, final int id) {
        return null;
      }

      @Override
      public long dictionaryKey(final int tag) {
        return 42L;
      }

      @Override
      public int dictionaryEntryCount(final int tag) {
        return 0;
      }
    };
    page.setGlobalStringDictionaries(resolver);
    KeyValueLeafPage copy = null;
    try {
      copy = page.deepCopy();
      // The serialization copy is the page the flush lane encodes, and the page the encode-completion
      // listener speaks for. A copy without the resolver would silently keep its bytes AND never be
      // reported as encoded, so its segment could never be sealed.
      assertSame(resolver, copy.globalStringDictionaries(), "the copy must resolve exactly what the page does");
    } finally {
      if (copy != null) {
        copy.retire();
      }
      page.retire();
    }
  }
}
