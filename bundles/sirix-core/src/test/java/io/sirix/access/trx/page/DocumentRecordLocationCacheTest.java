/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.cache.PageContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class DocumentRecordLocationCacheTest {

  @Test
  void directSlotRequiresExactKeyAndCollisionReplacesOnlyThatSlot() {
    final NodeStorageEngineWriter.DocumentRecordLocationCache cache =
        new NodeStorageEngineWriter.DocumentRecordLocationCache();
    final long firstIdentity = 0x0000_0002_0000_0003L;
    final long collidingIdentity = 0xFFFF_FFFE_0000_0007L;

    assertEquals(PageContainer.NULL_TRANSACTION_LOG_IDENTITY, cache.get(19));

    cache.put(19, firstIdentity);
    assertEquals(firstIdentity, cache.get(19));
    assertEquals(PageContainer.NULL_TRANSACTION_LOG_IDENTITY, cache.get(20));

    cache.put(19 + NodeStorageEngineWriter.DocumentRecordLocationCache.CAPACITY, collidingIdentity);
    assertEquals(PageContainer.NULL_TRANSACTION_LOG_IDENTITY, cache.get(19));
    assertEquals(collidingIdentity,
        cache.get(19 + NodeStorageEngineWriter.DocumentRecordLocationCache.CAPACITY));
  }

  @Test
  void clearInvalidatesCurrentAndPinnedIdentitiesWithoutRetainingPages() {
    final NodeStorageEngineWriter.DocumentRecordLocationCache cache =
        new NodeStorageEngineWriter.DocumentRecordLocationCache();
    final long currentIdentity = 0x0000_0004_0000_0001L;
    final long pinnedIdentity = 0xFFFF_FFFE_0000_0002L;

    cache.put(1, currentIdentity);
    cache.put(2, pinnedIdentity);
    cache.clear();

    assertEquals(PageContainer.NULL_TRANSACTION_LOG_IDENTITY, cache.get(1));
    assertEquals(PageContainer.NULL_TRANSACTION_LOG_IDENTITY, cache.get(2));
  }
}
