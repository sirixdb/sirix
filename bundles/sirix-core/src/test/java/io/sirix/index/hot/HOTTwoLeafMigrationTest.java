/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ResourceLock("HOT_TWO_LEAF_MIGRATION_AFTER_PUBLICATION_TEST_HOOK")
final class HOTTwoLeafMigrationTest {

  @AfterEach
  void clearPublicationHook() {
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterReattachTestHook(null);
  }

  @Test
  void publicInsertMigratesOneOffPathSiblingStrandWithoutRebuild() {
    final byte[] slotZeroKey = key(0x00);
    final byte[] slotOneKey = key(0x20);
    final byte[] descendedKey = key(0x80);
    final byte[] sourceRemainingKey = key(0xA0);
    final byte[] insertedKey = key(0xC0);
    final byte[] sourceMigratedKey = key(0xE0);

    final MigrationShape shape = migrationShape();
    final PageReference rootReference = shape.rootReference;
    final HOTLeafPage source = shape.source;
    final WriterFixture fixture = writerFixture(rootReference);
    final long migrationBefore = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();

    try {
      fixture.writer.insert(insertedKey, value(5));

      assertEquals(migrationBefore + 1, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get());
      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          "the two-leaf migration must publish an invariant-clean candidate");

      final Page migratedRoot = fixture.resolve(rootReference);
      assertTrue(migratedRoot instanceof HOTIndirectPage);
      final HOTIndirectPage migrated = (HOTIndirectPage) migratedRoot;
      assertArrayEquals(new int[] {0, 1, 4, 5, 6},
          java.util.Arrays.copyOf(migrated.getPartialKeysRef(), migrated.getNumChildren()));
      assertTrue(HOTMalformedSubtreeDetector.detect(rootReference, fixture::resolve).isEmpty(),
          "the published migration must be canonical");

      final Map<Integer, Integer> actual = new LinkedHashMap<>();
      collectEntries(migrated, fixture, actual);
      assertEquals(Map.of(0x00, 1, 0x20, 2, 0x80, 3, 0xA0, 4, 0xC0, 5, 0xE0, 6), actual,
          "the source split and migrated child must preserve every key and value exactly once");
      assertEquals(3, migrated.findChildIndex(sourceRemainingKey));
      assertEquals(4, migrated.findChildIndex(insertedKey));
      assertEquals(4, migrated.findChildIndex(sourceMigratedKey));

      verify(fixture.log).releaseOrphanedHOTLeaves(anyLong(), same(rootReference),
          argThat(refs -> refs.size() == 1 && refs.get(0).getPage() == source),
          eq(TransactionIntentLog.RELEASE_SITE_TWO_LEAF_MIGRATION));
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeReachableLeaves(rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!source.isClosed()) {
        source.close();
      }
    }
  }

  @Test
  void projectionMigrationPreservesZeroLengthSlotsAndRehomesSideReferencesLocally() {
    final ProjectionMigrationShape shape = projectionMigrationShapeWithTombstones();
    final WriterFixture fixture = writerFixture(shape.rootReference, IndexType.PROJECTION);
    final long migrationBefore = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
    final long propagationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();

    try {
      fixture.writer.insert(projectionKey(0xC0), value(5));

      assertEquals(migrationBefore + 1, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get());
      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get());
      assertEquals(propagationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get(),
          "projection side references must stay on the bounded two-leaf primitive");
      assertArrayEquals(new byte[0], storedValue(shape.rootReference, projectionKey(0xA0), fixture),
          "the zero-length slot retained in the rebuilt source leaf must remain a real tombstone");
      assertArrayEquals(new byte[0], storedValue(shape.rootReference, projectionKey(0xE0), fixture),
          "the zero-length slot migrated beside K must remain a real tombstone");
      assertArrayEquals(value(5), storedValue(shape.rootReference, projectionKey(0xC0), fixture));

      final HOTLeafPage remainingOwner = routedLeaf(shape.rootReference, projectionKey(0xA0), fixture);
      final HOTLeafPage migratedOwner = routedLeaf(shape.rootReference, projectionKey(0xE0), fixture);
      assertSame(shape.remainingReference, remainingOwner.getPageReference(shape.remainingRefKey),
          "the source-side reference identity must follow its owner slot");
      assertSame(shape.migratedReference, migratedOwner.getPageReference(shape.migratedRefKey),
          "the migrated-side reference identity must follow its owner slot");
      assertSame(shape.remainingReference, shape.source.getPageReference(shape.remainingRefKey),
          "the unpublished source page remains immutable until retirement");
      assertSame(shape.migratedReference, shape.source.getPageReference(shape.migratedRefKey),
          "reattachment must never steal a PageReference from the source page");
      assertTrue(HOTMalformedSubtreeDetector.detect(shape.rootReference, fixture::resolve).isEmpty());
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
    } finally {
      closeReachableLeaves(shape.rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!shape.source.isClosed()) {
        shape.source.close();
      }
    }
  }

  @Test
  void projectionReattachFailureClosesOnlyFreshReplacementsAndLeavesSourceOwnershipUntouched() {
    final ProjectionMigrationShape shape = projectionMigrationShapeWithTombstones();
    final WriterFixture fixture = writerFixture(shape.rootReference, IndexType.PROJECTION);
    final IllegalStateException sentinel = new IllegalStateException("injected after side-reference reattach");
    final AtomicReference<HOTIndirectPage> candidate = new AtomicReference<>();
    final AtomicReference<Page> rebuiltSource = new AtomicReference<>();
    final AtomicReference<Page> migratedChild = new AtomicReference<>();
    final long migrationBefore = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
    final long propagationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get();
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterReattachTestHook(freshCandidate -> {
      candidate.set(freshCandidate);
      rebuiltSource.set(freshCandidate.getChildReference(3).getPage());
      migratedChild.set(freshCandidate.getChildReference(4).getPage());
      markRetainedMigrationChildrenDurable(freshCandidate);
      throw sentinel;
    });

    try {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.insert(projectionKey(0xC0), value(5)));

      assertSame(sentinel, failure);
      final HOTIndirectPage unpublished = candidate.get();
      assertTrue(unpublished != null, "the hook must observe the fully reattached candidate");
      assertSubtreeLeavesClosed(rebuiltSource.get());
      assertSubtreeLeavesClosed(migratedChild.get());
      assertFalse(shape.source.isClosed(), "pre-publication cleanup must not retire the source leaf");
      assertSame(shape.remainingReference, shape.source.getPageReference(shape.remainingRefKey));
      assertSame(shape.migratedReference, shape.source.getPageReference(shape.migratedRefKey));
      assertEquals(migrationBefore, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get());
      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get());
      assertEquals(propagationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE.get());
      verify(fixture.log, never()).releaseOrphanedHOTLeaves(anyLong(), any(PageReference.class),
          org.mockito.ArgumentMatchers.<List<PageReference>>any(), anyInt());
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterReattachTestHook(null);
      closeReachableLeaves(shape.rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!shape.source.isClosed()) {
        shape.source.close();
      }
    }
  }

  @Test
  void secondRegistrationFailureKeepsFirstTransferredProjectionLeafOpen() {
    final ProjectionMigrationShape shape = projectionMigrationShapeWithTombstones();
    final AtomicBoolean publicationReached = new AtomicBoolean();
    final IllegalStateException sentinel = new IllegalStateException("injected second registration failure");
    final WriterFixture fixture =
        writerFixture(shape.rootReference, IndexType.PROJECTION, publicationReached, sentinel, 2);
    final AtomicReference<HOTIndirectPage> candidate = new AtomicReference<>();
    final AtomicReference<HOTLeafPage> registeredSourceLeaf = new AtomicReference<>();
    final AtomicReference<HOTLeafPage> failedMigratedLeaf = new AtomicReference<>();
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
      final HOTIndirectPage published = (HOTIndirectPage) shape.rootReference.getPage();
      candidate.set(published);
      registeredSourceLeaf.set((HOTLeafPage) published.getChildReference(3).getPage());
      failedMigratedLeaf.set((HOTLeafPage) published.getChildReference(4).getPage());
      markRetainedMigrationChildrenDurable(published);
      publicationReached.set(true);
    });

    try {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.insert(projectionKey(0xC0), value(5)));

      assertSame(sentinel, failure);
      verify(fixture.storageEngineWriter, atLeastOnce()).markTransactionRollbackOnly(same(sentinel));
      final HOTIndirectPage published = candidate.get();
      assertTrue(published != null);
      final PageReference registeredSourceRef = published.getChildReference(3);
      final HOTLeafPage registeredSource = registeredSourceLeaf.get();
      final HOTLeafPage failedMigratedChild = failedMigratedLeaf.get();
      assertTrue(registeredSourceRef.getLogKey() >= 0,
          "the first replacement leaf must have transferred to the TIL before the injected fault");
      assertFalse(registeredSource.isClosed(), "post-failure cleanup must stop at the exact TIL-owned child reference");
      assertSame(shape.remainingReference, registeredSource.getPageReference(shape.remainingRefKey));
      assertTrue(failedMigratedChild.isClosed(),
          "the leaf whose registration failed remains locally owned and must be closed");
      assertFalse(shape.source.isClosed(), "the old graph remains independently owned until rollback");
      assertSame(shape.remainingReference, shape.source.getPageReference(shape.remainingRefKey));
      assertSame(shape.migratedReference, shape.source.getPageReference(shape.migratedRefKey));
      verify(fixture.log, never()).releaseOrphanedHOTLeaves(anyLong(), any(PageReference.class),
          org.mockito.ArgumentMatchers.<List<PageReference>>any(), anyInt());
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
      closeReachableLeaves(shape.rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      final HOTLeafPage transferred = registeredSourceLeaf.get();
      if (transferred != null && !transferred.isClosed()) {
        transferred.close();
      }
      final HOTLeafPage unregistered = failedMigratedLeaf.get();
      if (unregistered != null && !unregistered.isClosed()) {
        unregistered.close();
      }
      if (!shape.source.isClosed()) {
        shape.source.close();
      }
    }
  }

  @Test
  void ordinaryParentFoldSplitRetiresTheReplacedLeafThroughThePublishedParent() {
    final HOTLeafPage source = new HOTLeafPage(30, 1, IndexType.PATH);
    assertTrue(source.put(key(0x10), largeValue(1)));
    assertTrue(source.put(key(0x20), largeValue(2)));
    assertTrue(source.put(key(0x30), largeValue(3)));
    assertTrue(source.put(key(0x40), largeValue(4)));
    final PageReference sourceReference = reference(source);
    final HOTLeafPage sibling = leaf(31, key(0x80), 9);
    final PageReference rootReference = reference(HOTIndirectPage.createSpanNode(32, 1, 0, 0x8000_0000_0000_0000L,
        new int[] {0, 1}, new PageReference[] {sourceReference, reference(sibling)}, 1));
    final WriterFixture fixture = writerFixture(rootReference);

    try {
      fixture.writer.insert(key(0x50), largeValue(5));

      verify(fixture.log).releaseOrphanedHOTLeaves(anyLong(), same(rootReference),
          argThat(refs -> refs.size() == 1 && refs.get(0).getPage() == source),
          eq(TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT));
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
      assertTrue(HOTMalformedSubtreeDetector.detect(rootReference, fixture::resolve).isEmpty());
    } finally {
      closeReachableLeaves(rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!source.isClosed()) {
        source.close();
      }
      if (!sibling.isClosed()) {
        sibling.close();
      }
    }
  }

  @Test
  void notFullOffPathOverflowRetiresTheReplacedLeafWithoutFallingThrough() {
    final HOTLeafPage source = largeLeaf(40, new int[] {0x00, 0x10, 0x40, 0x50});
    final PageReference sourceReference = reference(source);
    final HOTLeafPage highZero = leaf(41, key(0x80), 8);
    final HOTLeafPage highOne = leaf(42, key(0xC0), 9);
    final PageReference rootReference = reference(HOTIndirectPage.createSpanNode(43, 1, 0, 0xC000_0000_0000_0000L,
        new int[] {0, 2, 3}, new PageReference[] {sourceReference, reference(highZero), reference(highOne)}, 1));
    final WriterFixture fixture = writerFixture(rootReference);
    final long handledBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get();

    try {
      fixture.writer.insert(key(0x20), largeValue(5));

      assertEquals(handledBefore + 1, AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get());
      verify(fixture.log).releaseOrphanedHOTLeaves(anyLong(), same(rootReference),
          argThat(refs -> refs.size() == 1 && refs.get(0).getPage() == source),
          eq(TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT));
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
      assertTrue(HOTMalformedSubtreeDetector.detect(rootReference, fixture::resolve).isEmpty());
    } finally {
      closeReachableLeaves(rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!source.isClosed()) {
        source.close();
      }
      if (!highZero.isClosed()) {
        highZero.close();
      }
      if (!highOne.isClosed()) {
        highOne.close();
      }
    }
  }

  @Test
  void fullParentOffPathOverflowRetiresTheReplacedLeafAfterCapacityCascade() {
    final HOTLeafPage source = largeLeaf(50, new int[] {0x00, 0x01, 0x04, 0x05});
    final PageReference sourceReference = reference(source);
    final int[] partials = new int[HOTIndirectPage.MAX_NODE_ENTRIES];
    final PageReference[] children = new PageReference[HOTIndirectPage.MAX_NODE_ENTRIES];
    partials[0] = 0;
    children[0] = sourceReference;
    for (int slot = 1; slot < HOTIndirectPage.MAX_NODE_ENTRIES; slot++) {
      final int partial = slot + 1; // leave sparse combination 1 free for the split's beta=1 half
      partials[slot] = partial;
      children[slot] = reference(leaf(50L + slot, key(partial << 2), partial));
    }
    final PageReference rootReference =
        reference(HOTIndirectPage.createMultiNode(100, 1, 0, 0xFC00_0000_0000_0000L, partials, children, 1));
    final WriterFixture fixture = writerFixture(rootReference);
    final long handledBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get();

    try {
      fixture.writer.insert(key(0x02), largeValue(5));

      assertEquals(handledBefore + 1, AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_OK.get());
      verify(fixture.log).releaseOrphanedHOTLeaves(anyLong(), same(rootReference),
          argThat(refs -> refs.size() == 1 && refs.get(0).getPage() == source),
          eq(TransactionIntentLog.RELEASE_SITE_LEAF_SPLIT));
      verify(fixture.storageEngineWriter, never()).markTransactionRollbackOnly(any(Throwable.class));
      assertTrue(HOTMalformedSubtreeDetector.detect(rootReference, fixture::resolve).isEmpty());
    } finally {
      closeReachableLeaves(rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!source.isClosed()) {
        source.close();
      }
      for (int slot = 1; slot < children.length; slot++) {
        final Page page = children[slot].getPage();
        if (page instanceof HOTLeafPage leaf && !leaf.isClosed()) {
          leaf.close();
        }
      }
    }
  }

  @Test
  void postPublicationFailurePoisonsAndClosesBothReplacementRoots() {
    final MigrationShape shape = migrationShape();
    final WriterFixture fixture = writerFixture(shape.rootReference);
    final IllegalStateException sentinel = new IllegalStateException("injected after two-leaf publication");
    final AtomicReference<HOTIndirectPage> published = new AtomicReference<>();
    final AtomicReference<Page> rebuiltSource = new AtomicReference<>();
    final AtomicReference<Page> migratedChild = new AtomicReference<>();
    final long migrationBefore = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
      final HOTIndirectPage candidate = (HOTIndirectPage) shape.rootReference.getPage();
      published.set(candidate);
      markRetainedMigrationChildrenDurable(candidate);
      rebuiltSource.set(candidate.getChildReference(3).getPage());
      migratedChild.set(candidate.getChildReference(4).getPage());
      throw sentinel;
    });

    try {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> fixture.writer.insert(key(0xC0), value(5)));

      assertSame(sentinel, failure, "the publication boundary must preserve the original failure");
      verify(fixture.storageEngineWriter).markTransactionRollbackOnly(same(sentinel));
      assertEquals(migrationBefore, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get(),
          "a failed publication must not count as a completed migration");
      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get());
      verify(fixture.log, never()).releaseOrphanedHOTLeaves(anyLong(), any(PageReference.class),
          org.mockito.ArgumentMatchers.<List<PageReference>>any(), anyInt());

      final HOTIndirectPage candidate = published.get();
      assertTrue(candidate != null, "the fault must fire after the sole publication boundary");
      assertTrue(rebuiltSource.get().isClosed(), "the rebuilt source leaf must be retired when publication fails");
      assertTrue(migratedChild.get().isClosed(), "the migrated child leaf must be retired when publication fails");
      assertFalse(shape.source.isClosed(), "the original source is still owned by the pre-transaction graph");
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
      closeReachableLeaves(shape.rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!shape.source.isClosed()) {
        shape.source.close();
      }
    }
  }

  @Test
  void registrationIllegalArgumentAfterPublicationNeverEntersDirectionOneFallback() {
    final MigrationShape shape = migrationShape();
    final AtomicBoolean publicationReached = new AtomicBoolean();
    final IllegalArgumentException sentinel = new IllegalArgumentException("injected registration failure");
    final WriterFixture fixture = writerFixture(shape.rootReference, publicationReached, sentinel);
    final long subInsertBefore = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get();
    final long directionOneFallbackBefore = AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get();
    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
    final long migrationBefore = AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get();
    AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(() -> {
      markRetainedMigrationChildrenDurable((HOTIndirectPage) shape.rootReference.getPage());
      publicationReached.set(true);
    });

    try {
      final IllegalArgumentException failure =
          assertThrows(IllegalArgumentException.class, () -> fixture.writer.insert(key(0xC0), value(5)));

      assertSame(sentinel, failure, "the post-publication registration failure must escape unchanged");
      assertTrue(publicationReached.get(), "the fault must occur beyond the two-leaf publication boundary");
      verify(fixture.storageEngineWriter, atLeastOnce()).markTransactionRollbackOnly(same(sentinel));
      assertEquals(subInsertBefore, AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get(),
          "a post-publication IllegalArgumentException is not a C2 collision");
      assertEquals(directionOneFallbackBefore, AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get(),
          "the poisoned mutation must never enter Direction 1 fallback");
      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          "the injected failure must occur before post-publication invariant validation");
      assertEquals(migrationBefore, AbstractHOTIndexWriter.STRAND_TWO_LEAF_MIGRATE.get(),
          "a failed publication must not count as a completed migration");
      verify(fixture.log, never()).releaseOrphanedHOTLeaves(anyLong(), any(PageReference.class),
          org.mockito.ArgumentMatchers.<List<PageReference>>any(), anyInt());
    } finally {
      AbstractHOTIndexWriter.setTwoLeafMigrationAfterPublicationTestHook(null);
      closeReachableLeaves(shape.rootReference, fixture, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
      if (!shape.source.isClosed()) {
        shape.source.close();
      }
    }
  }

  private static MigrationShape migrationShape() {
    final HOTLeafPage slotZero = leaf(10, key(0x00), 1);
    final HOTLeafPage slotOne = leaf(11, key(0x20), 2);
    final HOTLeafPage descended = leaf(12, key(0x80), 3);
    final HOTLeafPage source = new HOTLeafPage(13, 1, IndexType.PATH);
    assertTrue(source.put(key(0xA0), value(4)));
    assertTrue(source.put(key(0xE0), value(6)));
    final PageReference rootReference =
        reference(HOTIndirectPage.createSpanNode(14, 1, 0, 0xA000_0000_0000_0000L, new int[] {0, 1, 2, 3},
            new PageReference[] {reference(slotZero), reference(slotOne), reference(descended), reference(source)}, 1));
    return new MigrationShape(rootReference, source);
  }

  private static ProjectionMigrationShape projectionMigrationShapeWithTombstones() {
    final HOTLeafPage slotZero = leaf(110, projectionKey(0x00), 1, IndexType.PROJECTION);
    final HOTLeafPage slotOne = leaf(111, projectionKey(0x20), 2, IndexType.PROJECTION);
    final HOTLeafPage descended = leaf(112, projectionKey(0x80), 3, IndexType.PROJECTION);
    final HOTLeafPage source = new HOTLeafPage(113, 1, IndexType.PROJECTION);
    final byte[] remainingKey = projectionKey(0xA0);
    final byte[] migratedKey = projectionKey(0xE0);
    assertTrue(source.put(remainingKey, new byte[0]));
    assertTrue(source.put(migratedKey, new byte[0]));
    final PageReference remainingReference = durableReference(9_001L);
    final PageReference migratedReference = durableReference(9_002L);
    final long remainingOwner = PathKeySerializer.INSTANCE.deserialize(remainingKey, 0, remainingKey.length);
    final long migratedOwner = PathKeySerializer.INSTANCE.deserialize(migratedKey, 0, migratedKey.length);
    final long remainingRefKey = HOTLeafPage.overflowPageRefKey(remainingOwner, 1);
    final long migratedRefKey = HOTLeafPage.overflowPageRefKey(migratedOwner, 2);
    source.setPageReference(remainingRefKey, remainingReference);
    source.setPageReference(migratedRefKey, migratedReference);
    final PageReference rootReference =
        reference(HOTIndirectPage.createSpanNode(114, 1, 7, 0xA000_0000_0000_0000L, new int[] {0, 1, 2, 3},
            new PageReference[] {reference(slotZero), reference(slotOne), reference(descended), reference(source)}, 1));
    return new ProjectionMigrationShape(rootReference, source, remainingRefKey, remainingReference, migratedRefKey,
        migratedReference);
  }

  private static WriterFixture writerFixture(final PageReference rootReference) {
    return writerFixture(rootReference, IndexType.PATH, null, null);
  }

  private static WriterFixture writerFixture(final PageReference rootReference, final IndexType indexType) {
    return writerFixture(rootReference, indexType, null, null);
  }

  private static WriterFixture writerFixture(final PageReference rootReference, final AtomicBoolean failRegistration,
      final RuntimeException registrationFailure) {
    return writerFixture(rootReference, IndexType.PATH, failRegistration, registrationFailure);
  }

  private static WriterFixture writerFixture(final PageReference rootReference, final IndexType indexType,
      final AtomicBoolean failRegistration, final RuntimeException registrationFailure) {
    return writerFixture(rootReference, indexType, failRegistration, registrationFailure, 1);
  }

  private static WriterFixture writerFixture(final PageReference rootReference, final IndexType indexType,
      final AtomicBoolean failRegistration, final RuntimeException registrationFailure,
      final int failureOrdinalAfterGate) {
    if (failureOrdinalAfterGate <= 0) {
      throw new IllegalArgumentException("failureOrdinalAfterGate must be positive");
    }
    final StorageEngineWriter storageEngineWriter = mock(StorageEngineWriter.class);
    final TransactionIntentLog log = mock(TransactionIntentLog.class);
    final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
    final PathPage pathPage = mock(PathPage.class);
    final ProjectionIndexPage projectionIndexPage = mock(ProjectionIndexPage.class);
    final AtomicLong pageKeys = new AtomicLong(1_000);
    final AtomicInteger logKeys = new AtomicInteger(1);
    final AtomicInteger gatedRegistrationAttempts = new AtomicInteger();
    final Map<PageReference, PageContainer> logged = new IdentityHashMap<>();

    when(storageEngineWriter.getLog()).thenReturn(log);
    when(storageEngineWriter.getRevisionNumber()).thenReturn(2);
    when(storageEngineWriter.getActualRevisionRootPage()).thenReturn(revisionRootPage);
    when(storageEngineWriter.getPathPage(revisionRootPage)).thenReturn(pathPage);
    when(storageEngineWriter.getProjectionIndexPage(revisionRootPage)).thenReturn(projectionIndexPage);
    when(storageEngineWriter.<PathPage>prepareSecondaryIndexPage(IndexType.PATH)).thenReturn(pathPage);
    when(storageEngineWriter.<ProjectionIndexPage>prepareSecondaryIndexPage(IndexType.PROJECTION)).thenReturn(
        projectionIndexPage);
    when(pathPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(invocation -> pageKeys.getAndIncrement());
    when(projectionIndexPage.incrementAndGetMaxHotPageKey(0)).thenAnswer(invocation -> pageKeys.getAndIncrement());
    when(log.get(any(PageReference.class))).thenAnswer(invocation -> {
      final PageReference reference = invocation.getArgument(0);
      final PageContainer registered = logged.get(reference);
      if (registered != null) {
        return registered;
      }
      final Page resident = reference.getPage();
      return resident instanceof HOTLeafPage leaf
          ? PageContainer.getInstance(leaf, leaf)
          : null;
    });
    doAnswer(invocation -> {
      if (failRegistration != null && failRegistration.get()
          && gatedRegistrationAttempts.incrementAndGet() == failureOrdinalAfterGate) {
        throw registrationFailure;
      }
      final PageReference reference = invocation.getArgument(0);
      final PageContainer container = invocation.getArgument(1);
      logged.put(reference, container);
      if (failRegistration != null && failRegistration.get() && reference.getLogKey() < 0) {
        reference.setLogKey(logKeys.getAndIncrement());
      }
      return null;
    }).when(log).put(any(PageReference.class), any(PageContainer.class));

    final TestIndexWriter writer = new TestIndexWriter(storageEngineWriter, indexType);
    writer.installRoot(rootReference);
    return new WriterFixture(storageEngineWriter, log, writer, logged);
  }

  private static void collectEntries(final Page page, final WriterFixture fixture,
      final Map<Integer, Integer> entries) {
    if (page instanceof HOTLeafPage leaf) {
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        final byte[] key = leaf.getKey(i);
        final byte[] value = leaf.getValue(i);
        assertEquals(1, key.length);
        assertEquals(1, value.length);
        final Integer previous = entries.put(Byte.toUnsignedInt(key[0]), Byte.toUnsignedInt(value[0]));
        assertEquals(null, previous, "a key must have exactly one physical owner");
      }
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      collectEntries(fixture.resolve(indirect.getChildReference(i)), fixture, entries);
    }
  }

  private static byte[] storedValue(final PageReference rootReference, final byte[] key, final WriterFixture fixture) {
    final HOTLeafPage leaf = routedLeaf(rootReference, key, fixture);
    final int index = leaf.findEntry(key);
    assertTrue(index >= 0, "the migrated leaf must contain the requested key");
    final long valueRef = leaf.valueRef(index);
    final int valueLength = HOTLeafPage.refLength(valueRef);
    assertTrue(valueLength >= 0, "the migrated value must be physically readable");
    final byte[] value = new byte[valueLength];
    if (valueLength > 0) {
      leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
    }
    return value;
  }

  private static HOTLeafPage routedLeaf(final PageReference rootReference, final byte[] key,
      final WriterFixture fixture) {
    Page page = fixture.resolve(rootReference);
    while (page instanceof HOTIndirectPage indirect) {
      final int childIndex = indirect.findChildIndex(key);
      assertTrue(childIndex >= 0, "the migrated trie must route the requested key");
      page = fixture.resolve(indirect.getChildReference(childIndex));
    }
    assertTrue(page instanceof HOTLeafPage, "the migrated route must end at a leaf");
    return (HOTLeafPage) page;
  }

  private static void assertSubtreeLeavesClosed(final Page page) {
    if (page instanceof HOTLeafPage leaf) {
      assertTrue(leaf.isClosed(), "every locally owned replacement leaf must be closed");
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      final PageReference childRef = indirect.getChildReference(i);
      if (childRef != null && childRef.getPage() != null) {
        assertSubtreeLeavesClosed(childRef.getPage());
      }
    }
  }

  /**
   * The focused fixture keeps the original graph resident without a real TIL/durable reader. Mark the
   * three retained children with the durable identities they have in production so failure cleanup
   * can distinguish them from the two locally owned replacement roots at slots 3 and 4.
   */
  private static void markRetainedMigrationChildrenDurable(final HOTIndirectPage candidate) {
    for (int slot = 0; slot < 3; slot++) {
      final PageReference retained = candidate.getChildReference(slot);
      if (retained.getKey() < 0 && retained.getLogKey() < 0) {
        final Page page = retained.getPage();
        if (page instanceof HOTLeafPage leaf) {
          retained.setKey(leaf.getPageKey());
        } else if (page instanceof HOTIndirectPage indirect) {
          retained.setKey(indirect.getPageKey());
        } else {
          throw new IllegalStateException("retained migration child " + slot + " is not a HOT page");
        }
      }
    }
  }

  private static void closeReachableLeaves(final PageReference reference, final WriterFixture fixture,
      final Set<Page> visited) {
    final Page page = fixture.resolve(reference);
    if (page == null || !visited.add(page)) {
      return;
    }
    if (page instanceof HOTLeafPage leaf) {
      if (!leaf.isClosed()) {
        leaf.close();
      }
      return;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      closeReachableLeaves(indirect.getChildReference(i), fixture, visited);
    }
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[] key, final int value) {
    return leaf(pageKey, key, value, IndexType.PATH);
  }

  private static HOTLeafPage leaf(final long pageKey, final byte[] key, final int value, final IndexType indexType) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, indexType);
    assertTrue(leaf.put(key, value(value)));
    return leaf;
  }

  private static HOTLeafPage largeLeaf(final long pageKey, final int[] unsignedKeys) {
    final HOTLeafPage leaf = new HOTLeafPage(pageKey, 1, IndexType.PATH);
    for (int i = 0; i < unsignedKeys.length; i++) {
      assertTrue(leaf.put(key(unsignedKeys[i]), largeValue(i + 1)));
    }
    return leaf;
  }

  private static PageReference reference(final Page page) {
    final PageReference reference = new PageReference();
    reference.setPage(page);
    return reference;
  }

  private static PageReference durableReference(final long key) {
    final PageReference reference = new PageReference();
    reference.setKey(key);
    return reference;
  }

  private static byte[] key(final int unsignedByte) {
    return new byte[] {(byte) unsignedByte};
  }

  private static byte[] projectionKey(final int unsignedFirstByte) {
    final byte[] key = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(unsignedFirstByte, key, 0);
    return key;
  }

  private static byte[] value(final int unsignedByte) {
    return new byte[] {(byte) unsignedByte};
  }

  private static byte[] largeValue(final int marker) {
    final byte[] value = new byte[14_000];
    java.util.Arrays.fill(value, (byte) marker);
    return value;
  }

  private record WriterFixture(StorageEngineWriter storageEngineWriter, TransactionIntentLog log,
      TestIndexWriter writer, Map<PageReference, PageContainer> logged) {
    private Page resolve(final PageReference reference) {
      final PageContainer registered = logged.get(reference);
      if (registered != null) {
        final Page modified = registered.getModified();
        if (modified != null) {
          return modified;
        }
        return registered.getComplete();
      }
      return reference.getPage();
    }
  }

  private record MigrationShape(PageReference rootReference, HOTLeafPage source) {
  }

  private record ProjectionMigrationShape(PageReference rootReference, HOTLeafPage source, long remainingRefKey,
      PageReference remainingReference, long migratedRefKey, PageReference migratedReference) {
  }

  private static final class TestIndexWriter extends AbstractHOTIndexWriter<byte[]> {
    private byte[] keyBuffer = new byte[8];

    private TestIndexWriter(final StorageEngineWriter storageEngineWriter, final IndexType indexType) {
      super(storageEngineWriter, indexType, 0);
    }

    private void installRoot(final PageReference root) {
      rootReference = root;
    }

    private void insert(final byte[] key, final byte[] value) {
      doIndex(key, key.length, value, value.length);
    }

    @Override
    protected byte[] getKeyBuffer() {
      return keyBuffer;
    }

    @Override
    protected void setKeyBuffer(final byte[] newBuffer) {
      keyBuffer = newBuffer;
    }

    @Override
    protected int serializeKey(final byte[] key, final byte[] buffer, final int offset) {
      System.arraycopy(key, 0, buffer, offset, key.length);
      return key.length;
    }

    @Override
    protected void prepareIndexPage() {
      // This focused fixture installs an already materialized root directly.
    }
  }
}
