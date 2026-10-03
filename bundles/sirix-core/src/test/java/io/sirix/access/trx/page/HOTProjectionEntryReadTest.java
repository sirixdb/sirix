/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader.LowerBoundResult;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.access.trx.RevisionEpochTracker.Ticket;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.HOTReadIntent;
import io.sirix.cache.BufferManager;
import io.sirix.cache.Cache;
import io.sirix.cache.EmptyCache;
import io.sirix.cache.HOTMiniPageCache;
import io.sirix.cache.ShardedPageCache;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.page.HOTLeafEntry;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class HOTProjectionEntryReadTest {

  @BeforeAll
  static void requireMergeDiagnostics() {
    assertTrue(VersioningType.hotMergeDiagEnabled(),
        "Run with -Dsirix.hot.mergeDiag=true (the gradle test configuration sets it).");
  }

  private static final byte[] KEY = {7, 11};
  private static final byte[] VALUE = {13, 17};
  private static final long SIDE_KEY = 42;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedMiniDiscardReleasesThePromotedCompleteLeafGuard(final boolean packedLimit) {
    try (Fixture fixture = new Fixture(true)) {
      for (int revision = 5; revision >= 1; revision--) {
        fixture.add(revision);
      }
      final HOTLeafPage base = fixture.images.get(1L);
      base.setCompleteDump(true);
      final byte[] value = packedLimit
          ? new byte[HOTMiniPage.MAX_DATA_BYTES / 2]
          : VALUE;
      final int previousReads = packedLimit
          ? 1
          : HOTMiniPageCache.POINT_PROMOTION_DISTINCT_KEYS - 1;
      for (int i = 0; i <= previousReads; i++) {
        assertTrue(base.put(new byte[] {7, (byte) i}, value));
      }
      for (int i = 0; i < previousReads; i++) {
        assertArrayEquals(value,
            fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, (byte) i}, SIDE_KEY).value());
      }
      assertEquals(0, fixture.completeCache.size());
      final HOTMiniPageCache failingMiniCache = spy(fixture.miniCache);
      final Error failure = new OutOfMemoryError("mini-page removal bookkeeping");
      doAnswer(_ -> {
        final HOTLeafPage complete = assertInstanceOf(HOTLeafPage.class, fixture.chain.getPage());
        assertEquals(1, complete.getGuardCount());
        throw failure;
      }).when(failingMiniCache).discard(any(PageReference.class));
      when(fixture.buffers.getHOTMiniPageCache()).thenReturn(failingMiniCache);
      assertSame(failure, assertThrows(Error.class,
          () -> fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, (byte) previousReads}, SIDE_KEY)));
      final HOTLeafPage complete = assertInstanceOf(HOTLeafPage.class, fixture.chain.getPage());
      assertEquals(0, complete.getGuardCount());
      assertEquals(1, fixture.completeCache.size());
      assertArrayEquals(value,
          fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, 0}, SIDE_KEY).value());
      assertEquals(0, complete.getGuardCount());
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void fourthDistinctPointPromotesOnceAndReusesEveryCachedRawImage(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(true, versioning)) {
      for (int revision = 5; revision >= 1; revision--)
        fixture.add(revision);
      final HOTLeafPage head = fixture.images.get(5L);
      final HOTLeafPage base = fixture.images.get(1L);
      base.setCompleteDump(true);
      for (int i = 0; i < 8; i++)
        base.put(new byte[] {7, (byte) i}, new byte[] {(byte) (10 + i)});
      head.put(new byte[] {7, 0}, VALUE);
      head.put(new byte[] {7, 6}, new byte[0]);
      final PageReference newestSide = new PageReference().setKey(900);
      newestSide.setHash(0L);
      head.setPageReference(SIDE_KEY, newestSide);
      base.setPageReference(SIDE_KEY, new PageReference().setKey(800));
      final long merges = VersioningType.multiFragmentMerges();
      for (int i = 0; i < 3; i++) {
        assertArrayEquals(i == 0
            ? VALUE
            : new byte[] {(byte) (10 + i)},
            fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, (byte) i}, SIDE_KEY).value());
      }
      assertEquals(merges, VersioningType.multiFragmentMerges());
      assertEquals(List.of(5L, 4L, 3L, 2L, 1L), fixture.reads);
      final HOTLeafEntry fourth = fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, 3}, SIDE_KEY);
      assertArrayEquals(new byte[] {13}, fourth.value());
      assertEquals(900, fourth.sideReference().getKey());
      assertTrue(fourth.sideReference().hasHash());
      assertEquals(0L, fourth.sideReference().getHashAsLong());
      assertEquals(merges + 1, VersioningType.multiFragmentMerges());
      assertEquals(List.of(5L, 4L, 3L, 2L, 1L), fixture.reads,
          "promotion reuses the head and every older raw image without another disk read");
      assertEquals(0, fixture.miniCache.getCurrentWeightBytes());
      for (int i = 0; i < 8; i++) {
        final byte[] expected = i == 0
            ? VALUE
            : i == 6
                ? new byte[0]
                : new byte[] {(byte) (10 + i)};
        assertArrayEquals(expected,
            fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, (byte) i}, SIDE_KEY).value());
      }
      assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {8}, SIDE_KEY));
      assertEquals(merges + 1, VersioningType.multiFragmentMerges(), "later distinct keys use the complete image");
      assertFalse(head.isClosed(), "complete adoption must not retire a cache-owned raw source");
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void detachedPointThenRangeThenPointReusesOneReaderWithoutStaleTraversalOrGuards(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(false, versioning); HOTTrieReader trie = new HOTTrieReader(fixture.storage)) {
      final byte[] rightKey = {(byte) 0x87, 11};
      final HOTLeafPage left = fixture.add(5);
      final HOTLeafPage right = fixture.add(4);
      left.put(KEY, VALUE);
      right.put(rightKey, new byte[] {23});
      final PageReference leftRef = new PageReference().setKey(5);
      final PageReference rightRef = new PageReference().setKey(4);
      leftRef.setPage(left);
      rightRef.setPage(right);
      final PageReference root = new PageReference().setKey(6);
      root.setPage(HOTIndirectPage.createBiNode(6, 5, 0, leftRef, rightRef));

      assertArrayEquals(VALUE, trie.readProjectionEntry(root, KEY, SIDE_KEY).value());
      assertEquals(0, trie.getPathDepth(), "a detached point does not position a range");
      trie.endWalk();
      fixture.assertReleased();

      final LowerBoundResult first = trie.lowerBound(root, KEY);
      assertSame(left, first.leaf);
      assertEquals(0, first.indexInLeaf);
      assertEquals(1, trie.getPathDepth(), "range descent retains its indirect parent");
      assertSame(right, trie.advanceToNextLeaf());
      assertTrue(trie.validateCurrentLeaf());
      assertNull(trie.advanceToNextLeaf());
      trie.endWalk();
      fixture.assertReleased();

      assertArrayEquals(new byte[] {23}, trie.readProjectionEntry(root, rightKey, SIDE_KEY).value());
      assertEquals(0, trie.getPathDepth());
      trie.endWalk();
      fixture.assertReleased();
      assertArrayEquals(VALUE, trie.readProjectionEntry(root, KEY, SIDE_KEY).value());
      trie.endWalk();
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void completePointImagePublishesAFullyBuiltLeafWithoutPartialAdmissions(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(true, versioning)) {
      fixture.chain.setPageFragments(new ArrayList<>());
      final HOTLeafPage complete = fixture.add(5);
      complete.setCompleteDump(true);
      complete.put(KEY, VALUE);
      final byte[] otherKey = {7, 12};
      complete.put(otherKey, new byte[0]);
      final long merges = VersioningType.multiFragmentMerges();
      final long older = VersioningType.pointFragmentsWalked();
      final HOTTrieReader trie = new HOTTrieReader(fixture.storage);
      try {
        assertArrayEquals(VALUE, trie.readProjectionEntry(fixture.chain, KEY, SIDE_KEY).value());
        assertArrayEquals(new byte[0], trie.readProjectionEntry(fixture.chain, otherKey, SIDE_KEY).value());
        assertNull(trie.readProjectionEntry(fixture.chain, new byte[] {7, 13}, SIDE_KEY));
        assertSame(complete, fixture.chain.getPage());
        assertEquals(0, fixture.miniCache.getCurrentWeightBytes(), "a complete image spends no partial admission");
        assertEquals(0, assertInstanceOf(ShardedPageCache.class, fixture.fragmentCache).size());
        assertEquals(merges, VersioningType.multiFragmentMerges());
        assertEquals(older, VersioningType.pointFragmentsWalked());
        verify(fixture.disk, never()).readHOTLeafFragment(any(PageReference.class), any(ResourceConfiguration.class));
        verify(fixture.disk).read(any(PageReference.class), any(ResourceConfiguration.class));
        assertEquals(List.of(5L), fixture.reads, "all later slots and ranges reuse the complete image");
        trie.endWalk();
        assertSame(complete, trie.lowerBound(fixture.chain, KEY).leaf);
      } finally {
        trie.endWalk();
      }
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void outOfBoundsRawImagesRemainCachedAcrossPresentAndAbsentLookups(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(true, versioning)) {
      fixture.add(5);
      fixture.add(4).put(new byte[] {99}, VALUE);
      final HOTLeafPage complete = fixture.add(3);
      complete.put(KEY, VALUE);
      complete.setCompleteDump(true);

      assertArrayEquals(VALUE, fixture.read().value());
      assertEquals(List.of(5L, 4L, 3L), fixture.reads);
      assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, 12}, SIDE_KEY));
      assertArrayEquals(VALUE, fixture.read().value());
      assertEquals(List.of(5L, 4L, 3L), fixture.reads,
          "fragments outside the queried key bounds must not be read again");
      assertEquals(3, assertInstanceOf(ShardedPageCache.class, fixture.fragmentCache).size());
      assertNull(fixture.chain.getPage(), "raw cached images never become complete-leaf swizzles");
      fixture.assertReleased();
    }
  }

  @Test
  void cachedCompleteReferenceIsGuardedThroughEvictionAndCopy() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      fixture.completeCache.put(new PageReference().setKey(5)
                                                   .setDatabaseId(fixture.storage.getDatabaseId())
                                                   .setResourceId(fixture.storage.getResourceId()),
          leaf);
      doAnswer(invocation -> {
        fixture.completeCache.clear();
        assertFalse(leaf.isClosed());
        assertEquals(1, leaf.getGuardCount());
        return invocation.callRealMethod();
      }).when(leaf).copyStoredValue(anyInt());
      assertArrayEquals(VALUE, fixture.read().value());
      assertTrue(fixture.reads.isEmpty());
      assertTrue(leaf.isClosed());
      fixture.assertReleased();
    }
  }

  @Test
  void wrongDirectLeafTypeFailsAndReleasesItsGuard() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      when(leaf.getIndexType()).thenReturn(IndexType.PATH);
      fixture.chain.setPage(leaf);
      assertThrows(IllegalArgumentException.class, fixture::read);
      fixture.assertReleased();
    }
  }

  @Test
  void wrongCachedCompleteLeafTypeFailsAndReleasesItsGuard() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      when(leaf.getIndexType()).thenReturn(IndexType.PATH);
      fixture.completeCache.put(new PageReference().setKey(5)
                                                   .setDatabaseId(fixture.storage.getDatabaseId())
                                                   .setResourceId(fixture.storage.getResourceId()),
          leaf);
      assertThrows(IllegalArgumentException.class, fixture::read);
      assertTrue(fixture.reads.isEmpty());
      fixture.assertReleased();
    }
  }

  @Test
  void trieRejectsAWrongSwizzledLeafTypeAndReleasesItsGuard() {
    try (Fixture fixture = new Fixture(false); HOTTrieReader trie = new HOTTrieReader(fixture.storage)) {
      final HOTLeafPage leaf = fixture.add(5);
      when(leaf.getIndexType()).thenReturn(IndexType.PATH);
      fixture.chain.setPage(leaf);
      assertThrows(IllegalArgumentException.class, () -> trie.readProjectionEntry(fixture.chain, KEY, SIDE_KEY));
      trie.endWalk();
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void directCompleteReferenceIsReadUnderGuardWithoutConsultingCaches(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(false, versioning)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      final PageReference sideReference = new PageReference().setKey(900);
      leaf.setPageReference(SIDE_KEY, sideReference);
      fixture.chain.setPage(leaf);
      doAnswer(invocation -> {
        assertEquals(1, leaf.getGuardCount(), "direct-reference bytes require a live guard");
        return invocation.callRealMethod();
      }).when(leaf).copyStoredValue(anyInt());

      final HOTLeafEntry entry = fixture.read();
      assertArrayEquals(VALUE, entry.value());
      assertSame(sideReference, entry.sideReference());
      assertSame(leaf, fixture.chain.getPage());
      assertTrue(fixture.reads.isEmpty());
      verify(fixture.buffers, never()).getHOTLeafPageCache();
      verify(fixture.buffers, never()).getHOTLeafFragmentCache();
      fixture.assertReleased();
    }
  }

  @Test
  void directCompleteReferenceSurvivesRetirementDuringCopy() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      fixture.chain.setPage(leaf);
      doAnswer(invocation -> {
        leaf.markOrphaned();
        assertFalse(leaf.isClosed());
        assertEquals(1, leaf.getGuardCount());
        return invocation.callRealMethod();
      }).when(leaf).copyStoredValue(anyInt());
      assertArrayEquals(VALUE, fixture.read().value());
      assertTrue(leaf.isClosed(), "last reader releases the retired direct reference");
      fixture.assertReleased();
    }
  }

  @Test
  void closedAndRetiredDirectReferencesFallThroughWithoutReadingTheirBytes() {
    for (final boolean retainedGuard : new boolean[] {false, true}) {
      try (Fixture fixture = new Fixture(false)) {
        final HOTLeafPage stale = fixture.add(5);
        stale.put(KEY, new byte[] {99});
        fixture.chain.setPage(stale);
        if (retainedGuard)
          assertTrue(stale.acquireGuard());
        stale.markOrphaned();
        assertEquals(!retainedGuard, stale.isClosed());
        fixture.add(5).put(KEY, VALUE);
        clearInvocations(stale);
        try {
          assertArrayEquals(VALUE, fixture.read().value());
          assertEquals(List.of(5L), fixture.reads);
          assertNull(fixture.chain.getPage(), "stale complete swizzle is cleared; raw image is not swizzled");
          verify(stale, never()).findEntry(any(byte[].class));
          verify(stale, never()).copyStoredValue(anyInt());
          assertEquals(retainedGuard
              ? 1
              : 0, stale.getGuardCount());
        } finally {
          if (retainedGuard)
            stale.releaseGuard();
        }
        fixture.assertReleased();
      }
    }
  }

  @Test
  void failedDirectValueCopyReleasesItsGuard() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      fixture.chain.setPage(leaf);
      final IllegalStateException failure = new IllegalStateException("injected direct value corruption");
      doAnswer(_ -> {
        throw failure;
      }).when(leaf).copyStoredValue(anyInt());
      assertSame(failure, assertThrows(IllegalStateException.class, fixture::read));
      assertTrue(fixture.reads.isEmpty());
      fixture.assertReleased();
    }
  }

  @Test
  void absentDirectValueDoesNotWalkOlderFragments() {
    try (Fixture fixture = new Fixture(false)) {
      fixture.chain.setPage(fixture.add(5));
      fixture.add(4).put(KEY, VALUE);
      assertNull(fixture.read());
      assertTrue(fixture.reads.isEmpty());
      fixture.assertReleased();
    }
  }

  @Test
  void newestValueNeedsOneFragmentAndNeverPublishesAPartialLeaf() {
    try (Fixture fixture = new Fixture(false)) {
      fixture.add(5).put(KEY, VALUE);
      final long before = VersioningType.pointLeafReads();
      final long walked = VersioningType.pointFragmentsWalked();
      assertArrayEquals(VALUE, fixture.read().value());
      assertEquals(List.of(5L), fixture.reads);
      assertEquals(before + 1, VersioningType.pointLeafReads());
      assertEquals(walked, VersioningType.pointFragmentsWalked());
      assertNull(fixture.chain.getPage(), "a raw fragment cannot become a complete-leaf swizzle");
      assertEquals(0, fixture.completeCache.size());
      fixture.assertReleased();
    }
  }

  @Test
  void newestTombstoneShadowsEveryOlderValue() {
    try (Fixture fixture = new Fixture(false)) {
      fixture.add(5).put(KEY, new byte[0]);
      fixture.add(4).put(KEY, VALUE);
      assertEquals(0, fixture.read().value().length);
      assertEquals(List.of(5L), fixture.reads);
      fixture.assertReleased();
    }
  }

  @Test
  void olderValueUsesNewestSideMapAndStopsBeforeUnneededFragments() {
    try (Fixture fixture = new Fixture(false)) {
      final PageReference newestReference = new PageReference().setKey(900);
      fixture.add(5).setPageReference(SIDE_KEY, newestReference);
      final HOTLeafPage older = fixture.add(4);
      older.put(KEY, VALUE);
      older.setPageReference(SIDE_KEY, new PageReference().setKey(800));
      final HOTLeafEntry entry = fixture.read();
      assertArrayEquals(VALUE, entry.value());
      assertSame(newestReference, entry.sideReference());
      assertEquals(List.of(5L, 4L), fixture.reads);
      fixture.assertReleased();
    }
  }

  @Test
  void completeDumpBoundsAbsenceEvenWhenAnOlderValueExists() {
    try (Fixture fixture = new Fixture(false)) {
      fixture.add(5);
      fixture.add(4).setCompleteDump(true);
      fixture.add(3).put(KEY, VALUE);
      assertNull(fixture.read());
      assertEquals(List.of(5L, 4L), fixture.reads);
      fixture.assertReleased();
    }
  }

  @Test
  void chainWalkTakesOneCommittedExtentAtItsFirstMissForEveryUncachedRead() {
    try (Fixture fixture = new Fixture(false)) {
      for (int revision = 5; revision >= 2; revision--)
        fixture.add(revision);
      fixture.add(1).put(KEY, VALUE);
      assertArrayEquals(VALUE, fixture.read().value());
      assertEquals(List.of(5L, 4L, 3L, 2L, 1L), fixture.reads);
      // Five uncached images, one file-extent capture, and the same bound handed to every read.
      verify(fixture.disk, times(1)).committedDataExtent();
      verify(fixture.disk, times(5)).readHOTLeafFragment(any(PageReference.class), any(ResourceConfiguration.class),
          eq(Fixture.EXTENT));
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void cachedChainWalkNeverConsultsTheCommittedExtent(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(true, versioning, 5)) {
      for (int revision = 5; revision >= 2; revision--)
        fixture.add(revision);
      fixture.add(1).put(KEY, VALUE);
      assertArrayEquals(VALUE, fixture.read().value());
      verify(fixture.disk, times(1)).committedDataExtent();
      clearInvocations(fixture.disk);
      // A different absent key walks the same five images out of the raw cache: no extent, no disk.
      assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {7, 12}, SIDE_KEY));
      verify(fixture.disk, never()).committedDataExtent();
      verify(fixture.disk, never()).read(any(PageReference.class), any(ResourceConfiguration.class));
      fixture.assertReleased();
    }
  }

  @Test
  void absentKeyWalksTheWholeDeclaredChain() {
    try (Fixture fixture = new Fixture(false)) {
      for (int revision = 5; revision >= 1; revision--)
        fixture.add(revision);
      assertNull(fixture.read());
      assertEquals(List.of(5L, 4L, 3L, 2L, 1L), fixture.reads);
      fixture.assertReleased();
    }
  }

  @Test
  void missingOrMismatchedFragmentFailsClosedAndReleasesEveryGuard() {
    for (final boolean mismatch : new boolean[] {false, true}) {
      try (Fixture fixture = new Fixture(false)) {
        fixture.add(5);
        if (mismatch)
          fixture.images.put(4L, fixture.add(3));
        assertThrows(SirixIOException.class, fixture::read);
        fixture.assertReleased();
      }
    }
  }

  @Test
  void cachedFragmentSurvivesEvictionDuringItsValueCopy() {
    try (Fixture fixture = new Fixture(true)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      doAnswer(invocation -> {
        fixture.fragmentCache.clear();
        assertFalse(leaf.isClosed());
        assertEquals(1, leaf.getGuardCount());
        return invocation.callRealMethod();
      }).when(leaf).copyStoredValue(anyInt());
      assertArrayEquals(VALUE, fixture.read().value());
      assertTrue(leaf.isClosed());
      fixture.assertReleased();
    }
  }

  @Test
  void failedValueCopyReleasesTheOrphanedFragment() {
    try (Fixture fixture = new Fixture(false)) {
      final HOTLeafPage leaf = fixture.add(5);
      leaf.put(KEY, VALUE);
      final IllegalStateException failure = new IllegalStateException("injected value corruption");
      doAnswer(_ -> {
        throw failure;
      }).when(leaf).copyStoredValue(anyInt());
      assertSame(failure, assertThrows(IllegalStateException.class, fixture::read));
      assertTrue(leaf.isClosed());
      fixture.assertReleased();
    }
  }

  @Test
  void clearingRawAndDerivedCachesInvalidatesTheHeadAtAReusedOffset() {
    try (Fixture fixture = new Fixture(true)) {
      fixture.add(5).put(KEY, VALUE);
      assertArrayEquals(VALUE, fixture.read().value());
      fixture.fragmentCache.clear();
      // BufferManager invalidates derived mini entries after its raw-fragment sweep.
      fixture.miniCache.clear();
      final byte[] replacement = {31};
      fixture.add(5).put(KEY, replacement);
      assertArrayEquals(replacement, fixture.read().value());
      assertEquals(List.of(5L, 5L), fixture.reads);
      assertNull(fixture.chain.getPage());
      fixture.assertReleased();
    }
  }

  @Test
  void byteCapPromotesOnceWhileRepeatedSingleKeyStaysSelective() {
    try (Fixture fixture = new Fixture(true)) {
      final HOTLeafPage head = fixture.add(5);
      final byte[] largeValue = new byte[1_024];
      for (int i = 0; i < 32; i++) {
        assertTrue(head.put(new byte[] {7, (byte) i}, largeValue));
      }
      final NodeStorageEngineReader reader = spy(fixture.storage);
      final List<HOTLeafPage> promoted = new ArrayList<>();
      doAnswer(_ -> {
        final HOTLeafPage complete = head.copy();
        assertTrue(complete.acquireGuard());
        complete.markOrphaned();
        promoted.add(complete);
        return complete;
      }).when(reader).loadHOTPageAndGuard(fixture.chain);
      for (int i = 0; i < 100; i++) {
        assertArrayEquals(largeValue,
            reader.readHOTProjectionEntry(fixture.chain, new byte[] {7, 0}, SIDE_KEY).value());
      }
      assertTrue(promoted.isEmpty(), "repeating one key must not trigger full reconstruction");
      for (int i = 1; i < 32; i++) {
        assertArrayEquals(largeValue,
            reader.readHOTProjectionEntry(fixture.chain, new byte[] {7, (byte) i}, SIDE_KEY).value());
      }
      verify(reader, times(1)).loadHOTPageAndGuard(fixture.chain);
      assertEquals(1, promoted.size());
      assertTrue(promoted.getFirst().isClosed(), "the complete-page fallback releases its guard too");
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void shortChainsResolveOnlyRequestedSlotsThenReusePositiveAndNegativeMiniEntries(final VersioningType versioning) {
    for (int length = 1; length <= 3; length++) {
      try (Fixture fixture = new Fixture(true, versioning)) {
        fixture.chain.setPageFragments(new ArrayList<>(fixture.chain.getPageFragments().subList(0, length)));
        fixture.add(5).put(KEY, VALUE);
        for (int revision = 4; revision >= 5 - length; revision--) {
          fixture.add(revision);
        }
        final long before = VersioningType.pointLeafReads();
        assertArrayEquals(VALUE, fixture.read().value());
        assertEquals(List.of(5L), fixture.reads, "first short-chain access resolves only the requested slot");
        final byte[] absent = {8};
        assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, absent, SIDE_KEY));
        assertEquals(1 + length, fixture.reads.size());
        for (int i = 0; i < 20; i++) {
          assertArrayEquals(VALUE, fixture.read().value());
          assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, absent, SIDE_KEY));
        }
        assertEquals(before + 2, VersioningType.pointLeafReads(), "both present and absent mini hits avoid the walk");
        assertEquals(1 + length, fixture.reads.size());
        assertNull(fixture.chain.getPage());
        assertEquals(0, fixture.completeCache.size());
        fixture.assertReleased();
      }
    }
  }

  @Test
  void miniHitCopiesNewestSideReferenceAndTombstones() {
    try (Fixture fixture = new Fixture(true)) {
      final PageReference newest = new PageReference().setKey(900).setDatabaseId(7).setResourceId(8);
      newest.setHash(1234L);
      fixture.add(5).setPageReference(SIDE_KEY, newest);
      final HOTLeafPage older = fixture.add(4);
      older.put(KEY, new byte[0]);
      older.setPageReference(SIDE_KEY, new PageReference().setKey(800));
      assertEquals(0, fixture.read().value().length);
      final HOTLeafEntry hit = fixture.read();
      assertEquals(0, hit.value().length);
      assertEquals(newest, hit.sideReference());
      assertTrue(hit.sideReference().hasHash());
      assertEquals(1234L, hit.sideReference().getHashAsLong());
      hit.sideReference().setKey(999);
      assertEquals(900L, fixture.read().sideReference().getKey());
      assertEquals(List.of(5L, 4L), fixture.reads);
      fixture.assertReleased();
    }
  }

  @Test
  void ordinaryCompleteLoaderDropsMiniAndReusesRawFragments() {
    try (Fixture fixture = new Fixture(true)) {
      fixture.add(5);
      fixture.add(4).put(KEY, VALUE);
      fixture.add(3).setCompleteDump(true);
      fixture.chain.setPageFragments(new ArrayList<>(fixture.chain.getPageFragments().subList(0, 2)));
      assertArrayEquals(VALUE, fixture.read().value());
      // A disk read returns a new image, not the object already owned by the raw cache.
      final HOTLeafPage freshHead = fixture.images.get(5L).copy();
      fixture.images.put(5L, freshHead);
      fixture.leaves.add(freshHead);
      final PageReference canonical = new PageReference().setKey(5)
                                                         .setDatabaseId(fixture.storage.getDatabaseId())
                                                         .setResourceId(fixture.storage.getResourceId());
      final HOTMiniPage mini = fixture.miniCache.getAndGuard(canonical);
      assertTrue(mini != null);
      final HOTLeafPage complete =
          assertInstanceOf(HOTLeafPage.class, fixture.storage.loadHOTPageAndGuard(fixture.chain));
      try {
        assertArrayEquals(VALUE, HOTLeafEntry.copyOf(complete, KEY, SIDE_KEY).value());
        assertNull(fixture.miniCache.getAndGuard(canonical));
        assertSame(complete, fixture.chain.getPage());
        assertEquals(1, mini.getGuardCount());
        assertFalse(mini.isClosed());
        assertFalse(mini.acquireGuard());
        // Head belongs to the complete loader; the already cached older fragment is reused.
        assertEquals(1, fixture.reads.stream().filter(offset -> offset == 4L).count());
      } finally {
        complete.releaseGuard();
        mini.releaseGuard();
      }
      assertTrue(mini.isClosed());
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void coldLongPointTailReadsScalarRawImagesWithoutCombiningAndReusesThem(final VersioningType versioning) {
    try (Fixture fixture = new Fixture(true, versioning, 32)) {
      for (int revision = 32; revision >= 1; revision--) {
        fixture.add(revision);
      }
      fixture.images.get(1L).put(KEY, VALUE);
      fixture.images.get(1L).setCompleteDump(true);
      final PageReference newestSide = new PageReference().setKey(900);
      newestSide.setHash(0L);
      fixture.images.get(32L).setPageReference(SIDE_KEY, newestSide);
      fixture.images.get(1L).setPageReference(SIDE_KEY, new PageReference().setKey(800));
      final long merges = VersioningType.multiFragmentMerges();
      final long walked = VersioningType.pointFragmentsWalked();
      final HOTLeafEntry result = fixture.read();
      assertArrayEquals(VALUE, result.value());
      assertSame(newestSide, result.sideReference());
      assertEquals(walked + 31, VersioningType.pointFragmentsWalked());
      assertEquals(merges, VersioningType.multiFragmentMerges(), "a point walk must not materialize a complete leaf");
      verify(fixture.disk, never()).readHOTLeafFragments(any(PageReference[].class), any(ResourceConfiguration.class));
      assertEquals(32, fixture.reads.size());
      for (int i = 0; i < fixture.reads.size(); i++) {
        assertEquals(32L - i, fixture.reads.get(i).longValue(), "raw images are read in logical newest-first order");
      }
      assertEquals(32, assertInstanceOf(ShardedPageCache.class, fixture.fragmentCache).size());
      assertNull(fixture.chain.getPage());
      fixture.assertReleased();
      assertNull(fixture.storage.readHOTProjectionEntry(fixture.chain, new byte[] {8}, SIDE_KEY));
      assertArrayEquals(VALUE, fixture.read().value());
      assertEquals(32, fixture.reads.size(), "both selective and cached point reads reuse all raw images");
      verify(fixture.disk, never()).readHOTLeafFragments(any(PageReference[].class), any(ResourceConfiguration.class));
      fixture.assertReleased();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void scalarPointTailStopsAtNewestTombstoneWithoutReadingOlderImages(final boolean cached) {
    try (Fixture fixture = new Fixture(cached, VersioningType.SLIDING_SNAPSHOT, 32)) {
      for (int revision = 32; revision >= 1; revision--) {
        final HOTLeafPage image = fixture.add(revision);
        if (revision < 32) {
          image.put(KEY, revision == 31
              ? new byte[0]
              : VALUE);
        }
      }
      final long walked = VersioningType.pointFragmentsWalked();
      assertEquals(0, fixture.read().value().length);
      assertEquals(walked + 1, VersioningType.pointFragmentsWalked());
      assertEquals(List.of(32L, 31L), fixture.reads, "a tombstone prevents all older physical reads");
      verify(fixture.disk, never()).readHOTLeafFragments(any(PageReference[].class), any(ResourceConfiguration.class));
      fixture.assertReleased();
      if (!cached) {
        for (final HOTLeafPage leaf : fixture.leaves) {
          assertEquals(leaf.getRevision() >= 31, leaf.isClosed(), "only the two read images became guarded orphans");
        }
      }
      assertNull(fixture.chain.getPage());
    }
  }

  @Test
  void failedScalarPointWalkReleasesItsLastGuardAndDoesNotReadPastFailure() {
    try (Fixture fixture = new Fixture(true, VersioningType.SLIDING_SNAPSHOT, 32)) {
      for (int revision = 32; revision >= 1; revision--) {
        fixture.add(revision);
      }
      when(fixture.images.get(16L).getRevision()).thenReturn(17);
      assertThrows(SirixIOException.class, fixture::read);
      assertEquals(17, fixture.reads.size());
      assertEquals(16L, fixture.reads.getLast().longValue());
      verify(fixture.disk, never()).readHOTLeafFragments(any(PageReference[].class), any(ResourceConfiguration.class));
      fixture.assertReleased();
      assertEquals(0, fixture.miniCache.getCurrentWeightBytes());
      assertNull(fixture.chain.getPage());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final Arena arena = Arena.ofShared();
    private final ShardedPageCache<HOTLeafPage> completeCache = new ShardedPageCache<>(1L << 20);
    private final Cache<PageReference, HOTLeafPage> fragmentCache;
    private final HOTMiniPageCache miniCache;
    private final Map<Long, HOTLeafPage> images = new HashMap<>();
    private final List<HOTLeafPage> leaves = new ArrayList<>();
    private final List<Long> reads = new ArrayList<>();
    private final PageReference chain = new PageReference().setKey(5);
    private final BufferManager buffers = mock(BufferManager.class);
    private final Reader disk = mock(Reader.class);
    private static final long EXTENT = 4096L;
    private final NodeStorageEngineReader storage;

    private Fixture(final boolean cached) {
      this(cached, VersioningType.SLIDING_SNAPSHOT);
    }

    private Fixture(final boolean cached, final VersioningType versioning) {
      this(cached, versioning, 5);
    }

    private Fixture(final boolean cached, final VersioningType versioning, final int headRevision) {
      chain.setKey(headRevision);
      miniCache = cached
          ? new HOTMiniPageCache(1L << 20)
          : HOTMiniPageCache.disabled();
      fragmentCache = cached
          ? new ShardedPageCache<>(Math.max(1L << 20, (long) headRevision * HOTLeafPage.DEFAULT_SIZE * 2))
          : new EmptyCache<>();
      final ResourceConfiguration config = ResourceConfiguration.newBuilder("point-fragments")
                                                                .versioningApproach(versioning)
                                                                .maxNumberOfRevisionsToRestore(32)
                                                                .build();
      final InternalResourceSession<?, ?> session = mock(InternalResourceSession.class);
      final RevisionEpochTracker tracker = mock(RevisionEpochTracker.class);
      when(tracker.register(anyInt())).thenReturn(mock(Ticket.class));
      when(session.getRevisionEpochTracker()).thenReturn(tracker);
      when(session.getResourceConfig()).thenReturn(config);
      when(buffers.getHOTLeafPageCache()).thenReturn(completeCache);
      when(buffers.getHOTLeafFragmentCache()).thenReturn(fragmentCache);
      when(buffers.getHOTMiniPageCache()).thenReturn(miniCache);
      when(disk.readHOTLeafFragment(any(PageReference.class), any(ResourceConfiguration.class))).thenCallRealMethod();
      when(disk.readHOTLeafFragment(any(PageReference.class), any(ResourceConfiguration.class),
          anyLong())).thenCallRealMethod();
      when(disk.committedDataExtent()).thenReturn(EXTENT);
      when(
          disk.readHOTLeafFragments(any(PageReference[].class), any(ResourceConfiguration.class))).thenCallRealMethod();
      when(disk.read(any(PageReference[].class), any(ResourceConfiguration.class))).thenCallRealMethod();
      when(disk.read(any(PageReference.class), any(ResourceConfiguration.class))).thenAnswer(invocation -> {
        final long offset = invocation.<PageReference>getArgument(0).getKey();
        reads.add(offset);
        return images.get(offset);
      });
      for (int revision = headRevision - 1; revision >= 1; revision--) {
        chain.addPageFragment(new PageFragmentKeyImpl(revision, revision, 0, 0));
      }
      storage = new NodeStorageEngineReader(1, session, new UberPage(), headRevision, disk, buffers,
          mock(RevisionRootPageReader.class), null);
    }

    private HOTLeafPage add(final int revision) {
      final HOTLeafPage leaf = spy(new HOTLeafPage(revision, revision, IndexType.PROJECTION,
          arena.allocate(HOTLeafPage.DEFAULT_SIZE), null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0));
      images.put((long) revision, leaf);
      leaves.add(leaf);
      return leaf;
    }

    private HOTLeafEntry read() {
      return storage.readHOTProjectionEntry(chain, KEY, SIDE_KEY);
    }

    private void assertReleased() {
      for (final HOTLeafPage leaf : leaves)
        assertEquals(0, leaf.getGuardCount());
    }

    @Override
    public void close() {
      storage.close();
      completeCache.clear();
      fragmentCache.clear();
      miniCache.clear();
      for (final HOTLeafPage leaf : leaves)
        leaf.close();
      arena.close();
    }
  }
}
