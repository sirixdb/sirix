/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.node.LE;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Committed projection reads must distinguish valid tombstones from corrupt packed values. */
final class ProjectionCommittedSlotCorruptionTest {

  @Test
  void everyCommittedSlotReaderFailsClosedOnAStableUnreadableValue() {
    assertUnreadableValueRejected(17L,
        (reader, slotKey) -> ProjectionIndexHOTStorage.readColumnSegmentSlot(reader, 0, slotKey));
    assertUnreadableValueRejected(-17L, (reader, slotKey) -> ProjectionIndexHOTStorage.readRawSlot(reader, 0, slotKey));
    assertUnreadableValueRejected(0L, (reader, slotKey) -> ProjectionIndexHOTStorage.readBlob(reader, 0, slotKey));
  }

  @Test
  void everyCommittedSlotReaderStillTreatsAValidZeroLengthValueAsATombstone() {
    assertValidTombstoneIsAbsent(17L,
        (reader, slotKey) -> ProjectionIndexHOTStorage.readColumnSegmentSlot(reader, 0, slotKey));
    assertValidTombstoneIsAbsent(-17L, (reader, slotKey) -> ProjectionIndexHOTStorage.readRawSlot(reader, 0, slotKey));
    assertValidTombstoneIsAbsent(0L, (reader, slotKey) -> ProjectionIndexHOTStorage.readBlob(reader, 0, slotKey));
  }

  private static void assertUnreadableValueRejected(final long slotKey, final CommittedSlotRead read) {
    try (Fixture fixture = fixture(slotKey, 100)) {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> read.read(fixture.reader, slotKey));
      assertTrue(failure.getMessage().contains("unreadable value"), failure::getMessage);
    }
  }

  private static void assertValidTombstoneIsAbsent(final long slotKey, final CommittedSlotRead read) {
    try (Fixture fixture = fixture(slotKey, 0)) {
      assertNull(read.read(fixture.reader, slotKey));
    }
  }

  /** One resident, stamp-stable leaf whose declared value may extend beyond its packed bytes. */
  private static Fixture fixture(final long slotKey, final int declaredValueLength) {
    final Arena arena = Arena.ofConfined();
    try {
      final MemorySegment packedEntry = arena.allocate(12);
      packedEntry.set(LE.SHORT, 0, (short) 8);
      final byte[] key = new byte[8];
      PathKeySerializer.INSTANCE.serialize(slotKey, key, 0);
      MemorySegment.copy(key, 0, packedEntry, ValueLayout.JAVA_BYTE, 2, key.length);
      packedEntry.set(LE.SHORT, 10, (short) declaredValueLength);

      final HOTLeafPage leaf =
          new HOTLeafPage(1, 1, IndexType.PROJECTION, packedEntry, null, new int[] {0}, 1, 12, new byte[0], 0);
      final ProjectionIndexPage projectionPage = new ProjectionIndexPage();
      final PageReference rootReference = projectionPage.getOrCreateReference(0);
      rootReference.setPage(leaf);
      final RevisionRootPage revisionRootPage = mock(RevisionRootPage.class);
      final StorageEngineReader reader = mock(StorageEngineReader.class);
      when(reader.getActualRevisionRootPage()).thenReturn(revisionRootPage);
      when(reader.getProjectionIndexPage(revisionRootPage)).thenReturn(projectionPage);
      return new Fixture(arena, leaf, reader);
    } catch (final RuntimeException | Error failure) {
      arena.close();
      throw failure;
    }
  }

  @FunctionalInterface
  private interface CommittedSlotRead {
    byte[] read(StorageEngineReader reader, long slotKey);
  }

  private record Fixture(Arena arena, HOTLeafPage leaf, StorageEngineReader reader) implements AutoCloseable {
    @Override
    public void close() {
      try {
        leaf.close();
      } finally {
        arena.close();
      }
    }
  }
}
