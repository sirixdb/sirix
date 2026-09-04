/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durable projection side-reference lifecycle across every page-versioning strategy. The test uses
 * a three-revision restore window and six commits so DIFFERENTIAL reaches two full-dump boundaries,
 * while INCREMENTAL and SLIDING_SNAPSHOT both rotate their fragment windows.
 */
final class ProjectionSideReferenceVersioningMatrixTest {

  private static final String RESOURCE = "resource";
  private static final int INDEX_NUMBER = 0;
  private static final long ROW_GROUP_ID = 1;
  private static final long CHANGED_BLOB_SLOT = ProjectionIndexHOTStorage.bloomBlockSlotKey(0);
  private static final long UNCHANGED_BLOB_SLOT = ProjectionIndexHOTStorage.bloomBlockSlotKey(1);
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
  private static final String[] DEPARTMENTS = departments();

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void referencedSegmentsAndBlobsSurviveEveryVersioningCadence(final VersioningType versioningType) throws IOException {
    final byte[] largeRowGroupA = rowGroup(ProjectionIndexRowGroupPage.MAX_ROWS, 50_000L, 0);
    final byte[] largeRowGroupB = rowGroup(ProjectionIndexRowGroupPage.MAX_ROWS, 50_000L, 1);
    final byte[] smallRowGroup = rowGroup(1, 50_000L, 2);
    final byte[] largeRowGroupC = rowGroup(ProjectionIndexRowGroupPage.MAX_ROWS, 50_000L, 3);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedA =
        ProjectionIndexColumnSegmentCodec.encode(largeRowGroupA);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedB =
        ProjectionIndexColumnSegmentCodec.encode(largeRowGroupB);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedSmall =
        ProjectionIndexColumnSegmentCodec.encode(smallRowGroup);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedC =
        ProjectionIndexColumnSegmentCodec.encode(largeRowGroupC);
    final int changingSegmentId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    final int unchangedSegmentId = ProjectionIndexColumnSegmentCodec.keysColumnSegmentId();

    assertTrue(segment(encodedA, changingSegmentId).length > 512);
    assertTrue(segment(encodedB, changingSegmentId).length > 512);
    assertTrue(segment(encodedSmall, changingSegmentId).length <= 512);
    assertTrue(segment(encodedC, changingSegmentId).length > 512);
    assertTrue(segment(encodedA, unchangedSegmentId).length > 512);

    final byte[] changedBlobA = bytes(700, 11);
    final byte[] changedBlobB = bytes(800, 12);
    final byte[] changedBlobInline = bytes(64, 13);
    final byte[] changedBlobC = bytes(900, 14);
    final byte[] unchangedBlob = bytes(750, 15);
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());

    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedA);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobA);
          storage.putBlob(UNCHANGED_BLOB_SLOT, unchangedBlob);
        });
        commit(session, storage -> storage.putBlob(CHANGED_BLOB_SLOT, changedBlobB));
        commit(session, storage -> storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedB));
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedSmall);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobInline);
        });
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedC);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobC);
        });
        commit(session, storage -> {
          storage.tombstoneRowGroupAsColumnSegmentSlots(ROW_GROUP_ID);
          storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
        });
        assertEquals(6, session.getMostRecentRevisionNumber());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final RevisionState revision1 = assertRevision(session, 1, largeRowGroupA, encodedA.descriptor(), changedBlobA,
          unchangedBlob, changingSegmentId, unchangedSegmentId);
      final RevisionState revision2 = assertRevision(session, 2, largeRowGroupA, encodedA.descriptor(), changedBlobB,
          unchangedBlob, changingSegmentId, unchangedSegmentId);
      final RevisionState revision3 = assertRevision(session, 3, largeRowGroupB, encodedB.descriptor(), changedBlobB,
          unchangedBlob, changingSegmentId, unchangedSegmentId);
      final RevisionState revision4 = assertRevision(session, 4, smallRowGroup, encodedSmall.descriptor(),
          changedBlobInline, unchangedBlob, changingSegmentId, unchangedSegmentId);
      final RevisionState revision5 = assertRevision(session, 5, largeRowGroupC, encodedC.descriptor(), changedBlobC,
          unchangedBlob, changingSegmentId, unchangedSegmentId);
      final RevisionState revision6 =
          assertRevision(session, 6, null, null, null, unchangedBlob, changingSegmentId, unchangedSegmentId);

      assertTrue(revision1.changingSegmentOffset() >= 0, "the large column body must be referenced");
      assertTrue(revision1.unchangedSegmentOffset() >= 0, "the large key segment must be referenced");
      assertEquals(revision1.changingSegmentOffset(), revision2.changingSegmentOffset(),
          "an untouched referenced segment must carry its exact durable page into revision 2");
      assertNotEquals(revision2.changingSegmentOffset(), revision3.changingSegmentOffset(),
          "updating a referenced segment must publish a new durable page");
      assertTrue(revision4.changingSegmentOffset() < 0, "referenced-to-inline must remove the side reference");
      assertTrue(revision5.changingSegmentOffset() >= 0, "inline-to-referenced must publish a side reference");
      assertTrue(revision6.changingSegmentOffset() < 0, "deleting the row group must remove its side reference");
      assertTrue(revision6.unchangedSegmentOffset() < 0,
          "deleting the row group must remove every unchanged segment side reference too");

      assertEquals(revision1.unchangedSegmentOffset(), revision2.unchangedSegmentOffset());
      assertEquals(revision2.unchangedSegmentOffset(), revision3.unchangedSegmentOffset(),
          "a segment outside the edited column must remain shared by durable offset");

      assertTrue(revision1.changedBlobOffset() >= 0, "the large changed blob must be referenced");
      assertNotEquals(revision1.changedBlobOffset(), revision2.changedBlobOffset(),
          "updating a referenced blob must publish a new durable page");
      assertEquals(revision2.changedBlobOffset(), revision3.changedBlobOffset(),
          "an untouched referenced blob must carry its exact durable page into revision 3");
      assertTrue(revision4.changedBlobOffset() < 0, "referenced-to-inline blob migration must remove the side ref");
      assertTrue(revision5.changedBlobOffset() >= 0, "inline-to-referenced blob migration must publish a side ref");
      assertTrue(revision6.changedBlobOffset() < 0, "deleting the blob must remove its side ref");

      final long unchangedBlobOffset = revision1.unchangedBlobOffset();
      assertTrue(unchangedBlobOffset >= 0);
      assertEquals(unchangedBlobOffset, revision2.unchangedBlobOffset());
      assertEquals(unchangedBlobOffset, revision3.unchangedBlobOffset());
      assertEquals(unchangedBlobOffset, revision4.unchangedBlobOffset());
      assertEquals(unchangedBlobOffset, revision5.unchangedBlobOffset());
      assertEquals(unchangedBlobOffset, revision6.unchangedBlobOffset(),
          "an unchanged referenced blob must survive two version-window rotations without a rewrite");
    }
  }

  @ParameterizedTest(name = "rollback-{0}")
  @EnumSource(VersioningType.class)
  void rolledBackSideReferenceMutationPreservesColdCommittedState(final VersioningType versioningType)
      throws IOException {
    final byte[] rowGroupA = rowGroup(ProjectionIndexRowGroupPage.MAX_ROWS, 70_000L, 0);
    final byte[] rowGroupB = rowGroup(ProjectionIndexRowGroupPage.MAX_ROWS, 70_000L, 1);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedA =
        ProjectionIndexColumnSegmentCodec.encode(rowGroupA);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedB =
        ProjectionIndexColumnSegmentCodec.encode(rowGroupB);
    final byte[] changedBlobA = bytes(700, 21);
    final byte[] changedBlobB = bytes(800, 22);
    final byte[] unchangedBlob = bytes(750, 23);
    final Path databasePath = temporaryDirectory.resolve("rollback-" + versioningType.name().toLowerCase());

    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .versioningApproach(versioningType)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedA);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobA);
          storage.putBlob(UNCHANGED_BLOB_SLOT, unchangedBlob);
        });
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedB);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobB);
        });
        commit(session, storage -> {
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedA);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobA);
        });

        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          storage.putRowGroupAsColumnSegmentSlots(ROW_GROUP_ID, encodedB);
          storage.putBlob(CHANGED_BLOB_SLOT, changedBlobB);
          storage.tombstoneBlob(UNCHANGED_BLOB_SLOT);
          assertArrayEquals(rowGroupB, storage.getRowGroupFromColumnSegmentSlots(ROW_GROUP_ID),
              "the writer must observe its uncommitted row-group replacement");
          assertArrayEquals(changedBlobB, storage.getBlob(CHANGED_BLOB_SLOT),
              "the writer must observe its uncommitted blob replacement");
          assertNull(storage.getBlob(UNCHANGED_BLOB_SLOT),
              "the writer must observe its uncommitted side-reference deletion");
          wtx.rollback();
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int changingSegmentId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
      final int unchangedSegmentId = ProjectionIndexColumnSegmentCodec.keysColumnSegmentId();
      final RevisionState revision3 = assertRevision(session, 3, rowGroupA, encodedA.descriptor(), changedBlobA,
          unchangedBlob, changingSegmentId, unchangedSegmentId);
      assertTrue(revision3.changingSegmentOffset() >= 0,
          "rollback must preserve the committed referenced column segment");
      assertTrue(revision3.changedBlobOffset() >= 0, "rollback must preserve the committed referenced blob");
      assertTrue(revision3.unchangedBlobOffset() >= 0,
          "rollback must preserve the committed side reference deleted only in the aborted revision");
    }
  }

  private static RevisionState assertRevision(final JsonResourceSession session, final int revision,
      final byte[] expectedRowGroup, final byte[] expectedDescriptor, final byte[] expectedChangedBlob,
      final byte[] expectedUnchangedBlob, final int changingSegmentId, final int unchangedSegmentId) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      if (expectedRowGroup == null) {
        assertNull(ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, INDEX_NUMBER, ROW_GROUP_ID));
        assertNull(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(ROW_GROUP_ID)));
      } else {
        assertArrayEquals(expectedRowGroup,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, INDEX_NUMBER, ROW_GROUP_ID));
        assertArrayEquals(expectedDescriptor, ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(ROW_GROUP_ID)));
      }
      if (expectedChangedBlob == null) {
        assertNull(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, CHANGED_BLOB_SLOT));
      } else {
        assertArrayEquals(expectedChangedBlob,
            ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, CHANGED_BLOB_SLOT));
      }
      assertArrayEquals(expectedUnchangedBlob,
          ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, UNCHANGED_BLOB_SLOT));

      return new RevisionState(segmentOffset(reader, changingSegmentId), segmentOffset(reader, unchangedSegmentId),
          ProjectionIndexHOTStorage.segmentPageOffset(reader, INDEX_NUMBER, CHANGED_BLOB_SLOT, 0),
          ProjectionIndexHOTStorage.segmentPageOffset(reader, INDEX_NUMBER, UNCHANGED_BLOB_SLOT, 0));
    }
  }

  private static long segmentOffset(final StorageEngineReader reader, final int columnSegmentId) {
    return ProjectionIndexHOTStorage.segmentPageOffset(reader, INDEX_NUMBER,
        ProjectionIndexHOTStorage.columnSegmentSlotKey(ROW_GROUP_ID, columnSegmentId), 0);
  }

  private static void commit(final JsonResourceSession session, final Consumer<ProjectionIndexHOTStorage> mutation) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      mutation.accept(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER));
      wtx.commit();
    }
  }

  private static byte[] segment(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded,
      final int columnSegmentId) {
    final int index = Arrays.binarySearch(encoded.columnSegmentIds(), columnSegmentId);
    assertTrue(index >= 0, "missing segment " + columnSegmentId);
    return encoded.segments()[index];
  }

  private static byte[] rowGroup(final int rows, final long keyBase, final long ageBump) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random random = new Random(keyBase);
    final long[] longs = new long[KINDS.length];
    final boolean[] booleans = new boolean[KINDS.length];
    final String[] strings = new String[KINDS.length];
    final boolean[] present = new boolean[KINDS.length];
    final boolean[] unrepresentable = new boolean[KINDS.length];
    final boolean[] nonIntegral = new boolean[KINDS.length];
    Arrays.fill(present, true);
    long key = keyBase;
    for (int row = 0; row < rows; row++) {
      key += 1 + random.nextInt(1 << 20);
      longs[0] = random.nextInt(1 << 20) + ageBump;
      booleans[1] = random.nextBoolean();
      strings[2] = DEPARTMENTS[random.nextInt(DEPARTMENTS.length)];
      assertTrue(page.appendRow(key, longs, booleans, strings, present, unrepresentable, nonIntegral));
    }
    return page.serialize();
  }

  private static byte[] bytes(final int length, final long seed) {
    final byte[] bytes = new byte[length];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  private static String[] departments() {
    final String[] departments = new String[128];
    for (int i = 0; i < departments.length; i++) {
      departments[i] = "Department-" + (char) ('A' + i % 26) + '-' + String.format("%010d", i);
    }
    return departments;
  }

  private record RevisionState(long changingSegmentOffset, long unchangedSegmentOffset, long changedBlobOffset,
      long unchangedBlobOffset) {
  }
}
