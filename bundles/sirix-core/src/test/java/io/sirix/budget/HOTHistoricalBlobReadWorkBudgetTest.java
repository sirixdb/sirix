/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Warm first/last blob probes use one bounded native suffix lane per search step. The historical
 * view is reconstructed before capture; its first slot forces a four-byte common prefix while the
 * remaining 128 eight-byte keys differ in their final byte. More than 32 entries disables the
 * small-leaf PEXT route. A referenced marker also checks overflow provenance.
 *
 * <p>
 * Measured at revisions 65, 1 and 130 in all four versioning modes: last-key reads use 7 suffix
 * lanes and 0 side-map probes, first/last reads 15 lanes and 0 probes, and an overflow marker 8
 * lanes and 1 probe. Restoring the original lookup at revision 65 reads 28 suffix lanes and probes
 * the inline slot's side map once; all four modes fail the unchanged 8-lane ceiling.
 */
@Isolated
final class HOTHistoricalBlobReadWorkBudgetTest {
  private static final long FIRST_SLOT = 1L;
  private static final long LAST_GROUP = 1L << 31;
  private static final int SLOTS = 128;
  private static final int REVISIONS = 130;
  private static final WorkCounter SUFFIX_READS = EngineWorkCounters.HOT_SUFFIX_PROBE_READS;
  private static final WorkCounter SIDE_READS = EngineWorkCounters.HOT_SIDE_REFERENCE_READS;
  private static final WorkCapture CAPTURE = WorkCapture.of(SUFFIX_READS, SIDE_READS);

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void historicalLastAndSpanReadsUseBoundedSuffixLanes(final VersioningType versioning) throws Exception {
    final Path path = directory.resolve(versioning.name());
    final byte[] overflow = new byte[1200];
    Arrays.fill(overflow, (byte) 73);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .useTextCompression(false)
                                                              .useDeweyIDs(false)
                                                              .storeDiffs(false)
                                                              .storeNodeHistory(false)
                                                              .buildPathSummary(false)
                                                              .hashKind(HashType.NONE)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= REVISIONS; revision++) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
            if (revision == 1) {
              for (int slot = 0; slot < SLOTS; slot++) {
                storage.putBlob(LAST_GROUP + slot, payload(slot, 1));
              }
              storage.putBlob(FIRST_SLOT, payload(0, 1));
              storage.putBlob(LAST_GROUP + SLOTS / 2, overflow);
            } else {
              storage.putBlob(FIRST_SLOT, payload(0, revision));
            }
            writer.commit();
          }
        }
      }
    }
    for (final int revision : new int[] {65, 1, REVISIONS}) {
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
        final StorageEngineReader reader = trx.getStorageEngineReader();
        final byte[] last = payload(SLOTS - 1, 1);
        assertArrayEquals(last, ProjectionIndexHOTStorage.readBlob(reader, 0, LAST_GROUP + SLOTS - 1));
        final WorkCapture.Captured<byte[]> maximum =
            CAPTURE.call(() -> ProjectionIndexHOTStorage.readBlob(reader, 0, LAST_GROUP + SLOTS - 1));
        assertArrayEquals(last, maximum.result());
        maximum.work()
               .assertBetween(SUFFIX_READS, 1, 8,
                   "revision " + revision + ": a last-key probe must read one bounded suffix lane per search step")
               .assertExactly(SIDE_READS, 0, "an inline last-key read needs no overflow reference");
        final WorkReport span = CAPTURE.run(() -> {
          assertArrayEquals(payload(0, revision), ProjectionIndexHOTStorage.readBlob(reader, 0, FIRST_SLOT));
          assertArrayEquals(last, ProjectionIndexHOTStorage.readBlob(reader, 0, LAST_GROUP + SLOTS - 1));
        });
        span.assertBetween(SUFFIX_READS, 2, 16,
            "first/last probes must each stay within one suffix lane per search step")
            .assertExactly(SIDE_READS, 0, "both span endpoints are inline");
        final WorkCapture.Captured<byte[]> referenced =
            CAPTURE.call(() -> ProjectionIndexHOTStorage.readBlob(reader, 0, LAST_GROUP + SLOTS / 2));
        assertArrayEquals(overflow, referenced.result());
        referenced.work()
                  .assertBetween(SUFFIX_READS, 1, 8, "the same lane budget applies to an overflow marker")
                  .assertExactly(SIDE_READS, 1, "an overflow marker must still resolve its newest reference");
      }
    }
  }

  private static byte[] payload(final int slot, final int revision) {
    return new byte[] {(byte) slot, (byte) revision, 2, 3, 4, 5, 6, 7};
  }
}
