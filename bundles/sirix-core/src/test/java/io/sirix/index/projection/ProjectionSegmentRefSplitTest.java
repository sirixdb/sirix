/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.HOTIncrementalInsert;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Segment references must survive the HOT trie's <em>incremental</em> leaf split, the split the
 * strand-discharge branch path takes.
 *
 * <p>
 * <b>The defect this pins down.</b> {@code HOTIncrementalInsert#splitLeafPage} rebuilds both halves
 * from {@code (key, value)} pairs and abandons the source leaf, so a leaf's segment-reference side
 * map — the durable offsets of a projection's out-of-line column segments — had no way across. The
 * path carried a loud guard instead of the routing, and the guard fired for real while building the
 * projection over a 100M-row corpus ("Incremental leaf split would drop 64 segment reference(s) on
 * leaf pageKey=5033"), aborting the build with the shred intact but no index. The 1M and 10M tiers
 * never produced the shape.
 *
 * <p>
 * <b>Why the writes are shuffled.</b> Ascending appends — the real build's order at the 1M/10M
 * tiers, and the shape {@code HOTBinnaConformanceTest} covers — never reach the incremental split:
 * they overflow leaves through the trie writer's own instrumented split variants. The strand
 * discharge that routes into {@code splitLeafPage} needs a key branching off ABOVE the leaf it
 * descended to, which an out-of-order arrival produces in quantity (73 ref-carrying splits at this
 * scale; zero when the same keys are written in order).
 */
@DisplayName("Projection segment references — incremental leaf split")
final class ProjectionSegmentRefSplitTest {

  private static final String RESOURCE_NAME = "projection-segment-ref-split";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Row groups written out of order. 1500 produced 73 ref-carrying incremental splits. */
  private static final int ROW_GROUPS = 1500;

  /** Column segments per row group, each in its own slot with its own segment page. */
  private static final int COLUMNS = 8;

  /** Above {@code BLOB_INLINE_MAX}, so every segment is REFERENCED (a side-map page), not inline. */
  private static final int SEGMENT_BYTES = 600;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("out-of-order row groups: every segment survives the incremental split, with its own bytes")
  @Timeout(value = 600, unit = TimeUnit.SECONDS)
  void segmentsSurviveIncrementalLeafSplits() throws IOException {
    final long carriesBefore = HOTIncrementalInsert.SPLIT_SEGMENT_REF_CARRIES.get();
    final long frontierSplicesBefore = AbstractHOTIndexWriter.DIRECTION_ONE_LEAF_FRONTIER_SPLICE.get();
    final long multiLeafFrontierSplicesBefore = AbstractHOTIndexWriter.DIRECTION_ONE_MULTI_LEAF_FRONTIER_SPLICE.get();
    final long fullHalfSubinsertsBefore = AbstractHOTIndexWriter.FULL_EXISTING_BIT_DIRECTION_ONE_SUBINSERT.get();
    final long directionOneFallbacksBefore = AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get();
    final long projectionRebuildAttemptsBefore = AbstractHOTIndexWriter.PROJECTION_REBUILD_SUBTREE_ATTEMPTED.get();
    final long subtreeRebuildsBefore = AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get();
    final List<Long> order = new ArrayList<>(ROW_GROUPS);
    for (long rg = 1; rg <= ROW_GROUPS; rg++) {
      order.add(rg);
    }
    Collections.shuffle(order, new Random(42));

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        final byte[] descriptor = new byte[24];
        for (final long rg : order) {
          descriptor[0] = (byte) rg;
          storage.putBlob(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(rg), descriptor.clone());
          for (int col = 0; col < COLUMNS; col++) {
            storage.putColumnSegmentSlot(ProjectionIndexHOTStorage.columnSegmentSlotKey(rg, col), segment(rg, col));
          }
        }
        wtx.commit();
      }

      final long carries = HOTIncrementalInsert.SPLIT_SEGMENT_REF_CARRIES.get() - carriesBefore;
      assertTrue(carries > 0, "the build did not reach the incremental split with references pending — the "
          + "test no longer covers the path it exists for (carries=" + carries + ")");
      assertTrue(AbstractHOTIndexWriter.DIRECTION_ONE_LEAF_FRONTIER_SPLICE.get() - frontierSplicesBefore > 0,
          "the build no longer covers the I8/I12-safe direct-leaf-frontier Direction-1 splice");
      assertTrue(
          AbstractHOTIndexWriter.DIRECTION_ONE_MULTI_LEAF_FRONTIER_SPLICE.get() - multiLeafFrontierSplicesBefore > 0,
          "the build no longer covers a non-sibling adjacent pair expanded to its complete leaf frontier");
      assertTrue(AbstractHOTIndexWriter.FULL_EXISTING_BIT_DIRECTION_ONE_SUBINSERT.get() - fullHalfSubinsertsBefore > 0,
          "the build no longer covers Direction 1 inside a freshly split full-node half");
      assertEquals(0L, AbstractHOTIndexWriter.DIRECTION_ONE_FALLBACK.get() - directionOneFallbacksBefore,
          "projection insertion must not abandon a Direction-1 shape to subtree reconstruction");
      assertEquals(0L,
          AbstractHOTIndexWriter.PROJECTION_REBUILD_SUBTREE_ATTEMPTED.get() - projectionRebuildAttemptsBefore,
          "projection insertion attempted a forbidden subtree rebuild");
      assertEquals(0L, AbstractHOTIndexWriter.REBUILD_SUBTREE_CALLED.get() - subtreeRebuildsBefore,
          "projection insertion performed a subtree rebuild");

      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
        assertNotNull(root, "projection HOT root must exist after commit");

        int missing = 0;
        int wrongBytes = 0;
        final StringBuilder detail = new StringBuilder();
        for (long rg = 1; rg <= ROW_GROUPS; rg++) {
          for (int col = 0; col < COLUMNS; col++) {
            final long slotKey = ProjectionIndexHOTStorage.columnSegmentSlotKey(rg, col);
            final byte[] got = ProjectionIndexHOTStorage.readColumnSegmentSlot(reader, 0, slotKey);
            final boolean orphaned = got == null;
            final boolean misRouted = !orphaned && !Arrays.equals(got, segment(rg, col));
            if (orphaned) {
              missing++;
            } else if (misRouted) {
              wrongBytes++;
            }
            if ((orphaned || misRouted) && detail.length() < 400) {
              detail.append("\nrowGroup ")
                    .append(rg)
                    .append(" column ")
                    .append(col)
                    .append(orphaned
                        ? " (segment page orphaned)"
                        : " (segment page belongs to another slot)");
            }
          }
        }
        assertTrue(missing == 0 && wrongBytes == 0, "segment pages lost or mis-routed by a leaf split (" + missing
            + " orphaned, " + wrongBytes + " mis-routed of " + (ROW_GROUPS * COLUMNS) + "):" + detail);

        // The descriptors ride the same leaves; a split that mangled the entries would show here.
        final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, reader);
        assertTrue(result.hardViolations().isEmpty(),
            "hard invariant violations after ref-carrying splits: " + result.hardViolations());
      }
    }
  }

  /** Distinct bytes per (rowGroup, column), so a mis-routed reference reads as wrong content. */
  private static byte[] segment(final long rowGroupId, final int column) {
    final byte[] bytes = new byte[SEGMENT_BYTES];
    bytes[0] = (byte) rowGroupId;
    bytes[1] = (byte) (rowGroupId >>> 8);
    bytes[2] = (byte) column;
    return bytes;
  }
}
