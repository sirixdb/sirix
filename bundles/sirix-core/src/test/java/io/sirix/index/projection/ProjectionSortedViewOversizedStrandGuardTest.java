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
import io.sirix.index.hot.AbstractHOTIndexWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bounded reproduction of the JSONBench 1M load refusal that the sorted projection chunk layout
 * (#1287) exposed: {@code PROJECTION index 0 cannot complete mandatory bounded mutation traversal
 * 'existing-key routing-to-new-slot' (status=LEAF_LIMIT ...)} from the sorted directory's first
 * node write.
 *
 * <p>
 * The slot stream below is the initial sorted-view build as the projection storage emits it, with
 * every payload replaced by a same-sized filler: 15 numeric-proof chunks, then per data leaf a
 * referenced data-leaf blob, a referenced group summary and a run of inline group-SMA nodes (14 per
 * leaf for the first 44 leaves, 7 afterwards), an inline bounds chunk every 16 leaves, and finally
 * the first directory node. All of these families share the {@code 1 << 46} slot base and differ
 * only in bits 32..34, so they share one HOT compound node. The dense SMA family makes that node
 * discriminate its own id bits early; HOT's merge-versus-branch rule then forbids merging a
 * data-leaf or summary key whose mismatch bit the node already discriminates, so those two families
 * branch every 64 ids into under-filled leaves. By 1200 data leaves the child holding both families
 * spans more than one bounded HOT block. The directory node's key is the first of its family and
 * needs a new discriminative bit at that node; the stranding guard of the incremental fold must
 * inspect the oversized child, which exceeds the one-frontier budget. The guard now declines the
 * optional fold instead of refusing the transaction, and the complete structural frontier places
 * the key.
 * </p>
 */
final class ProjectionSortedViewOversizedStrandGuardTest {
  private static final String RESOURCE = "resource";
  private static final int INDEX = 0;
  private static final int DATA_LEAVES = 1_200;
  private static final int DENSE_SMA_LEAVES = 44;
  private static final int SMA_NODES_PER_DENSE_LEAF = 14;
  private static final int SMA_NODES_PER_LEAF = 7;
  private static final int BOUNDS_LEAF_SHIFT = 4;
  private static final int NUMERIC_PROOF_CHUNKS = 15;
  /** One blob marker plus 495 payload bytes: the 512-byte inline slot value of a group-SMA node. */
  private static final int SMA_NODE_PAYLOAD_BYTES = 495;
  private static final int DATA_LEAF_PAYLOAD_BYTES = 6_000;
  private static final int SUMMARY_PAYLOAD_BYTES = 3_000;
  private static final int BOUNDS_PAYLOAD_BYTES = 272;
  private static final int NUMERIC_PROOF_PAYLOAD_BYTES = 2_072;
  private static final int DIRECTORY_NODE_PAYLOAD_BYTES = 2_000;

  @TempDir
  Path temporaryDirectory;

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  @DisplayName("the first directory node after a dense group-SMA family commits through the complete frontier")
  void firstDirectoryNodeAfterDenseSmaFamilyCommits() {
    final Path databasePath = temporaryDirectory.resolve("sorted-view-oversized-strand-guard");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final long refusedBefore = AbstractHOTIndexWriter.MUTATION_TRAVERSAL_REFUSED.get();
    final long declinedBefore = AbstractHOTIndexWriter.STRAND_GUARD_BUDGET_DECLINED.get();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
          final int smaNodes = writeSortedViewSlotStream(storage);
          wtx.commit();
          assertEquals(refusedBefore, AbstractHOTIndexWriter.MUTATION_TRAVERSAL_REFUSED.get(),
              "an oversized optional-fold guard must decline, never refuse the mutation");
          assertTrue(AbstractHOTIndexWriter.STRAND_GUARD_BUDGET_DECLINED.get() > declinedBefore,
              "the stream must reach the declined stranding guard; another HOT geometry proves nothing");
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            final StorageEngineReader reader = rtx.getStorageEngineReader();
            assertBlob(reader, ProjectionSortedDirectory.HEADER_SLOT + 1, DIRECTORY_NODE_PAYLOAD_BYTES);
            assertBlob(reader, dataLeafSlot(1), DATA_LEAF_PAYLOAD_BYTES);
            assertBlob(reader, dataLeafSlot(64), DATA_LEAF_PAYLOAD_BYTES);
            assertBlob(reader, dataLeafSlot(DATA_LEAVES), DATA_LEAF_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionSortedGroupSummary.slot(1), SUMMARY_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionSortedGroupSummary.slot(DATA_LEAVES), SUMMARY_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionSortedLeafBounds.slot(17), BOUNDS_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionNumericProofs.slot(1, 0), NUMERIC_PROOF_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionSortedGroupSma.HEADER_SLOT + 1, SMA_NODE_PAYLOAD_BYTES);
            assertBlob(reader, ProjectionSortedGroupSma.HEADER_SLOT + smaNodes, SMA_NODE_PAYLOAD_BYTES);
          }
        }
      }
    }
  }

  /**
   * Emit the captured initial-build slot stream shape; returns the number of group-SMA nodes written.
   */
  private static int writeSortedViewSlotStream(final ProjectionIndexHOTStorage storage) {
    for (int chunk = 0; chunk < NUMERIC_PROOF_CHUNKS; chunk++) {
      final long slot = ProjectionNumericProofs.slot(1, chunk);
      storage.putBlob(slot, payload(slot, NUMERIC_PROOF_PAYLOAD_BYTES));
    }
    int smaNode = 0;
    for (int leaf = 1; leaf <= DATA_LEAVES; leaf++) {
      storage.putBlob(dataLeafSlot(leaf), payload(dataLeafSlot(leaf), DATA_LEAF_PAYLOAD_BYTES));
      storage.putBlob(ProjectionSortedGroupSummary.slot(leaf),
          payload(ProjectionSortedGroupSummary.slot(leaf), SUMMARY_PAYLOAD_BYTES));
      final int smaNodes = leaf <= DENSE_SMA_LEAVES
          ? SMA_NODES_PER_DENSE_LEAF
          : SMA_NODES_PER_LEAF;
      for (int i = 0; i < smaNodes; i++) {
        smaNode++;
        final long slot = ProjectionSortedGroupSma.HEADER_SLOT + smaNode;
        storage.putBlob(slot, payload(slot, SMA_NODE_PAYLOAD_BYTES));
      }
      if (leaf > 1 && (leaf & ((1 << BOUNDS_LEAF_SHIFT) - 1)) == 1) {
        final long slot = ProjectionSortedLeafBounds.slot(leaf);
        storage.putBlob(slot, payload(slot, BOUNDS_PAYLOAD_BYTES));
      }
    }
    final long directoryNode = ProjectionSortedDirectory.HEADER_SLOT + 1;
    storage.putBlob(directoryNode, payload(directoryNode, DIRECTORY_NODE_PAYLOAD_BYTES));
    return smaNode;
  }

  private static long dataLeafSlot(final int leafId) {
    return ProjectionSortedLeafStore.LEAF_SLOT_BASE + leafId;
  }

  /** A slot-specific filler so a misrouted or stale blob cannot pass as the expected one. */
  private static byte[] payload(final long slot, final int length) {
    final byte[] payload = new byte[length];
    final byte seed = (byte) (slot ^ (slot >>> 32) ^ length);
    for (int i = 0; i < length; i++) {
      payload[i] = (byte) (seed + i);
    }
    return payload;
  }

  private static void assertBlob(final StorageEngineReader reader, final long slot, final int length) {
    final byte[] blob = ProjectionIndexHOTStorage.readBlob(reader, INDEX, slot);
    assertNotNull(blob, "slot " + slot + " must be readable after the commit");
    assertArrayEquals(payload(slot, length), blob, "slot " + slot + " must hold its own payload");
  }
}
