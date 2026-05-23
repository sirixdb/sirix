/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.name;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Formal differential verification for Approach B
 * (docs/NAME_DICTIONARY_RECONSTRUCTION_PLAN.md): the O(live) bitmap-driven
 * {@link Names#fromStorage(io.sirix.api.StorageEngineReader, int, long, Roaring64Bitmap)}
 * reconstruction must produce a dictionary identical to the proven-correct
 * {@code 1..maxNodeKey} scan, for every committed revision, under name churn (which inflates the
 * node-key high-water) and under hash collisions.
 *
 * <p>The scan is the oracle (I-N1..I-N7 in the plan). Equivalence to the scan transitively
 * establishes that the bitmap path preserves the bijection, count consistency and historical
 * fidelity invariants.
 */
final class NamesReconstructionDifferentialTest {

  private static final int JSON_KEYS_OFFSET = NamePage.JSON_OBJECT_KEY_REFERENCE_OFFSET;

  /** A churning, sliding window of object-key names → old names removed, new ones allocate fresh
   *  node keys, so {@code maxNodeKey} grows while the live set stays small. */
  @Test
  void bitmapReconstructionEqualsScanUnderNameChurn(@TempDir final Path tempDir) {
    final Path dbPath = tempDir.resolve("names-churn");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
          .maxNumberOfRevisionsToRestore(5).build());
      try (final JsonResourceSession session = database.beginResourceSession("res");
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(window(0, 10)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
        assertReconstructionsMatch(session, JSON_KEYS_OFFSET);

        for (int rev = 1; rev <= 40; rev++) {
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(window(rev, 10)),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
          assertReconstructionsMatch(session, JSON_KEYS_OFFSET);
        }
      }
    }
  }

  /** Forced hash collisions: "Aa" and "BB" share {@link String#hashCode()} (2112), as do their
   *  concatenations. The dictionary resolves these to distinct keys via getNewKey; reconstruction
   *  must reproduce the stored keys identically through the bitmap path. */
  @Test
  void bitmapReconstructionEqualsScanWithHashCollisions(@TempDir final Path tempDir) {
    final Path dbPath = tempDir.resolve("names-collision");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("res")
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
          .maxNumberOfRevisionsToRestore(5).build());
      try (final JsonResourceSession session = database.beginResourceSession("res");
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        // All four keys collide on hashCode() == 2*2112* ... (Aa/BB family).
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"Aa\":0,\"BB\":1,\"AaAa\":2,\"AaBB\":3,\"BBAa\":4,\"BBBB\":5}"), JsonNodeTrx.Commit.NO);
        wtx.commit();
        assertReconstructionsMatch(session, JSON_KEYS_OFFSET);

        // Churn the collided set: remove all, re-add a subset → re-allocates colliding keys.
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.remove();
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"BB\":0,\"AaAa\":1,\"BBBB\":2}"), JsonNodeTrx.Commit.NO);
        wtx.commit();
        assertReconstructionsMatch(session, JSON_KEYS_OFFSET);
      }
    }
  }

  /** Reconstruct the jsonObjectKeys dictionary at the current revision both ways and assert equal. */
  private static void assertReconstructionsMatch(final JsonResourceSession session, final int offset) {
    final int rev = session.getMostRecentRevisionNumber();
    try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
      final var reader = rtx.getStorageEngineReader();
      final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
      final long maxNodeKey = namePage.getMaxNodeKey(offset);
      final Roaring64Bitmap liveKeys = namePage.getLiveEntryNodeKeysToSerialize(offset);

      final Names scanReconstructed = Names.fromStorage(reader, offset, maxNodeKey);
      final Names bitmapReconstructed = Names.fromStorage(reader, offset, maxNodeKey, liveKeys);

      assertTrue(scanReconstructed.contentEquals(bitmapReconstructed),
          "bitmap reconstruction must equal scan reconstruction at rev " + rev
              + " (maxNodeKey=" + maxNodeKey + ", liveKeys=" + liveKeys.getLongCardinality() + ")");
      // Symmetry guard against a vacuous contentEquals.
      assertTrue(bitmapReconstructed.contentEquals(scanReconstructed), "contentEquals must be symmetric");
    }
  }

  /** {@code {"k":{"f<lo>":0,...,"f<lo+n-1>":0}}} — a sliding window of n distinct object keys. */
  private static String window(final int lo, final int n) {
    final StringBuilder sb = new StringBuilder("{\"k\":{");
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("\"f").append(lo + i).append("\":0");
    }
    return sb.append("}}").toString();
  }
}
