package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bounded ("chunked") region read, which engages only when checksum verification is off.
 *
 * <p>That condition is the whole reason this test exists. The column-scan tests all run with
 * checksums ON, so they exercise the full-page region read and never touch this path — and the
 * defect it hid was invisible from every other angle: the chunk decoder built its
 * {@link RegionsOnlyPage} without a slot bitmap, so {@code hasSlotBitmap()} was false, so the
 * versioned column merge refused every fragment and quietly sent each multi-fragment page back to
 * reconstructing records. Nothing failed; the fast path simply stopped existing whenever it was
 * eligible, which is exactly the configuration a benchmark run selects.
 *
 * <p>So the assertion here is deliberately about the bitmap rather than about a query result: a
 * count would have agreed either way.
 */
@DisplayName("Chunked region read")
final class ChunkedRegionReadTest {

  private Path dbDir;

  @AfterEach
  void tearDown() {
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
      dbDir = null;
    }
  }

  /** Two revisions under a fragmenting strategy, so pages span commits. */
  private JsonResourceSession openWithSecondRevision() throws Exception {
    dbDir = Files.createTempDirectory("sirix-chunked-region-");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbDir));
    final var database = Databases.openJsonDatabase(dbDir);
    database.createResource(ResourceConfiguration.newBuilder("chunked")
                                                 .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                 // The two conditions that make the chunk path
                                                 // eligible; see FileChannelReader#regionChunkEligible.
                                                 .storeNodeHistory(false)
                                                 .buildPathSummary(true)
                                                 .build());
    final var session = database.beginResourceSession("chunked");

    final StringBuilder json = new StringBuilder(4096).append('[');
    for (int i = 0; i < 400; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(i).append(",\"year\":").append(1900 + (i % 120)).append('}');
    }
    json.append(']');

    try (final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
      wtx.commit();
    }
    // A second revision touching a subset leaves pages made of more than one fragment, which is
    // what the merge path needs and what the bitmap decides.
    try (final var wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      wtx.commit();
    }
    return session;
  }

  @Test
  @DisplayName("a chunk-read page carries its slot bitmap, so fragments stay mergeable")
  void chunkReadPageCarriesSlotBitmap() throws Exception {
    try (final var session = openWithSecondRevision()) {
      final int revision = session.getMostRecentRevisionNumber();
      final var reader = session.createStorageEngineReader(revision);
      try {
        RegionsOnlyPage seen = null;
        // Walk the first handful of record pages; the resource is small, and any one of them
        // proves the decoder keeps the bitmap.
        for (long pageKey = 0; pageKey < 8 && seen == null; pageKey++) {
          seen = reader.getRecordPageRegionsOnly(
              new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision), 0, 0);
        }
        assertNotNull(seen, "no record page could be read column-only at all");
        assertTrue(seen.hasSlotBitmap(),
                   "a column-only page came back without its slot bitmap: every fragment of a "
                       + "multi-fragment page is then refused by the merge, silently disabling the "
                       + "column path exactly when chunked reads are eligible");
      } finally {
        reader.close();
      }
    }
  }
}
