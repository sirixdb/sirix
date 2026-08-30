package io.sirix.io;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.pax.RegionTable;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bounded ("chunked") region read, which engages only when checksum verification is off.
 *
 * <p>
 * That condition is the whole reason this test exists. The column-scan tests all run with checksums
 * ON, so they exercise the full-page region read and never touch this path — and the defect it hid
 * was invisible from every other angle: the chunk decoder built its {@link RegionsOnlyPage} without
 * decoding the slot bitmap, so the versioned column merge refused every fragment and quietly sent
 * each multi-fragment page back to reconstructing records. Nothing failed; the fast path simply
 * stopped existing whenever it was eligible, which is exactly the configuration a benchmark run
 * selects.
 *
 * <h2>What these assertions have to be about</h2>
 *
 * <p>
 * Not a query result: a count is answered correctly by the record path either way, so it agrees
 * whether or not the column path exists.
 *
 * <p>
 * Not {@link RegionsOnlyPage#hasSlotBitmap()} either, which is what an earlier version of this test
 * asserted and why it passed with the fix reverted. That method reports {@code slotBitmap !=
 * null}, and the chunk reader allocates the bitmap array from a thread-local scratch and passes a
 * copy of it unconditionally — so the array is non-null however the probe behaved. Skipping the
 * bitmap leaves it all ZEROS, not absent, and an all-zero bitmap is worse than a missing one: it is
 * the confident and false claim that this fragment defines no slots, which resolves every slot to
 * an older fragment.
 *
 * <p>
 * So each assertion below is about bitmap CONTENT — its cardinality against the populated-slot
 * count the same header carries — plus one about the chunk path having been taken at all, because
 * the read declines by silently falling back to the whole page, and a test that stopped covering
 * this path would otherwise keep passing on the fallback's bitmap.
 */
@DisplayName("Chunked region read")
final class ChunkedRegionReadTest {

  /** The columns a numeric scan asks for; also what the fixture's pages actually carry. */
  private static final int NUMBER_MASK = 1 << RegionTable.KIND_NUMBER;

  /** Record pages to walk looking for the fixture's pages. The resource is small. */
  private static final int MAX_PAGE_KEY = 8;

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
                                                 // The conditions FileChannelReader#regionChunkEligible
                                                 // actually tests. verifyChecksumsOnRead defaults to
                                                 // TRUE, and without clearing it the read never enters
                                                 // the chunk path at all. FSST is excluded for the same
                                                 // reason: its symbol-table id lives in the page tail,
                                                 // behind everything a bounded read would stop before.
                                                 .verifyChecksumsOnRead(false)
                                                 .stringCompressionType(StringCompressionType.NONE)
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
      // Change a real value so the second revision genuinely leaves a second fragment: array ->
      // first object -> its "year" field.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      wtx.moveToFirstChild();
      wtx.moveToRightSibling();
      wtx.setNumberValue(4242);
      wtx.commit();
    }
    // Reopen from scratch. Everything written above is still in the page cache, and a cached page
    // is handed back without going near the disk decoder — which is where the bitmap is read and
    // therefore the only place the defect can show. Reading through the same session proves
    // nothing about what a chunked read off disk returns.
    session.close();
    database.close();
    final var reopened = Databases.openJsonDatabase(dbDir);
    return reopened.beginResourceSession("chunked");
  }

  /**
   * The bitmap decoded off disk has to describe the page it came from. Cardinality against the
   * populated-slot count is the check that distinguishes a decoded bitmap from an allocated one: both
   * are non-null, only one of them counts the page's slots.
   */
  private static void assertBitmapDescribesPage(final RegionsOnlyPage page, final String what) {
    assertTrue(page.hasSlotBitmap(), what + " came back with no slot bitmap at all");
    assertTrue(page.getPopulatedSlotCount() > 0, what + " reports no populated slots, so it cannot witness anything");
    assertEquals(page.getPopulatedSlotCount(), page.definedSlotCount(),
        what + " carries a slot bitmap that does not describe its own slots. All-zero "
            + "means the region decoder allocated the bitmap but never read it, and a "
            + "fragment that claims to define nothing loses every slot it owns to an "
            + "older fragment in the column merge");
  }

  @Test
  @DisplayName("a chunk-read page decodes its slot bitmap, not just an empty one")
  void chunkReadPageDecodesItsSlotBitmap() throws Exception {
    try (final var session = openWithSecondRevision()) {
      final int revision = session.getMostRecentRevisionNumber();
      // Evict the resident pages. NodeStorageEngineReader serves a cached page whenever its region
      // table satisfies the requested kinds, and that page carries its bitmap for reasons that have
      // nothing to do with the disk decoder this test is about.
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      final var reader = session.createStorageEngineReader(revision);
      try {
        final long chunkHitsBefore = AbstractReader.regionChunkHits();
        RegionsOnlyPage seen = null;
        // Walk the record pages for one the single-page entry point serves. A multi-fragment page
        // declines here by design and is covered by the fragment test below.
        for (long pageKey = 0; pageKey < MAX_PAGE_KEY && seen == null; pageKey++) {
          seen = reader.getRecordPageRegionsOnly(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision), NUMBER_MASK,
              0);
        }
        assertNotNull(seen, "no record page could be read column-only at all");
        try (RegionsOnlyPage page = seen) {
          assertTrue(AbstractReader.regionChunkHits() > chunkHitsBefore,
              "the column-only read never took the bounded chunk path, so this test is "
                  + "asserting about the whole-page reader instead. Check "
                  + "FileChannelReader#regionChunkEligible against this fixture's config "
                  + "(verifyChecksumsOnRead off, non-FSST) before trusting it again");
          assertBitmapDescribesPage(page, "a chunk-read page");
        }
      } finally {
        reader.close();
      }
    }
  }

  /**
   * The reason the bitmap is load-bearing. {@code getRecordPageFragmentRegions} is what feeds the
   * versioned column merge, and it hands back the whole chain or nothing — so a chain in which any
   * fragment misreports the slots it defines produces a merged column that silently resolves those
   * slots to an older revision.
   */
  @Test
  @DisplayName("every fragment of a multi-fragment page reports the slots it defines")
  void everyFragmentReportsTheSlotsItDefines() throws Exception {
    try (final var session = openWithSecondRevision()) {
      final int revision = session.getMostRecentRevisionNumber();
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      final var reader = session.createStorageEngineReader(revision);
      try {
        final long chunkHitsBefore = AbstractReader.regionChunkHits();
        RegionsOnlyPage[] fragments = null;
        for (long pageKey = 0; pageKey < MAX_PAGE_KEY && fragments == null; pageKey++) {
          fragments = reader.getRecordPageFragmentRegions(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision),
              NUMBER_MASK);
        }
        assertNotNull(fragments, "no multi-fragment page was served column-only, so the merge input this "
            + "test exists for was never built — the second revision may no longer " + "be leaving a second fragment");
        assertTrue(fragments.length > 1, "expected a chain, got " + fragments.length + " fragment");
        assertTrue(AbstractReader.regionChunkHits() > chunkHitsBefore,
            "the fragment reads never took the bounded chunk path");
        try {
          for (int i = 0; i < fragments.length; i++) {
            assertBitmapDescribesPage(fragments[i], "fragment " + i + " of " + fragments.length);
          }
        } finally {
          for (final RegionsOnlyPage fragment : fragments) {
            fragment.close();
          }
        }
      } finally {
        reader.close();
      }
    }
  }
}
