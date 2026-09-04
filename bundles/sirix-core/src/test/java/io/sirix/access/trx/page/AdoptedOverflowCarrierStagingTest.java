package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.io.SharedArenas;
import io.sirix.io.StorageType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A bulk-adopted leaf whose refused records became overflow carriers must NOT be pinned in the
 * intent log until final commit: its carriers are staged as immutable side pages at adoption, the
 * background flush defers the leaf exactly one epoch, and the next epoch writes it with durable
 * carrier keys. The 100M ClickBench load exhausted an 8 GiB arena at 3.6 GB on disk because ~40 %
 * of its leaves were pinned this way.
 *
 * <p>
 * Witnesses: the diagnostic counters (staged &gt; 0 as the precondition; pinned-by-promotion == 0
 * and retried &gt; 0 as the mechanism), the pinned-region size sampled at every flush, and
 * exactness of every value after a COLD reopen plus a second revision — the q20 class of defect
 * must not come back through the carrier route. Two mutation arms prove the guards are
 * load-bearing: with staging off every carrier-bearing leaf pins; with the deferral cap at zero the
 * flush lane pins past the cap and says so.
 */
final class AdoptedOverflowCarrierStagingTest {

  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final String RESOURCE = JsonTestHelper.RESOURCE;
  /** Four nodes per record: 12,000 records span ~48 document leaves, i.e. several combined epochs. */
  private static final int RECORDS = 12_000;
  private static final int CHUNK_BUDGET_BYTES = 64 * 1024;
  private static final int BUILDERS = 3;
  /** Structural pages (trie spine, roots) the pinned region legitimately holds for this corpus. */
  private static final int STRUCTURAL_PIN_BOUND = 32;

  private boolean priorStaging;
  private int priorCap;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    priorStaging = NodeStorageEngineWriter.STAGE_ADOPTED_OVERFLOW_CARRIERS;
    priorCap = NodeStorageEngineWriter.MAX_KVL_FLUSH_DEFERRALS;
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    BulkAdoptionDiagnostics.reset();
    TransactionIntentLog.resetKvlPromotionDiagnostics();
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.STAGE_ADOPTED_OVERFLOW_CARRIERS = priorStaging;
    NodeStorageEngineWriter.MAX_KVL_FLUSH_DEFERRALS = priorCap;
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** The staging lane needs a reclaimable backend and a deterministically closable arena. */
  private static void assumeStagingBackend() {
    assumeTrue(Boolean.parseBoolean(System.getProperty("sirix.commit.preallocated", "true")),
        "carrier staging needs a preallocated (reclaimable) commit backend");
    assumeTrue(SharedArenas.supportsDeterministicClose(),
        "carrier staging needs an arena strategy with deterministic close");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("carriers are staged, the leaf is deferred once, nothing is pinned, and every value survives")
  void stagedCarriersKeepTheLogBoundedAndValuesExact(final VersioningType versioning) {
    assumeStagingBackend();
    final List<String> urls = new ArrayList<>(RECORDS);
    final byte[] corpus = corpus(urls);
    final AtomicInteger maxPinned = new AtomicInteger();
    final AtomicInteger flushes = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("after-flush".equals(site)) {
        flushes.incrementAndGet();
        maxPinned.accumulateAndGet(engineWriter.getLog().pinnedSize(), Math::max);
      }
    };

    load(versioning, corpus);

    // Mechanism engaged, and it did its job.
    assertTrue(flushes.get() >= 2, "the corpus must drive at least two flush epochs, got " + flushes.get());
    assertTrue(BulkAdoptionDiagnostics.carriersStaged() > 0,
        "the corpus produced no overflow carriers — this test would prove nothing");
    assertEquals(0L, BulkAdoptionDiagnostics.carriersUnstaged(),
        "a carrier was left resident by the backend gates; staging is inert on this configuration");
    assertEquals(0L, BulkAdoptionDiagnostics.carriersRefused(), "the staging lane refused a carrier");
    assertEquals(0L, BulkAdoptionDiagnostics.carriersOversized(), "a corpus carrier exceeded a whole batch");
    assertTrue(TransactionIntentLog.kvlPagesRetriedNextEpoch() > 0,
        "no leaf was deferred for its staged carriers — the flush lane never met a pending carrier");
    assertEquals(0L, TransactionIntentLog.kvlPagesPinnedByPromotion(),
        "a leaf with overflow carriers was pinned until final commit — the 100M arena exhaustion");
    assertEquals(0L, BulkAdoptionDiagnostics.kvlPagesPinnedAfterDeferralCap(),
        "a leaf exhausted its deferrals with carriers still pending — the epoch ordering regressed");
    assertTrue(maxPinned.get() <= STRUCTURAL_PIN_BOUND,
        "the pinned region grew past the structural spine: " + maxPinned.get() + " entries");

    // Exactness through the carrier route, cold, then through a second revision.
    assertCorpusExact(urls, "revision 1 (cold reopen)");
    final int rewritten = rewriteEveryNinetySeventhUrl(urls);
    assertTrue(rewritten > 50, "the corpus must offer enough URL records to rewrite");
    assertCorpusExact(urls, "revision 2 (cold reopen)");
  }

  @Test
  @DisplayName("mutation: with staging off every carrier-bearing leaf pins until final commit")
  void withoutStagingLeavesPin() {
    assumeStagingBackend();
    NodeStorageEngineWriter.STAGE_ADOPTED_OVERFLOW_CARRIERS = false;
    final List<String> urls = new ArrayList<>(RECORDS);
    load(VersioningType.FULL, corpus(urls));
    assertTrue(BulkAdoptionDiagnostics.carriersUnstaged() > 0, "the seam did not disable staging");
    assertTrue(TransactionIntentLog.kvlPagesPinnedByPromotion() > 0,
        "with staging off the flush lane must pin carrier-bearing leaves — the guard above is vacuous");
    assertEquals(0L, TransactionIntentLog.kvlPagesRetriedNextEpoch(), "nothing was staged, so nothing may defer");
    assertCorpusExact(urls, "revision 1 without staging (cold reopen)");
  }

  @Test
  @DisplayName("mutation: with the deferral cap at zero pending leaves pin, and the counter says so")
  void withoutDeferralsPendingLeavesPinPastTheCap() {
    assumeStagingBackend();
    NodeStorageEngineWriter.MAX_KVL_FLUSH_DEFERRALS = 0;
    final List<String> urls = new ArrayList<>(RECORDS);
    load(VersioningType.FULL, corpus(urls));
    assertTrue(BulkAdoptionDiagnostics.carriersStaged() > 0, "staging must still happen");
    assertEquals(0L, TransactionIntentLog.kvlPagesRetriedNextEpoch(), "a zero cap must defer nothing");
    assertTrue(BulkAdoptionDiagnostics.kvlPagesPinnedAfterDeferralCap() > 0,
        "pending leaves past the cap must be counted as pinned — the positive arm's zero is vacuous");
    assertCorpusExact(urls, "revision 1 past the cap (cold reopen)");
  }

  // ==== load / verify =========================================================================

  private static void load(final VersioningType versioning, final byte[] corpus) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .versioningApproach(versioning)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), CHUNK_BUDGET_BYTES, BUILDERS);
        wtx.commit();
      }
    }
  }

  /** Rewrite a spread of URL values through the ordinary transaction; returns how many. */
  private static int rewriteEveryNinetySeventhUrl(final List<String> urls) {
    final Random rng = new Random(0xC0FFEEL);
    int rewritten = 0;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      assertTrue(wtx.moveToFirstChild(), "no root array");
      assertTrue(wtx.moveToFirstChild(), "empty root array");
      int record = 0;
      do {
        if (record % 97 == 0) {
          final long recordKey = wtx.getNodeKey();
          moveToField(wtx, "URL");
          final String fresh = payload(rng, 430 + record % 271);
          wtx.setStringValue(fresh);
          urls.set(record, fresh);
          rewritten++;
          assertTrue(wtx.moveTo(recordKey));
        }
        record++;
      } while (wtx.moveToRightSibling());
      wtx.commit();
    }
    return rewritten;
  }

  private static void assertCorpusExact(final List<String> urls, final String where) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), where + ": no root array");
      assertTrue(rtx.moveToFirstChild(), where + ": empty root array");
      int record = 0;
      int urlNameKey = Integer.MIN_VALUE;
      final long maxNodeKey = rtx.getMaxNodeKey();
      final int pages = (int) ((maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1);
      final int[] truthPerPage = new int[pages];
      do {
        final long recordKey = rtx.getNodeKey();
        moveToField(rtx, "URL");
        urlNameKey = rtx.getNameKey();
        truthPerPage[(int) (rtx.getNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT)]++;
        String value = rtx.getValue();
        if ((value == null || value.isEmpty()) && rtx.moveToFirstChild()) {
          value = rtx.getValue();
        }
        assertEquals(urls.get(record), value, where + ": URL of record " + record + " differs");
        record++;
        assertTrue(rtx.moveTo(recordKey));
      } while (rtx.moveToRightSibling());
      assertEquals(RECORDS, record, where + ": record count");

      // The q20 defect class: every URL slot must be visible to the name-key slot index, whether it
      // is inline or a carrier descriptor.
      final var reader = rtx.getStorageEngineReader();
      final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, rtx.getRevisionNumber());
      long indexed = 0;
      long descriptors = 0;
      for (int pk = 0; pk < pages; pk++) {
        final var res = reader.getRecordPage(key.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) {
          assertEquals(0, truthPerPage[pk], where + ": page " + pk + " unreadable but holds URL records");
          continue;
        }
        final int[] slots = kv.getObjectKeySlotsForNameKey(urlNameKey);
        assertEquals(truthPerPage[pk], slots.length,
            where + ": page " + pk + " slot index disagrees with the document");
        indexed += slots.length;
        for (final int slot : slots) {
          if (kv.isFusedOverflowDescriptor(slot)) {
            descriptors++;
          }
        }
      }
      assertEquals(RECORDS, indexed, where + ": slot-index census");
      assertTrue(descriptors > 0, where + ": no overflow descriptor is visible — the carrier route was not exercised");
    }
  }

  private static void moveToField(final JsonNodeReadOnlyTrx trx, final String fieldName) {
    assertTrue(trx.moveToFirstChild(), "record without fields");
    do {
      final var name = trx.getName();
      if (name != null && fieldName.equals(name.getLocalName())) {
        return;
      }
    } while (trx.moveToRightSibling());
    throw new AssertionError("record has no field " + fieldName);
  }

  // ==== corpus ================================================================================

  /**
   * URL payloads sweep {@code cap - 82 .. cap + 188} bytes around {@link Constants#MAX_RECORD_SIZE}:
   * both sides of the fused inline cap, so inline and carrier records share leaves. Sized from the
   * constant, not a literal, so a cap change moves the sweep with it and the positive-witness guard
   * ("the corpus produced no overflow carriers") keeps meaning what it says. Uniform over 92
   * printable characters so FSST declines them and the bands hold (a hex alphabet would be halved and
   * every value would silently fit inline).
   */
  private static byte[] corpus(final List<String> urls) {
    final Random rng = new Random(0x5711D5EEDL);
    final StringBuilder sb = new StringBuilder(RECORDS * (Constants.MAX_RECORD_SIZE + 108));
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final String url = payload(rng, Constants.MAX_RECORD_SIZE - 82 + i % 271);
      urls.add(url);
      sb.append("{\"id\":")
        .append(i)
        .append(",\"note\":")
        .append(i % 41)
        .append(",\"URL\":\"")
        .append(url)
        .append("\"}");
    }
    sb.append(']');
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String payload(final Random rng, final int len) {
    final StringBuilder sb = new StringBuilder(len);
    while (sb.length() < len) {
      final char c = (char) (33 + rng.nextInt(94));
      if (c == '"' || c == '\\') {
        continue;
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
