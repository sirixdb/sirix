/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every fused named record a page holds must be visible through the page's own name-key slot index
 * — whatever the record's size made of it.
 *
 * <h2>The four size bands</h2>
 *
 * A fused string record's fate is decided against {@link PageConstants#MAX_RECORD_SIZE} twice: once
 * on the padded ceiling estimate, once on the actual serialized bytes. That yields four bands:
 *
 * <ol>
 * <li><b>Small</b> — ceiling under the cap: fused inline, always worked.</li>
 * <li><b>False-refusal band</b> — ceiling over the cap, actual under it. Before the floor-keyed
 * refusal these records were re-serialized generically at commit under the raw-record sentinel kind
 * (0): readable through the cursor, absent from the name-key region, absent from
 * {@link KeyValueLeafPage#getObjectKeySlotsForNameKey}, and therefore invisible to every anchored
 * scan. On the ClickBench hits corpus this band held 6146 of 1,000,000 records — q20 answered 94
 * where DuckDB and the record path answered 95.</li>
 * <li><b>Fused-overflow band</b> — fused actual over the cap, generic actual under it. These
 * records must take the overflow-carrier route (inline fused descriptor + OverflowPage), never the
 * generic-inline lane; 1428 hits records sat here after the floor fix alone.</li>
 * <li><b>True overflow</b> — over the cap in every form: descriptor + OverflowPage, as always.</li>
 * </ol>
 *
 * <p>
 * The corpus here sweeps payloads across all four bands with HIGH-ENTROPY values (hex from a fixed
 * seed), so FSST cannot compress a value out of its band and quietly shrink the test's coverage.
 */
final class FusedRecordDirectoryKindCompletenessTest {

  private static final String RESOURCE = "fusedKindCompleteness";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Records per corpus; six nodes each, so the corpus spans several document pages. */
  private static final int RECORDS = 2_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * High-entropy payload of exactly {@code len} ASCII bytes; deterministic across runs.
   *
   * <p>
   * Uniform over 92 printable characters, NOT hex: a 16-symbol alphabet carries four bits per byte
   * and FSST halves it at commit, silently moving every value out of the size band it was built to
   * exercise — the first version of this corpus passed its mutation check vacuously exactly that way.
   * At ~6.5 bits per byte FSST declines these values and the bands hold.
   */
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

  private static String corpus() {
    // Sweep 400..599 payload bytes: crosses the false-refusal band (~433..485), the fused-overflow
    // band (~486..512+) and, with the +3000 spikes, the true-overflow band.
    final Random rng = new Random(0x5151C5EEDL);
    final StringBuilder sb = new StringBuilder(RECORDS * 540);
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int len = i % 97 == 0
          ? 3_000
          : 400 + i % 200;
      sb.append("{\"id\":")
        .append(i)
        .append(",\"note\":")
        .append(i % 41)
        .append(",\"URL\":\"")
        .append(payload(rng, len))
        .append("\"}");
    }
    sb.append(']');
    return sb.toString();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("the slot index reports every fused record the document holds, in every size band")
  void slotIndexIsCensusExactAcrossAllSizeBands(final VersioningType versioning) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(corpus()), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    // COLD reopen: the census must hold against what was persisted, not against writer state.
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertCensusExact(rtx, "revision 1");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("a second-revision value update into the refusal bands stays census-exact")
  void updatedValuesInLaterRevisionsStayVisible(final VersioningType versioning) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(corpus()), JsonNodeTrx.Commit.NO);
      wtx.commit();
    }
    // Revision 2: rewrite a spread of URL values INTO the bands, through the mutation path — the
    // fragment-combining read of a later revision must see them exactly like a fresh shred.
    final Random rng = new Random(0xBEEFCAFEL);
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      int updated = 0;
      final long maxNodeKey = wtx.getMaxNodeKey();
      for (long nk = 1; nk <= maxNodeKey && updated < 120; nk++) {
        if (!wtx.moveTo(nk) || !wtx.isObjectKey()) {
          continue;
        }
        final var name = wtx.getName();
        if (name == null || !"URL".equals(name.getLocalName())) {
          continue;
        }
        if (updated % 3 != 0) {
          updated++;
          continue;
        }
        // Phase 4: every object key is a fused OBJECT_NAMED_* record carrying its value inline, so
        // the value write happens on the named node itself.
        wtx.setStringValue(payload(rng, 440 + updated % 90));
        updated++;
      }
      assertTrue(updated >= 120, "the corpus must offer enough URL records to update");
      wtx.commit();
    }
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertCensusExact(rtx, "revision 2 (cold reopen)");
    }
  }

  /**
   * The census: for every document leaf page, {@link KeyValueLeafPage#getObjectKeySlotsForNameKey}
   * must report exactly the URL slots the cursor walk finds. The band assertions keep the test honest
   * — a corpus change that stopped exercising a band would fail here, not silently pass.
   */
  private static void assertCensusExact(final JsonNodeReadOnlyTrx rtx, final String where) {
    int urlNameKey = Integer.MIN_VALUE;
    long truthTotal = 0;
    long bandFalseRefusal = 0;
    long bandBeyondInline = 0;
    final long maxNodeKey = rtx.getMaxNodeKey();
    final int pages = (int) ((maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1);
    final int[] truthPerPage = new int[pages];
    for (long nk = 1; nk <= maxNodeKey; nk++) {
      if (!rtx.moveTo(nk) || !rtx.isObjectKey()) {
        continue;
      }
      final var name = rtx.getName();
      if (name == null || !"URL".equals(name.getLocalName())) {
        continue;
      }
      urlNameKey = rtx.getNameKey();
      truthTotal++;
      truthPerPage[(int) (nk >>> Constants.INP_REFERENCE_COUNT_EXPONENT)]++;
      // A fused OBJECT_NAMED_STRING answers getValue() itself and has no child; the parallel bulk
      // lane's representation reads through the first child. Accept either.
      String value = rtx.getValue();
      if ((value == null || value.isEmpty()) && rtx.moveToFirstChild()) {
        value = rtx.getValue();
      }
      assertTrue(value != null && !value.isEmpty(), where + ": URL record " + nk + " has no readable value");
      final int len = value.length();
      if (len >= 433 && len <= 485) {
        bandFalseRefusal++;
      } else if (len > 485) {
        bandBeyondInline++;
      }
    }
    assertTrue(truthTotal > 0, where + ": corpus lost its URL records entirely");
    // Non-vacuity: the corpus must actually exercise the two bands the regression lived in.
    assertTrue(bandFalseRefusal > 50, where + ": corpus no longer exercises the false-refusal band");
    assertTrue(bandBeyondInline > 50, where + ": corpus no longer exercises the beyond-inline bands");

    final var reader = rtx.getStorageEngineReader();
    final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, rtx.getRevisionNumber());
    long indexTotal = 0;
    long inlineFalseRefusalBand = 0;
    long descriptorFalseRefusalBand = 0;
    long descriptorsSeen = 0;
    long sideSlots = 0;
    for (int pk = 0; pk < pages; pk++) {
      final var res = reader.getRecordPage(key.setRecordPageKey(pk));
      if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) {
        assertEquals(0, truthPerPage[pk], where + ": page " + pk + " is unreadable but holds URL records");
        continue;
      }
      sideSlots += kv.getSideSlotCount();
      final int[] slots = kv.getObjectKeySlotsForNameKey(urlNameKey);
      assertEquals(truthPerPage[pk], slots.length,
          where + ": page " + pk + " slot index disagrees with the document — a record's size band "
              + "demoted it out of the fused directory kind");
      indexTotal += slots.length;
      final long base = (long) pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
      for (final int slot : slots) {
        final boolean descriptor = kv.isFusedOverflowDescriptor(slot);
        if (descriptor) {
          descriptorsSeen++;
        }
        assertTrue(rtx.moveTo(base + slot), where + ": indexed slot " + slot + " unreadable");
        final String v = rtx.getValue();
        final int len = v == null
            ? 0
            : v.length();
        // The heart of the storage half of the fix: a record in the false-refusal band fits the
        // fused inline format, and the floor-keyed refusal must therefore KEEP it inline. Routing
        // it out of line through the overflow carrier would still pass the census above — the
        // descriptor stays visible — but silently trade an inline read for a page indirection on
        // every one of these records. The band is clipped at 470 because a payload above that can
        // legitimately overflow once its actual metadata is added.
        if (len >= 433 && len <= 470) {
          if (descriptor) {
            descriptorFalseRefusalBand++;
          } else {
            inlineFalseRefusalBand++;
          }
        }
      }
    }
    assertEquals(truthTotal, indexTotal, where + ": total census mismatch");
    assertTrue(inlineFalseRefusalBand > 50,
        where + ": the false-refusal band no longer serializes INLINE — its records reach the reader "
            + "as overflow descriptors, paying a page indirection the fused format does not require");
    assertEquals(0, descriptorFalseRefusalBand,
        where + ": records small enough for the fused inline format were routed out of line");
    assertTrue(descriptorsSeen > 0,
        where + ": no overflow descriptor visible at all — the beyond-inline bands are not exercised");
    // No record in this corpus legitimately needs the overflow-slot SIDECAR: every value either
    // fits the fused inline format or takes an inline descriptor beside an OverflowPage. A side
    // slot appearing here means an inline-capable record was refused into it — census-invisible it
    // is not, but the page then refuses to emit ANY PAX region, so every column scan over it falls
    // back to the record heap. That is the silent cost a ceiling-keyed refusal reintroduces.
    assertEquals(0, sideSlots, where + ": inline-capable records were pushed into the overflow-slot sidecar");
  }
}
