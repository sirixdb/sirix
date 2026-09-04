/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;


import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Splitting the verdict sweep across bucket ranges, which is what lets it run on more than one
 * thread.
 *
 * <h2>The property, and why a small corpus cannot test it</h2>
 *
 * <p>
 * Ids are 1-based and bucketed by {@code (id - 1) >>> 8}, so bucket {@code b} owns ids
 * {@code 256b+1 .. 256b+256} and therefore verdict WORDS {@code 4b .. 4b+4} — five words, the last
 * of which is bucket {@code b+1}'s first. Adjacent buckets SHARE a boundary word. A lane that OR-ed
 * into one shared array would lose the other lane's bits there, dropping matches at one id in 256;
 * the existing verdict test interns a dozen values, occupies a single bucket, and could never see
 * it.
 * </p>
 *
 * <p>
 * So this corpus spans several buckets and asserts that any split, merged, is bit-for-bit the whole
 * sweep — with the ids either side of every boundary checked by name.
 * </p>
 */
final class GlobalValueDictionaryVerdictRangeTest {

  private static final String RESOURCE = "verdictRange";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Enough values for four buckets and a partial fifth, so boundaries are interior to the sweep. */
  private static final int VALUE_COUNT = 1100;

  private static List<String> corpus() {
    final List<String> values = new ArrayList<>(VALUE_COUNT);
    for (int i = 0; i < VALUE_COUNT; i++) {
      // Every 7th value contains the needle, so matches straddle every bucket boundary rather than
      // clustering where a split happens to fall.
      values.add(i % 7 == 0
          ? "row-" + i + "-google-suffix"
          : "row-" + i + "-plain");
    }
    return values;
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static long buildDictionary(final List<String> values) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
    }
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (final String value : values) {
        final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
      return headerKey;
    }
  }

  private interface ViewTask {
    void run(GlobalValueDictionary.ReadView view);
  }

  private static void withView(final long headerKey, final ViewTask task) {
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = db.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final GlobalValueDictionary.ReadView view =
          GlobalValueDictionary.readView(headerKey, rtx.getStorageEngineReader());
      assertNotNull(view, "dictionary must be readable");
      task.run(view);
    }
  }

  /**
   * The whole sweep, split at {@code lanes} bucket boundaries and merged.
   *
   * <p>
   * Every number here comes from {@link GlobalValueDictionary.VerdictSlice}, which is the SAME code
   * the executor's parallel sweep splits and merges through. A copy of the slice sizing, the word
   * base or the merge clamp in this file would agree with itself while the shipped arithmetic drifted
   * — and drift in exactly those three numbers is the dropped row this class exists to catch, so
   * there is no copy.
   * </p>
   */
  private static long[] split(final GlobalValueDictionary.ReadView view, final ProjectionIndexScan.Op op,
      final byte[] literal, final int lanes) {
    final long[] merged = view.newVerdict();
    for (int lane = 0; lane < lanes; lane++) {
      final GlobalValueDictionary.VerdictSlice slice = view.verdictSlice(lane, lanes);
      slice.fill(view, op, literal);
      slice.mergeInto(merged);
    }
    return merged;
  }

  @Test
  @DisplayName("the corpus really does span several buckets, or the rest of this class proves nothing")
  void theCorpusSpansBuckets() {
    final long headerKey = buildDictionary(corpus());
    withView(headerKey, view -> {
      assertEquals(VALUE_COUNT, view.entryCount());
      assertTrue(view.verdictBucketCount() >= 4,
          "need several buckets for a boundary to exist, got " + view.verdictBucketCount());
    });
  }

  @Test
  @DisplayName("ANY split, merged, is bit-for-bit the single-threaded sweep")
  void everySplitReproducesTheWholeSweep() {
    final List<String> values = corpus();
    final long headerKey = buildDictionary(values);
    final byte[] needle = "google".getBytes(StandardCharsets.UTF_8);
    withView(headerKey, view -> {
      for (final ProjectionIndexScan.Op op : List.of(ProjectionIndexScan.Op.STR_CONTAINS, ProjectionIndexScan.Op.EQ,
          ProjectionIndexScan.Op.NE, ProjectionIndexScan.Op.STR_LT, ProjectionIndexScan.Op.STR_GE)) {
        final long[] whole = view.stringOpVerdict(op, needle);
        for (final int lanes : new int[] {1, 2, 3, 4, 5, 8, 17}) {
          assertArrayEquals(whole, split(view, op, needle, lanes),
              op + " split into " + lanes + " lanes must equal the whole sweep");
        }
      }
    });
  }

  @Test
  @DisplayName("THE BOUNDARY: the ids either side of every bucket edge survive a split")
  void boundaryIdsSurviveASplit() {
    final List<String> values = corpus();
    final long headerKey = buildDictionary(values);
    final byte[] needle = "google".getBytes(StandardCharsets.UTF_8);
    withView(headerKey, view -> {
      final long[] merged = split(view, ProjectionIndexScan.Op.STR_CONTAINS, needle, 4);
      int checked = 0;
      for (int bucket = 1; bucket < view.verdictBucketCount(); bucket++) {
        // id 256*bucket is the LAST of the previous bucket, 256*bucket+1 the first of this one, and
        // they share verdict word 4*bucket. This is the pair a lost update would corrupt.
        for (final int id : new int[] {bucket << 8, (bucket << 8) + 1}) {
          if (id < 1 || id > VALUE_COUNT) {
            continue;
          }
          final boolean expected = values.get(id - 1).contains("google");
          final boolean actual = (merged[id >>> 6] & 1L << (id & 63)) != 0L;
          assertEquals(expected, actual, "boundary id " + id + " (" + values.get(id - 1) + ")");
          checked++;
        }
      }
      assertTrue(checked >= 6, "expected several boundary ids, checked " + checked);
    });
  }

  @Test
  @DisplayName("a range outside the dictionary is refused rather than read past")
  void anInvalidRangeIsRefused() {
    final long headerKey = buildDictionary(corpus());
    withView(headerKey, view -> {
      final byte[] needle = "google".getBytes(StandardCharsets.UTF_8);
      final int buckets = view.verdictBucketCount();
      final long[] out = new long[(buckets << 2) + 1];
      assertThrows(IllegalArgumentException.class,
          () -> view.fillStringOpVerdict(ProjectionIndexScan.Op.STR_CONTAINS, needle, -1, buckets, out, 0));
      assertThrows(IllegalArgumentException.class,
          () -> view.fillStringOpVerdict(ProjectionIndexScan.Op.STR_CONTAINS, needle, 0, buckets + 1, out, 0));
      assertThrows(IllegalArgumentException.class,
          () -> view.fillStringOpVerdict(ProjectionIndexScan.Op.STR_CONTAINS, needle, 2, 1, out, 0));
      // An empty range is legal and writes nothing.
      view.fillStringOpVerdict(ProjectionIndexScan.Op.STR_CONTAINS, needle, 1, 1, out, 4);
      for (final long word : out) {
        assertEquals(0L, word, "an empty range must write nothing");
      }
    });
  }
}
