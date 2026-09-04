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
import io.sirix.node.ValueDictionaryValueBucketNode;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Incremental append must EXTEND the open tail run, not open a new one per revision.
 *
 * <p>
 * Without tail copy-on-write, a resource that adds one dictionary value per revision grows one
 * directory run per revision: after 300 revisions a single 256-id bucket would describe its values
 * with hundreds of one-value runs. That is a storage-efficiency defect AND a design violation — the
 * agreed rule is that only the still-open tail is ever rewritten.
 *
 * <p>
 * These tests pin the rule from both sides: the run count stays bounded as revisions accumulate,
 * and every completed run keeps the key it was first written under, so older revisions keep reading
 * the records they always addressed.
 */
final class GlobalValueDictionaryTailCowTest {

  private static final String RESOURCE = "tailCowResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Enough one-value revisions to cross a sub-block and approach the 256-id bucket boundary. */
  private static final int REVISIONS = 300;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String valueOf(final int id) {
    return "tail-value-" + id;
  }

  private record Snapshot(long headerKey, int entryCount, List<Long> blockKeys, int runCount) {
  }

  /**
   * One revision, one new value. The FIRST creates the dictionary; every later one APPENDS to the
   * existing header, which is the path tail copy-on-write exists for.
   */
  private static long appendOne(final JsonResourceSession session, final int id, final long headerKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      if (id == 1) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      }
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      final byte[] utf8 = valueOf(id).getBytes(StandardCharsets.UTF_8);
      dictionary.intern(utf8, 0, utf8.length);
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long resulting;
      if (id == 1) {
        resulting = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      } else {
        dictionary.flushAppend(GlobalValueDictionary.header(headerKey, writer), namePage, DatabaseType.JSON, writer,
            writer.getLog());
        resulting = headerKey;
      }
      wtx.commit();
      return resulting;
    }
  }

  private static Snapshot inspect(final JsonResourceSession session, final long headerKey, final int entryCount) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = rtx.getStorageEngineReader();
      final var header = GlobalValueDictionary.header(headerKey, reader);
      assertNotNull(header, "header must be readable");
      final ValueDictionaryValueBucketNode bucket = GlobalValueDictionaryRadix.valueBucketOf(header.getReverseRootKey(),
          0, reader.getNamePage(reader.getActualRevisionRootPage()), DatabaseType.JSON, reader);
      assertNotNull(bucket, "bucket 0 must exist");
      final List<Long> keys = new ArrayList<>();
      for (int i = 0; i < bucket.blockCount(); i++) {
        keys.add(bucket.blockKey(i));
      }
      return new Snapshot(headerKey, entryCount, keys, bucket.blockCount() + bucket.spillCount());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("one-value revisions extend the open tail: runs stay bounded and completed keys never move")
  void oneValueRevisionsExtendTheOpenTail(final VersioningType versioning) {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
    final List<Long> headerKeys = new ArrayList<>();
    final List<Integer> runCounts = new ArrayList<>();
    final Set<Long> everCompleted = new LinkedHashSet<>();

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      long headerKey = 0L;
      for (int id = 1; id <= REVISIONS; id++) {
        headerKey = appendOne(session, id, headerKey);
        headerKeys.add(headerKey);
        final Snapshot now = inspect(session, headerKey, id);
        runCounts.add(now.runCount());
        // Every key EXCEPT the current tail is completed and must never change again.
        final List<Long> keys = now.blockKeys();
        for (int i = 0; i < keys.size() - 1; i++) {
          everCompleted.add(keys.get(i));
        }
        if (keys.size() > 1) {
          final List<Long> completed = keys.subList(0, keys.size() - 1);
          assertTrue(everCompleted.containsAll(completed),
              "a completed block key changed at revision " + id + ": " + completed);
        }
      }

      // BOUNDED: without tail COW this is one run per revision. With it, a run closes only when the
      // block fills, so the count is governed by byte capacity, not by revision count.
      final int finalRuns = runCounts.get(runCounts.size() - 1);
      assertTrue(finalRuns < REVISIONS / 4, "directory grew " + finalRuns + " runs over " + REVISIONS
          + " one-value revisions — the open tail is not being extended");

      // HISTORY: every revision still resolves every id it knew, through its own header.
      for (int revision = 1; revision <= REVISIONS; revision++) {
        try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final GlobalValueDictionary.ReadView view =
              GlobalValueDictionary.readView(headerKeys.get(revision - 1), reader);
          assertNotNull(view, "revision " + revision + " must expose its dictionary");
          assertEquals(revision, view.entryCount(), "revision " + revision + " cardinality");
          for (int id = 1; id <= revision; id++) {
            assertEquals(valueOf(id), GlobalValueDictionary.value(headerKeys.get(revision - 1), id, reader),
                "revision " + revision + " lost id " + id);
          }
        }
      }
    }
  }

  /** Append one arbitrary value, returning the (possibly new) header key. */
  private static long appendValue(final JsonResourceSession session, final String value, final long headerKey,
      final boolean create) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      if (create) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
      }
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
      dictionary.intern(utf8, 0, utf8.length);
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long resulting;
      if (create) {
        resulting = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      } else {
        dictionary.flushAppend(GlobalValueDictionary.header(headerKey, writer), namePage, DatabaseType.JSON, writer,
            writer.getLog());
        resulting = headerKey;
      }
      wtx.commit();
      return resulting;
    }
  }

  /** A distinct value whose UTF-8 length is EXACTLY 1024 bytes. */
  private static String exactKilobyte(final int index) {
    final String head = "v" + index + "-";
    return head + "y".repeat(1024 - head.length());
  }

  private static ValueDictionaryValueBucketNode bucketZero(final JsonNodeReadOnlyTrx rtx, final long headerKey) {
    final var reader = rtx.getStorageEngineReader();
    final var header = GlobalValueDictionary.header(headerKey, reader);
    assertNotNull(header);
    return GlobalValueDictionaryRadix.valueBucketOf(header.getReverseRootKey(), 0,
        reader.getNamePage(reader.getActualRevisionRootPage()), DatabaseType.JSON, reader);
  }

  private static void createResource(final VersioningType versioning) {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("a SPILLED tail is not extended: the next append opens a new block")
  void spilledTailFallsBackToANewBlock(final VersioningType versioning) {
    createResource(versioning);
    // An individually oversized value cannot be packed, so it takes the spill lane and there is no
    // open run to extend.
    final String oversized = "x".repeat(io.sirix.node.ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES + 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      long headerKey = appendValue(session, "first", 0L, true);
      headerKey = appendValue(session, oversized, headerKey, false);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        assertEquals(1, bucket.spillCount(), "the oversized value must spill");
        assertEquals(2, bucket.spillId(0), "and it is the last id");
      }
      final long before = headerKey;
      headerKey = appendValue(session, "after-spill", headerKey, false);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        // A new run for id 3, and the spill plus the original block both survive untouched.
        assertEquals(2, bucket.blockCount(), "a spilled tail must not be extended — a new block opens");
        assertEquals(1, bucket.spillCount());
        final var reader = rtx.getStorageEngineReader();
        assertEquals("first", GlobalValueDictionary.value(headerKey, 1, reader));
        assertEquals(oversized, GlobalValueDictionary.value(headerKey, 2, reader), "the spilled value survives");
        assertEquals("after-spill", GlobalValueDictionary.value(headerKey, 3, reader));
      }
      assertEquals(before, headerKey, "appends reuse the header");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("a BYTE-FULL tail is not extended: the next append opens a new block and reuses its key")
  void fullTailFallsBackToANewBlockAndKeepsTheOldKey(final VersioningType versioning) {
    createResource(versioning);
    // EXACTLY 1024 bytes each, so 64 of them reach MAX_BLOCK_BYTES precisely and the tail is full by
    // BYTES rather than by count. Values of merely ~1 KiB close the block one value early and leave
    // a tail that is NOT full — which is what a first version of this test actually measured.
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      long headerKey = appendValue(session, exactKilobyte(0), 0L, true);
      for (int i = 1; i < 64; i++) {
        headerKey = appendValue(session, exactKilobyte(i), headerKey, false);
      }
      final long fullTailKey;
      final int runsWhenFull;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        runsWhenFull = bucket.blockCount();
        fullTailKey = bucket.blockKey(bucket.blockCount() - 1);
      }
      headerKey = appendValue(session, "after-full", headerKey, false);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        assertEquals(runsWhenFull + 1, bucket.blockCount(), "a full tail must not be extended");
        boolean keptOldKey = false;
        for (int i = 0; i < bucket.blockCount(); i++) {
          keptOldKey |= bucket.blockKey(i) == fullTailKey;
        }
        assertTrue(keptOldKey, "a full tail is COMPLETED, so its key must be reused unchanged");
        final var reader = rtx.getStorageEngineReader();
        for (int i = 0; i < 64; i++) {
          assertEquals(exactKilobyte(i), GlobalValueDictionary.value(headerKey, i + 1, reader), "value " + i);
        }
        assertEquals("after-full", GlobalValueDictionary.value(headerKey, 65, reader));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("a tail extension replaces exactly one run, and every record stays reachable")
  void tailExtensionReplacesExactlyOneRunAndStaysReachable(final VersioningType versioning) {
    createResource(versioning);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      long headerKey = appendValue(session, valueOf(1), 0L, true);
      headerKey = appendValue(session, valueOf(2), headerKey, false);
      final long tailKeyBefore;
      final int runsBefore;
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        runsBefore = bucket.blockCount();
        tailKeyBefore = bucket.blockKey(bucket.blockCount() - 1);
      }
      headerKey = appendValue(session, valueOf(3), headerKey, false);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ValueDictionaryValueBucketNode bucket = bucketZero(rtx, headerKey);
        // EXACTLY one run replaced: the count is unchanged and the superseded key is gone.
        assertEquals(runsBefore, bucket.blockCount(), "an extension must not add a run");
        for (int i = 0; i < bucket.blockCount(); i++) {
          assertTrue(bucket.blockKey(i) != tailKeyBefore, "the superseded tail key must leave the directory");
        }
        final var reader = rtx.getStorageEngineReader();
        for (int id = 1; id <= 3; id++) {
          assertEquals(valueOf(id), GlobalValueDictionary.value(headerKey, id, reader), "id " + id);
        }
      }
      // The OLD revision still reads its own two ids through the tail it was written with.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final var reader = rtx.getStorageEngineReader();
        final GlobalValueDictionary.ReadView view = GlobalValueDictionary.readView(headerKey, reader);
        assertNotNull(view);
        assertEquals(2, view.entryCount(), "revision 2 keeps its own cardinality");
        assertEquals(valueOf(1), GlobalValueDictionary.value(headerKey, 1, reader));
        assertEquals(valueOf(2), GlobalValueDictionary.value(headerKey, 2, reader));
      }
    }
  }
}
