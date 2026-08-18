/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.GlobalValueDictionaryWriter;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The global projection value dictionary is a new sub-trie inside {@link NamePage}, and everything
 * downstream of it — integer group-by, integer distinct folds, integer equality — is only sound if
 * an id keeps meaning the same value for as long as any row group referring to it can be read.
 *
 * <p>These tests are about that, not about the in-memory intern table. Four properties carry the
 * design:
 *
 * <ul>
 * <li><b>The offset run stays gapless.</b> {@link NamePage} serializes its bookkeeping positionally,
 * so occupying offset 2 on a resource whose offset 1 was never used would make the next commit
 * throw. A resource that never enabled FSST is therefore the interesting case, not the boring
 * one.</li>
 * <li><b>Keys are addressable.</b> The sub-trie's indirect-page traversal only grows a level when a
 * densely allocated page key crosses a power-of-two boundary, so a dictionary spanning many record
 * pages is the case that catches a key layout the trie cannot address — one that would otherwise
 * resolve every page to the root reference and let records overwrite each other silently.</li>
 * <li><b>Revisions are self-consistent.</b> A rebuild re-mints ids from 1; that is safe only
 * because copy-on-write keeps the earlier revision reading the dictionary it was built against.</li>
 * <li><b>"Absent" and "cannot say" are different answers.</b> A probe that reported a value missing
 * when it merely could not see it would turn a fast path into a wrong answer.</li>
 * </ul>
 */
@DisplayName("the global projection value dictionary is a versioned sub-trie")
public final class GlobalValueDictionaryStoreTest {

  private static final String RESOURCE_NAME = "valueDictionaryResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * The gapless-run case. This resource stores no FSST table, so its NamePage holds offset 0 alone;
   * writing the dictionary at offset 2 has to root offset 1 as well or the commit throws.
   */
  @Test
  @DisplayName("a dictionary commits on a resource that never used FSST")
  void dictionaryCommitsWithoutAnFsstTable() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "alpha");
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals("alpha", GlobalValueDictionary.value(header, 1, rtx.getStorageEngineReader()));
      }
    }
  }

  /**
   * Enough values to span many record pages and many directory blocks. This is the test that fails
   * loudly if the key layout is one the indirect-page trie cannot address.
   */
  @Test
  @DisplayName("both directions round-trip across a commit at multi-page scale")
  void bothDirectionsRoundTripAcrossACommit() {
    final int count = 4000;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < count; i++) {
          assertEquals(i + 1, intern(writer, value(i)), "ids must be minted densely from 1 in first-seen order");
        }
        // Re-interning must return the same id rather than mint a second one.
        for (int i = 0; i < count; i += 97) {
          assertEquals(i + 1, intern(writer, value(i)));
        }
        assertEquals(count, writer.entryCount());
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        for (int i = 0; i < count; i++) {
          assertEquals(value(i), GlobalValueDictionary.value(header, i + 1, reader),
              "reverse lookup returned the wrong value for id " + (i + 1));
          assertEquals(i + 1, GlobalValueDictionary.probe(header, utf8(value(i)), reader),
              "forward probe returned the wrong id for " + value(i));
        }
      }
    }
  }

  @Test
  @DisplayName("a value the dictionary does not hold probes as absent, not as unknown")
  void absentValueProbesAsAbsent() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < 500; i++) {
          intern(writer, value(i));
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(GlobalValueDictionary.ID_ABSENT,
            GlobalValueDictionary.probe(header, utf8("nothing-like-this"), rtx.getStorageEngineReader()));
      }
    }
  }

  /** A dictionary nothing ever wrote must decline, never claim the value is absent. */
  @Test
  @DisplayName("an unwritten dictionary declines rather than reporting absence")
  void unwrittenDictionaryDeclines() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals(GlobalValueDictionary.ID_UNKNOWN, GlobalValueDictionary.probe(17L, utf8("alpha"), reader));
        assertNull(GlobalValueDictionary.value(17L, 1, reader));
        // "No dictionary at all" must also decline rather than throw.
        assertEquals(GlobalValueDictionary.ID_UNKNOWN, GlobalValueDictionary.probe(0L, utf8("alpha"), reader));
      }
    }
  }

  /**
   * The property the whole scheme rests on: a rebuild re-mints ids from 1, so id 1 means one thing
   * in revision 1 and another in revision 2. That is only safe because each revision reads the
   * dictionary it was built against.
   */
  @Test
  @DisplayName("a rebuild in a later revision leaves the earlier revision's mapping intact")
  void rebuildDoesNotDisturbEarlierRevisions() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerOne;
      final long headerTwo;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "first-revision-a");
        intern(writer, "first-revision-b");
        headerOne = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        intern(writer, "second-revision-a");
        intern(writer, "second-revision-b");
        intern(writer, "second-revision-c");
        headerTwo = flush(wtx, writer);
        wtx.commit();
      }
      assertNotEquals(headerOne, headerTwo,
          "a rebuild reused the previous run's keys, so the two dictionaries share records");

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("first-revision-a", GlobalValueDictionary.value(headerOne, 1, reader),
            "revision 1's id 1 was rewritten by revision 2's rebuild — every row group committed in "
                + "revision 1 now means something else");
        assertEquals("first-revision-b", GlobalValueDictionary.value(headerOne, 2, reader));
        assertEquals(1, GlobalValueDictionary.probe(headerOne, utf8("first-revision-a"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT,
            GlobalValueDictionary.probe(headerOne, utf8("second-revision-a"), reader));
        assertNull(GlobalValueDictionary.value(headerTwo, 1, reader),
            "revision 1 can see a dictionary only revision 2 wrote, so the store is not going "
                + "through the versioned trie");
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("second-revision-a", GlobalValueDictionary.value(headerTwo, 1, reader));
        assertEquals("second-revision-c", GlobalValueDictionary.value(headerTwo, 3, reader));
        assertEquals(3, GlobalValueDictionary.probe(headerTwo, utf8("second-revision-c"), reader));
        // The earlier revision's dictionary is still readable from the later revision, unchanged.
        assertEquals("first-revision-a", GlobalValueDictionary.value(headerOne, 1, reader));
      }
    }
  }

  /** Two dictionaries written in one transaction must not see each other's values. */
  @Test
  @DisplayName("dictionaries are isolated inside the one sub-trie")
  void dictionariesAreIsolated() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerA;
      final long headerB;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter a = new GlobalValueDictionaryWriter();
        intern(a, "shared-value");
        intern(a, "only-in-a");
        headerA = flush(wtx, a);
        final GlobalValueDictionaryWriter b = new GlobalValueDictionaryWriter();
        intern(b, "only-in-b");
        intern(b, "shared-value");
        headerB = flush(wtx, b);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        // The same string holds a different id in each dictionary, and each answers only for itself.
        assertEquals(1, GlobalValueDictionary.probe(headerA, utf8("shared-value"), reader));
        assertEquals(2, GlobalValueDictionary.probe(headerB, utf8("shared-value"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(headerA, utf8("only-in-b"), reader));
        assertEquals(GlobalValueDictionary.ID_ABSENT, GlobalValueDictionary.probe(headerB, utf8("only-in-a"), reader));
        assertEquals("only-in-a", GlobalValueDictionary.value(headerA, 2, reader));
        assertEquals("only-in-b", GlobalValueDictionary.value(headerB, 1, reader));
      }
    }
  }

  /** The batch reverse lookup must agree with the single-id one, order preserved. */
  @Test
  @DisplayName("batch reverse lookup preserves the caller's order")
  void batchReverseLookupPreservesOrder() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < 1000; i++) {
          intern(writer, value(i));
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        final int[] ids = { 900, 3, 512, 1, 777, 0 };
        final String[] values = GlobalValueDictionary.values(header, ids, reader);
        assertEquals(ids.length, values.length);
        for (int i = 0; i < ids.length; i++) {
          if (ids[i] == 0) {
            assertNull(values[i], "id 0 means 'no id' and must not resolve");
          } else {
            assertEquals(value(ids[i] - 1), values[i], "batch lookup disagreed at slot " + i);
          }
        }
      }
    }
  }

  /**
   * Long values must not overflow a record page's slotted buffer at the layout's density — the
   * ceiling is a hard failure at commit, not a slow path, so the layout has to leave headroom.
   */
  @Test
  @DisplayName("long values survive the record-page layout")
  void longValuesSurviveTheLayout() {
    final int count = 900;
    final String padding = "x".repeat(400);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        for (int i = 0; i < count; i++) {
          intern(writer, padding + "-" + i);
        }
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        for (int i = 0; i < count; i += 41) {
          assertEquals(padding + "-" + i, GlobalValueDictionary.value(header, i + 1, reader));
        }
      }
    }
  }

  /** Values that differ only past a shared prefix, plus the empty string, which is a real value. */
  @Test
  @DisplayName("the empty string and shared-prefix values are distinct entries")
  void emptyAndSharedPrefixValues() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter();
        assertEquals(1, intern(writer, ""));
        assertEquals(2, intern(writer, "prefix"));
        assertEquals(3, intern(writer, "prefixx"));
        assertEquals(1, intern(writer, ""), "the empty string must intern to one id, not a fresh one each time");
        header = flush(wtx, writer);
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertEquals("", GlobalValueDictionary.value(header, 1, reader));
        assertEquals(1, GlobalValueDictionary.probe(header, utf8(""), reader));
        assertEquals(2, GlobalValueDictionary.probe(header, utf8("prefix"), reader));
        assertEquals(3, GlobalValueDictionary.probe(header, utf8("prefixx"), reader));
      }
    }
  }

  /** An empty dictionary is legitimate; it must round-trip and decline every probe. */
  @Test
  @DisplayName("an empty dictionary round-trips and answers nothing")
  void emptyDictionaryRoundTrips() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long header;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        seed(wtx);
        header = flush(wtx, new GlobalValueDictionaryWriter());
        wtx.commit();
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        assertNull(GlobalValueDictionary.value(header, 1, reader));
        // No directory was written, so the probe cannot claim absence.
        assertEquals(GlobalValueDictionary.ID_UNKNOWN, GlobalValueDictionary.probe(header, utf8("x"), reader));
      }
    }
  }

  /** Key arithmetic must keep the three record families disjoint. */
  @Test
  @DisplayName("the key layout keeps the record families disjoint")
  void keyLayoutIsDisjoint() {
    final int entries = 1000;
    final int blocks = 8;
    final long reserved = GlobalValueDictionary.keysToReserve(entries, blocks);
    assertTrue(reserved >= 1L + (long) GlobalValueDictionary.ENTRY_STRIDE * entries
        + (long) GlobalValueDictionary.DIRECTORY_STRIDE * blocks, "the reservation is short of the keys used");
    assertEquals(io.sirix.settings.Constants.INP_REFERENCE_COUNT,
        GlobalValueDictionary.ENTRIES_PER_PAGE * GlobalValueDictionary.ENTRY_STRIDE,
        "the entry stride and per-page count must describe the same record page");
    assertEquals(io.sirix.settings.Constants.INP_REFERENCE_COUNT,
        GlobalValueDictionary.DIRECTORY_BLOCKS_PER_PAGE * GlobalValueDictionary.DIRECTORY_STRIDE,
        "the directory stride and per-page count must describe the same record page");
  }

  private static void seed(final JsonNodeTrx wtx) {
    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"), JsonNodeTrx.Commit.NO);
  }

  private static int intern(final GlobalValueDictionaryWriter writer, final String value) {
    final byte[] bytes = utf8(value);
    return writer.intern(bytes, 0, bytes.length);
  }

  private static long flush(final JsonNodeTrx wtx, final GlobalValueDictionaryWriter writer) {
    final var storageEngineWriter = wtx.getStorageEngineWriter();
    final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    return writer.flush(namePage, DatabaseType.JSON, storageEngineWriter, storageEngineWriter.getLog());
  }

  /** A did-shaped value, spread over the hash space rather than sequential. */
  private static String value(final int i) {
    return "did:plc:" + Integer.toHexString(i * 31 + 7) + "abcdefghij";
  }

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
