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
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FSST symbol tables are stored as records in the name dictionary's trie, and must therefore be
 * versioned like every other record.
 *
 * <p>This is the property the whole design rests on. A page's FSST-compressed bytes are only
 * meaningful against the exact symbol table they were encoded with, so a table can never be
 * updated in place — doing so would leave every page in every earlier revision decoding its
 * strings against the wrong table and silently returning corrupt values, with no error anywhere.
 * Storing tables as append-only records under copy-on-write is what makes rebuilding safe: the
 * old table stays reachable from the old revision root.
 *
 * <p>The tests below therefore care less about "can I write and read a table" than about what a
 * <em>later</em> revision does to an <em>earlier</em> one.
 */
@DisplayName("FSST symbol tables are versioned records")
public final class FsstSymbolTableStoreTest {

  private static final String RESOURCE_NAME = "fsstSymbolTableResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final byte[] TABLE_A = symbolTable("alpha-symbols");
  private static final byte[] TABLE_B = symbolTable("beta-symbols-differ");

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

  @Test
  @DisplayName("a table written in one revision reads back byte-for-byte in the next")
  void tableRoundTripsAcrossACommit() {
    final long id;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"),
            JsonNodeTrx.Commit.NO);
        id = storeTable(wtx, TABLE_A);
        wtx.commit();
      }
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(TABLE_A, readTable(rtx, id), "the stored table did not survive a commit");
      }
    }
  }

  /**
   * The one that matters: rebuilding must not disturb what came before.
   *
   * <p>If {@code setFsstSymbolTable} overwrote rather than appended, the second table would take
   * the first one's id and every page committed in revision 1 would start decoding against
   * revision 2's symbols.
   */
  @Test
  @DisplayName("a rebuild in a later revision leaves the earlier revision's table intact")
  void rebuildingDoesNotDisturbEarlierRevisions() {
    final long idA;
    final long idB;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"),
            JsonNodeTrx.Commit.NO);
        idA = storeTable(wtx, TABLE_A);
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        idB = storeTable(wtx, TABLE_B);
        wtx.commit();
      }

      assertNotEquals(idA, idB, "a rebuilt table reused the previous table's id, which would make "
          + "every page of the earlier revision decode against the wrong symbols");

      // Reading at the latest revision, the old table is unchanged and both are reachable.
      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(TABLE_A, readTable(rtx, idA),
            "the earlier revision's table changed when a later revision added its own");
        assertArrayEquals(TABLE_B, readTable(rtx, idB));
      }

      // Reading *at* the earlier revision, its own table is still there.
      try (final var rtx = session.beginNodeReadOnlyTrx(1)) {
        assertArrayEquals(TABLE_A, readTable(rtx, idA),
            "revision 1 lost its own symbol table once revision 2 was written");
      }
    }
  }

  /**
   * A table written in revision 2 must not be visible from revision 1.
   *
   * <p>Not pedantry: if it were visible, the store would not be going through the versioned trie
   * at all — it would be reading some shared mutable state, and the guarantee above would be
   * accidental rather than structural.
   */
  @Test
  @DisplayName("a table added later is not visible from the earlier revision")
  void laterTablesAreInvisibleToEarlierRevisions() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"),
            JsonNodeTrx.Commit.NO);
        storeTable(wtx, TABLE_A);
        wtx.commit();
      }
      final long idB;
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        idB = storeTable(wtx, TABLE_B);
        wtx.commit();
      }
      try (final var rtx = session.beginNodeReadOnlyTrx(1)) {
        assertNull(readTable(rtx, idB),
            "revision 1 can see a symbol table that only revision 2 wrote, so the store is not "
                + "actually going through the versioned trie");
      }
    }
  }

  @Test
  @DisplayName("an empty table is refused rather than stored")
  void emptyTablesAreRefused() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"),
            JsonNodeTrx.Commit.NO);
        // "Do not compress" is expressed by a page omitting the reference. A stored empty table
        // would be a second, redundant way to say it, and one the reader would have to test for
        // on every page.
        assertThrows(IllegalArgumentException.class, () -> storeTable(wtx, new byte[0]));
        wtx.commit();
      }
    }
  }

  /**
   * Ids are positive, strictly increasing and never reused — and that is all they promise.
   *
   * <p>They are not consecutive: {@code createRecord} allocates a key from the same counter and
   * then discards it for a record that carries its own node key, so the counter moves twice per
   * table. Asserting {@code 1, 2} would be pinning an implementation detail of a method this one
   * only borrows; asserting the properties pages actually depend on is what protects them.
   */
  @Test
  @DisplayName("ids are positive, increasing and distinct")
  void idsAreAllocatedInSequence() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"k\":\"v\"}"),
            JsonNodeTrx.Commit.NO);
        final long first = storeTable(wtx, TABLE_A);
        final long second = storeTable(wtx, TABLE_B);
        assertTrue(first > 0, "ids must be positive, got " + first);
        assertTrue(second > first, "ids must increase, got " + first + " then " + second);
        wtx.commit();
        // Both are readable at the ids handed out, which is the property that actually matters.
        assertArrayEquals(TABLE_A, readTableFromWriter(wtx, first));
        assertArrayEquals(TABLE_B, readTableFromWriter(wtx, second));
      }
    }
  }

  private static byte[] readTableFromWriter(final JsonNodeTrx wtx, final long id) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    return namePage.getFsstSymbolTable(id, DatabaseType.JSON, writer);
  }

  private static long storeTable(final JsonNodeTrx wtx, final byte[] table) {
    final var writer = wtx.getStorageEngineWriter();
    final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
    return namePage.setFsstSymbolTable(table, DatabaseType.JSON, writer, writer.getLog());
  }

  private static byte[] readTable(final io.sirix.api.json.JsonNodeReadOnlyTrx rtx, final long id) {
    final var reader = rtx.getStorageEngineReader();
    final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
    return namePage.getFsstSymbolTable(id, DatabaseType.JSON, reader);
  }

  /** A stand-in for a real table — the store treats the bytes as opaque. */
  private static byte[] symbolTable(final String marker) {
    return marker.getBytes(StandardCharsets.UTF_8);
  }
}
