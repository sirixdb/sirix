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
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.StringCompressionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FSST end to end, with the symbol table built once per revision and stored in the dictionary trie.
 *
 * <p>
 * The write path builds one table per commit from strings pooled across every page (the per-page
 * build it replaces made ingest 18× slower and rarely gathered enough samples to build a table at
 * all), stores it as a versioned record, and pages carry only its id. The property that proves the
 * whole chain — sampling, pooled build, dictionary store, page reference, lazy resolution — is
 * simply that every string reads back <em>equal</em> after the caches are dropped, because a break
 * anywhere in that chain does not throw: it decodes against the wrong symbols and returns plausible
 * garbage.
 *
 * <p>
 * Two revisions with differently-shaped vocabularies pin the versioning half: each revision's pages
 * must decode against the table that existed when they were written, not against whatever the
 * latest rebuild produced.
 */
@DisplayName("FSST with a per-revision symbol table")
public final class FsstRevisionSymbolTableIntegrationTest {

  private static final String RESOURCE_NAME = "fsstRevisionResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /**
   * Enough strings for a table to build and pay: the sampler ignores strings under 32 bytes, the
   * builder wants 64 of them and 4 KB in total, and the benefit check wants a 15% saving — URLs with
   * a long shared prefix clear all three easily.
   */
  private static final int STRINGS_PER_REVISION = 120;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).stringCompressionType(StringCompressionType.FSST).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("strings survive commit, cache drop and reopen, at every revision")
  void stringsRoundTripAcrossRevisionsAndReopen() {
    final List<String> revisionOneValues = urlValues("https://example.org/catalog/products/item-");
    final List<String> revisionTwoValues = urlValues("wss://telemetry.example.net/stream/sensor-");

    // Revision 1: one vocabulary.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(asJsonArray(revisionOneValues)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      // Revision 2: a differently-shaped vocabulary, nested as the outer array's first member, so
      // a fresh table is the right outcome and decoding revision 1 against it would visibly
      // corrupt.
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // the revision-1 array
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(asJsonArray(revisionTwoValues)),
            JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    // Cold read: nothing cached, so every page re-parses from bytes and every symbol table is
    // resolved from the dictionary trie through the reference on the page.
    Databases.getGlobalBufferManager().clearAllCaches();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      // The mechanism engaged: revision 1 stored a symbol table in the dictionary. Without this,
      // the value assertions below would also pass for plain raw strings and prove nothing.
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        final var reader = rtx.getStorageEngineReader();
        final NamePage namePage = reader.getNamePage(reader.getActualRevisionRootPage());
        assertNotNull(namePage.getFsstSymbolTable(1L, DatabaseType.JSON, reader),
            "no symbol table was stored for revision 1 — the per-revision build never engaged, "
                + "so this test is passing vacuously on raw strings");
      }

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertEquals(revisionOneValues, readStringArray(rtx), "revision 1's strings did not survive the round trip");
      }
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild(), "the outer array is missing");
        assertTrue(rtx.moveToFirstChild(), "revision 2's nested array is missing");
        assertEquals(revisionTwoValues, readArrayAtCursor(rtx), "revision 2's strings did not survive the round trip");
        // The cursor is back on the nested array; its right siblings are revision 1's strings,
        // now being read at revision 2 — the cross-revision case that would corrupt if these
        // pages resolved the latest table instead of the one they name.
        final List<String> revisionOneReadAtTwo = new ArrayList<>(STRINGS_PER_REVISION);
        while (rtx.moveToRightSibling()) {
          revisionOneReadAtTwo.add(rtx.getValue());
        }
        assertEquals(revisionOneValues, revisionOneReadAtTwo,
            "revision 1's strings read differently from revision 2 — their pages decoded "
                + "against the wrong revision's symbol table");
      }
    }
  }

  /**
   * Object-field (fused kind-50) values through the whole pipeline: sampled for the revision table,
   * FSST-rewritten in the heap, mirrored verbatim into the string region, elided, cold reopen,
   * re-injected, decoded on read. These are the strings that hold nearly all bytes on real JSON — the
   * array-string test above cannot stand in for them, since fused records take entirely different
   * write and read paths.
   */
  @Test
  @DisplayName("fused object-field strings survive the full FSST pipeline")
  void fusedFieldStringsRoundTrip() {
    final List<String> values = urlValues("https://cdn.example.com/assets/media/poster-");
    final StringBuilder json = new StringBuilder(values.size() * 96).append('{');
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("\"field").append(i).append("\":\"").append(values.get(i)).append('"');
    }
    json.append('}');

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild(), "the object is missing");
        assertTrue(rtx.moveToFirstChild(), "the first field is missing");
        final List<String> read = new ArrayList<>(values.size());
        int valueIndex = 0;
        do {
          assertArrayEquals(values.get(valueIndex).getBytes(StandardCharsets.UTF_8), rtx.getValueBytes(),
              "fused field semantic UTF-8 changed across the cold FSST round trip at index " + valueIndex);
          read.add(rtx.getValue());
          valueIndex++;
        } while (rtx.moveToRightSibling());
        assertEquals(values, read, "fused field values did not survive the FSST pipeline round trip");
      }
    }
  }

  private static List<String> urlValues(final String prefix) {
    final List<String> values = new ArrayList<>(STRINGS_PER_REVISION);
    for (int i = 0; i < STRINGS_PER_REVISION; i++) {
      values.add(prefix + i + "/details?locale=en-US&currency=EUR&campaign=summer-sale");
    }
    return values;
  }

  private static String asJsonArray(final List<String> values) {
    final StringBuilder json = new StringBuilder(values.size() * 80).append('[');
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append('"').append(values.get(i)).append('"');
    }
    return json.append(']').toString();
  }

  /** Read the first array under the document root. */
  private static List<String> readStringArray(final JsonNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    assertTrue(rtx.moveToFirstChild(), "no array under the document root");
    return readArrayAtCursor(rtx);
  }

  /** Read the string members of the array the cursor stands on. */
  private static List<String> readArrayAtCursor(final JsonNodeReadOnlyTrx rtx) {
    final List<String> values = new ArrayList<>();
    if (!rtx.moveToFirstChild()) {
      return values;
    }
    do {
      values.add(rtx.getValue());
    } while (rtx.moveToRightSibling());
    rtx.moveToParent();
    return values;
  }
}
