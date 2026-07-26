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
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node record larger than the projection index's 16 MB segment ceiling must still commit and read
 * back.
 *
 * <p>{@link OverflowPage} is SHARED: {@code KeyValueLeafPage} spills any record past the slot-heap
 * threshold into one, and that threshold is a spill TRIGGER, not a ceiling — a single large string
 * or binary value legitimately produces an arbitrarily large page. A size cap on the page therefore
 * rejects valid user data at commit and, worse, makes already-committed pages of that size
 * unreadable on every subsequent read. The projection index's own 16 MB limit lives on
 * {@code RowGroupDescriptor.MAX_SEGMENT_BYTES} instead, where it bounds only projection segments.</p>
 */
public final class OverflowPageLargeRecordTest {

  private static final String RESOURCE_NAME = "largeRecordResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Comfortably past the projection index's 16 MB segment ceiling. */
  private static final int VALUE_CHARS = 17 * 1024 * 1024;

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
  void recordLargerThanTheProjectionSegmentCeilingRoundTrips() {
    final StringBuilder value = new StringBuilder(VALUE_CHARS);
    for (int i = 0; i < VALUE_CHARS; i++) {
      value.append((char) ('a' + (i % 26))); // plain ASCII — no escaping, exact byte length
    }
    final String json = "{\"big\":\"" + value + "\"}";

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();

      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());   // OBJECT
        assertTrue(rtx.moveToFirstChild());   // the "big" object key
        if (rtx.hasFirstChild()) {
          // Shape-tolerant: depending on the node kind the value is either a separate child node or
          // carried by the object-key node itself.
          assertTrue(rtx.moveToFirstChild());
        }
        assertEquals(VALUE_CHARS, rtx.getValue().length(),
            "a >16 MB record must survive commit and read back intact");
      }
    }
  }
}
