/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Both directions of the bulk-adoption refusal.
 *
 * <p>
 * A page marked {@code markAdoptedImmutableForFlush()} is serialized IN PLACE by the async snapshot
 * flush — the encoded image is copied over the page's own slotted frame — while it stays resolvable
 * through the intent log's snapshot layer until cleanup. Reading or copy-on-writing it in that
 * window would return the encoded bytes reinterpreted as slot data: corruption with no error. The
 * writer therefore refuses it at the record-read and page-prepare seams.
 *
 * <p>
 * The refusal is deliberately WIDER than that window: it fires from the moment of adoption, not
 * from the moment the serializer overwrites the frame. Narrowing it to the frame overwrite would
 * mean publishing a flag from the flush lane and reading it on the record path, and it would buy
 * nothing — adoption already declares the page immutable and finished, so every read it refuses is
 * a caller that has broken that contract and is racing an overwrite it cannot observe. The trade is
 * a refusal a moment early instead of a corruption a moment late.
 */
final class AdoptedPageRefusalTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static ResourceConfiguration config() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .hashKind(HashType.NONE)
                                .storeNodeHistory(false)
                                .build();
  }

  @Test
  void readingAnAdoptedPageIsRefused() {
    try (
        Database<JsonResourceSession> database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config());
        JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        StorageEngineWriter writer = session.createStorageEngineWriter()) {
      final ResourceConfiguration resourceConfig = session.getResourceConfig();
      final long recordPageKey = 1L;
      final KeyValueLeafPage adopted = new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT, resourceConfig,
          writer.getRevisionNumber(), null, null, false);

      writer.adoptDocumentLeafPage(adopted);
      assertTrue(adopted.isAdoptedImmutableForFlush(),
          "a heap-record-free adopted page is exactly the one the flush serializes in place");

      final long recordKeyOnThatPage = recordPageKey << 10;
      final IllegalStateException read =
          assertThrows(IllegalStateException.class, () -> writer.getRecord(recordKeyOnThatPage, IndexType.DOCUMENT, -1),
              "reading an adopted page must be refused, not answered from a frame the flush may have overwritten");
      assertTrue(read.getMessage().contains("adopted immutable for the bulk flush"), read.getMessage());
      assertTrue(read.getMessage().contains(Long.toString(recordPageKey)), read.getMessage());

      final IllegalStateException prepare =
          assertThrows(IllegalStateException.class, () -> writer.prepareDocumentLeafForBlit(recordPageKey),
              "preparing an adopted page for modification must be refused");
      assertTrue(prepare.getMessage().contains("adopted immutable for the bulk flush"), prepare.getMessage());
    }
  }

  @Test
  void anOrdinaryDocumentPageIsNotRefused() {
    try (
        Database<JsonResourceSession> database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config());
        JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        StorageEngineWriter writer = session.createStorageEngineWriter()) {
      // Same seam, same index type, an unadopted page: the guard must be inert.
      assertNotNull(writer.prepareDocumentLeafForBlit(1L), "an unadopted page must still be preparable");
      assertNull(writer.getRecord(1L << 10, IndexType.DOCUMENT, -1),
          "an unadopted, unpopulated slot reads as absent rather than throwing");
    }
  }

  /**
   * The regression direction: the legitimate parallel bulk import adopts many pages and must not trip
   * its own guard — not during assembly, not at commit, and not on read-back afterwards.
   */
  @Test
  void theBulkImportFlowDoesNotTripTheRefusal() {
    final int records = 4_000; // well past page 0, so real adoption happens
    final StringBuilder json = new StringBuilder(records * 24);
    json.append('[');
    for (int i = 0; i < records; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(i).append('}');
    }
    json.append(']');

    try (
        Database<JsonResourceSession> database =
            JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(), config());
        JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        ParallelBulkJsonImporter.assemble(wtx, new StringReader(json.toString()), 4096, 3);
        wtx.commit();
      }
      try (var rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());
        assertEquals(records, rtx.getChildCount(), "every imported member must be readable back");
      }
    }
  }
}
