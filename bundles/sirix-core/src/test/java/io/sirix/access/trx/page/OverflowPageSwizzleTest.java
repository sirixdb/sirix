package io.sirix.access.trx.page;

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
import io.sirix.node.ValueDictionaryValueBlockNode;
import io.sirix.page.NamePage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Overflow records of a DOCUMENT page are swizzled onto their reference (sibling walks over large
 * values re-resolve them on every step, #1076); overflow records of an INDEX page are not. A swizzled
 * page lives as long as the record page owning the reference stays cached, and that cache weighs slot
 * memory only — on the projection value dictionary, whose 64 KiB blocks are all overlong, the swizzle
 * pinned every block ever walked (3.9 GB after two 100M-row dictionary walks) beside the weighed
 * dictionary record cache that exists to bound exactly that retention.
 */
final class OverflowPageSwizzleTest {

  private static final String RESOURCE_NAME = "overflowSwizzleResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Enough dictionary entries for several 64 KiB value blocks, every one an overlong NAME record. */
  private static final int ENTRIES = 3_000;

  /** A string value far past the inline record limit, so the DOCUMENT record is overlong too. */
  private static final int BIG_VALUE_CHARS = 8_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String valueOf(final int id) {
    return "value-" + id + "-" + "x".repeat(20 + id % 37);
  }

  /** One document with an over-long string plus a global dictionary of {@link #ENTRIES} values. */
  private static long build(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(
          JsonShredder.createStringReader("{\"big\":\"" + "b".repeat(BIG_VALUE_CHARS) + "\",\"k\":\"v\"}"),
          JsonNodeTrx.Commit.NO);
      final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
      for (int i = 1; i <= ENTRIES; i++) {
        final byte[] utf8 = valueOf(i).getBytes(StandardCharsets.UTF_8);
        dictionary.intern(utf8, 0, utf8.length);
      }
      final var writer = wtx.getStorageEngineWriter();
      final NamePage namePage = writer.getNamePage(writer.getActualRevisionRootPage());
      final long headerKey = dictionary.flush(namePage, DatabaseType.JSON, writer, writer.getLog());
      wtx.commit();
      return headerKey;
    }
  }

  @Test
  @DisplayName("dictionary blocks are read without being pinned; the document's overflow record is swizzled")
  void indexOverflowPagesAreNotPinned() {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
      final long headerKey = build(session);
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();

        // Every dictionary value, so every value block (an overlong NAME record) is read from storage
        // at least once — through the bounded record cache, never pinned on the page's reference.
        final long swizzledBefore = NodeStorageEngineReader.overflowPagesSwizzled();
        final long unpinnedBefore = NodeStorageEngineReader.overflowPagesReadUnpinned();
        long totalBytes = 0;
        for (int id = 1; id <= ENTRIES; id++) {
          final String value = GlobalValueDictionary.value(headerKey, id, reader);
          assertEquals(valueOf(id), value, "dictionary value of id " + id);
          totalBytes += value.length();
        }
        assertTrue(totalBytes > ValueDictionaryValueBlockNode.MAX_BLOCK_BYTES,
            "the fixture must span more than one value block: " + totalBytes + " B");
        final long unpinnedByDictionary = NodeStorageEngineReader.overflowPagesReadUnpinned() - unpinnedBefore;
        assertTrue(unpinnedByDictionary >= 1,
            "dictionary blocks are overlong NAME records and must have been read unpinned: " + unpinnedByDictionary);
        assertEquals(0L, NodeStorageEngineReader.overflowPagesSwizzled() - swizzledBefore,
            "no NAME-index overflow page may be swizzled onto its reference");

        // The document's over-long string: an overflow DOCUMENT record, swizzled for navigation.
        final long swizzledBeforeDocument = NodeStorageEngineReader.overflowPagesSwizzled();
        final long unpinnedBeforeDocument = NodeStorageEngineReader.overflowPagesReadUnpinned();
        rtx.moveToDocumentRoot();
        assertTrue(rtx.moveToFirstChild(), "object");
        assertTrue(rtx.moveToFirstChild(), "object key");
        // A fused key+string record answers getValue on the key itself; an unfused one has the string
        // as its child. Either way the record carrying the value is the overlong one.
        rtx.moveToFirstChild();
        assertEquals(BIG_VALUE_CHARS, rtx.getValue().length(), "the over-long value round-trips");
        assertTrue(NodeStorageEngineReader.overflowPagesSwizzled() - swizzledBeforeDocument >= 1,
            "an overflow DOCUMENT record must be swizzled onto its reference");
        assertEquals(0L, NodeStorageEngineReader.overflowPagesReadUnpinned() - unpinnedBeforeDocument,
            "a DOCUMENT overflow read is never the unpinned kind");
      }
    }
  }
}
