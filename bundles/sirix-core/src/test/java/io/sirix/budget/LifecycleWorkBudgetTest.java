package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.NodeCursor;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.io.StorageType;
import io.sirix.index.IndexType;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** CONV-301 / Firstmate097: ownership work must not walk the stored document at commit/close. */
@Isolated
final class LifecycleWorkBudgetTest {
  private static final WorkCapture CAPTURE = WorkCapture.of(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS,
      EngineWorkCounters.REPLAY_RECORD_VISITS, EngineWorkCounters.DATA_FILE_FORCES);

  @TempDir
  Path directory;

  static Stream<Arguments> cases() {
    return Stream.of("json", "xml")
                 .flatMap(
                     format -> Stream.of(16, 4096)
                                     .flatMap(size -> Stream.of("writer", "session", "database")
                                                            .flatMap(owner -> Stream.of(AfterCommitState.KEEP_OPEN,
                                                                AfterCommitState.KEEP_OPEN_ASYNC_FLUSH,
                                                                AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)
                                                                                    .map(mode -> Arguments.of(format,
                                                                                        size, owner, mode)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void cleanCommitAndCloseHaveConstantDocumentWork(final String format, final int size, final String owner,
      final AfterCommitState mode) throws Exception {
    final Path path = directory.resolve("database");
    final DatabaseConfiguration config = new DatabaseConfiguration(path);
    if (format.equals("json")) {
      Databases.createJsonDatabase(config);
    } else {
      Databases.createXmlDatabase(config);
    }
    try (Database<?> database = open(format, path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .storeDiffs(false)
                                                   .useDeweyIDs(false)
                                                   .build());
      // Retain the pool independently: its terminal safety flush is outside owner-retirement work.
      try (Database<?> peer = open(format, path);
          ResourceSession<?, ?> retained = peer.beginResourceSession("resource");
          ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final NodeTrx writer = session.beginNodeTrx(0, 0, TimeUnit.MILLISECONDS, mode);
        populate(writer, size);
        writer.commit();
        writer.awaitPendingAsyncCommit();
        final NodeReadOnlyTrx[] readers = new NodeReadOnlyTrx[8];
        for (int index = 0; index < readers.length; index++) {
          readers[index] = session.beginNodeReadOnlyTrx();
        }
        final int nextRevision = writer.getRevisionNumber();
        final WorkReport committed = CAPTURE.run(() -> {
          writer.commit();
          writer.awaitPendingAsyncCommit();
        });
        committed.assertBetween(EngineWorkCounters.DATA_FILE_FORCES, 1, 2,
            "TXN-302: one durability barrier and at most one beacon flush")
                 .assertAtMost(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, 16,
                     "CONV-301: lifecycle rebind must not traverse unchanged document records");
        assertEquals(nextRevision, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx view = session.beginNodeReadOnlyTrx()) {
          final WorkReport control = CAPTURE.run(() -> assertDocument(view, size));
          control.assertAtLeast(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, size,
              "a real document traversal must exercise the record-visit diagnostic");
          final NodeCursor cursor = (NodeCursor) view;
          final long key = view.getNodeKey();
          CAPTURE.run(() -> {
            for (int move = 0; move < 16; move++) {
              assertTrue(cursor.moveTo(key));
            }
          })
                 .assertAtLeast(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, 16,
                     "cached cursor moves must exercise their own diagnostic seam")
                 .assertAtLeast(EngineWorkCounters.REPLAY_RECORD_VISITS, 16,
                     "the existing all-index total must retain cursor events");
          CAPTURE.run(() -> {
            for (int read = 0; read < 16; read++) {
              assertEquals(0L, view.getStorageEngineReader().getRecord(0, IndexType.DOCUMENT, -1).getNodeKey());
            }
          })
                 .assertAtLeast(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, 16,
                     "direct document storage reads must exercise their own diagnostic seam")
                 .assertAtLeast(EngineWorkCounters.REPLAY_RECORD_VISITS, 16,
                     "the existing all-index total must retain storage events");
        }
        // Derive the owner matrix from TXN-503: the same writer can be retired at each boundary.
        final Runnable close = switch (owner) {
          case "writer" -> writer::close;
          case "session" -> session::close;
          case "database" -> database::close;
          default -> throw new AssertionError(owner);
        };
        CAPTURE.run(close::run)
               .assertZero(EngineWorkCounters.DATA_FILE_FORCES,
                   "clean lifecycle teardown must not repeat a completed durability barrier")
               .assertAtMost(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, 16,
                   "teardown may rebind one owned writer, never scan its document");
        assertTrue(writer.isClosed());
        CAPTURE.run(close::run)
               .assertZero(EngineWorkCounters.DATA_FILE_FORCES, "TXN-503: idempotent close")
               .assertZero(EngineWorkCounters.LIFECYCLE_DOCUMENT_VISITS, "TXN-503: no repeated ownership work");
        for (final NodeReadOnlyTrx reader : readers) {
          if (!owner.equals("writer")) {
            assertTrue(reader.isClosed(), "enclosing close must release every owned reader");
          } else {
            assertFalse(reader.isClosed(), "writer close must preserve independent reader ownership");
          }
          reader.close();
        }
      }
    }
    try (Database<?> reopened = open(format, path);
        ResourceSession<?, ?> session = reopened.beginResourceSession("resource");
        NodeReadOnlyTrx view = session.beginNodeReadOnlyTrx()) {
      assertDocument(view, size);
      assertEquals(2, view.getRevisionNumber(), "both successful commits must remain published");
    }
  }

  private static Database<?> open(final String format, final Path path) {
    return format.equals("json")
        ? Databases.openJsonDatabase(path)
        : Databases.openXmlDatabase(path);
  }

  private static void populate(final NodeTrx writer, final int size) {
    if (writer instanceof JsonNodeTrx json) {
      json.insertArrayAsFirstChild();
      final long root = json.getNodeKey();
      for (int value = 0; value < size; value++) {
        json.moveTo(root);
        json.insertNumberValueAsLastChild(value);
      }
    } else {
      final XmlNodeTrx xml = (XmlNodeTrx) writer;
      xml.insertElementAsFirstChild(new QNm("root"));
      for (int value = 0; value < size; value++) {
        if (value == 0) {
          xml.insertElementAsFirstChild(new QNm("v" + value));
        } else {
          xml.insertElementAsRightSibling(new QNm("v" + value));
        }
      }
    }
  }

  private static void assertDocument(final NodeReadOnlyTrx view, final int size) {
    final NodeCursor cursor = (NodeCursor) view;
    cursor.moveToDocumentRoot();
    assertTrue(cursor.moveToFirstChild());
    assertTrue(cursor.moveToFirstChild());
    for (int value = 0; value < size; value++) {
      if (view instanceof JsonNodeReadOnlyTrx json) {
        assertEquals(value, json.getNumberValue().intValue());
      } else {
        assertEquals("v" + value, ((XmlNodeReadOnlyTrx) view).getName().getLocalName());
      }
      assertEquals(value + 1 < size, cursor.moveToRightSibling());
    }
    assertFalse(cursor.hasRightSibling());
  }
}
