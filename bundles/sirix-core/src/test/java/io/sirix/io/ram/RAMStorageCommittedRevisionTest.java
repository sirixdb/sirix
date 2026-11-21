/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.ram;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.exception.SirixIOException;
import java.io.IOException;
import java.nio.file.Files;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.bytepipe.DeflateCompressor;
import io.sirix.io.bytepipe.FFILz4Compressor;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A successful commit on an {@link StorageType#IN_MEMORY} resource guarantees what it guarantees on
 * a file-backed one, for as long as the database stays open: every later writer, reader and
 * resource session of that database sees the committed revision, and a later commit never changes
 * what an earlier revision reads.
 *
 * <p>
 * The in-memory store used to keep the very page instances the writer handed it. The writer's
 * commit cleanup then closed those instances, and readers and the next writer got closed pages
 * back: a node inserted and committed was gone as soon as the writer was reopened.
 * </p>
 */
final class RAMStorageCommittedRevisionTest {

  /** The two node models; each inserts one value node whose key the test tracks. */
  enum Model {
    JSON, XML
  }

  @TempDir
  Path directory;

  @AfterEach
  void removeDatabase() {
    // The committed state of an in-memory resource outlives its database handles; removal drops it.
    Databases.removeDatabase(directory.resolve("database"));
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void reopenedWriterSeesCommittedNode(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long key = insertValueAndCommit(session, model, "0");
        try (NodeTrx reopened = session.beginNodeTrx()) {
          assertTrue(reopened.moveTo(key), "a reopened writer must see the committed node");
          assertValue(reopened, model, "0");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void readerOfTheSameSessionSeesCommittedNode(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long key = insertValueAndCommit(session, model, "0");
        assertEquals(1, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveTo(key), "a reader of the committed revision must see the node");
          assertValue(reader, model, "0");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void newResourceSessionSeesCommittedNode(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      final long key;
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        key = insertValueAndCommit(session, model, "0");
      }
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        assertEquals(1, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveTo(key), "a reader of a new session must see the committed node");
          assertValue(reader, model, "0");
        }
        try (NodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(key), "a writer of a new session must see the committed node");
          assertValue(writer, model, "0");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void laterCommitLeavesEarlierRevisionUnchanged(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long first = insertValueAndCommit(session, model, "0");
        final long second;
        try (NodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(first));
          // Change the committed node in place and add a sibling: the record page holding both is
          // rewritten, so revision 1 must keep reading its own copy.
          setValue(writer, model, "1");
          second = insertValueAsRightSibling(writer, model, "2");
          writer.commit();
        }
        // Reopen the writer a second time: a commit must also survive a writer that only read.
        try (NodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(second), "a writer reopened after the second commit must see its node");
          writer.commit();
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx revisionOne = session.beginNodeReadOnlyTrx(1)) {
          assertTrue(revisionOne.moveTo(first));
          assertValue(revisionOne, model, "0");
          assertFalse(revisionOne.moveTo(second), "revision 1 must not see a node a later commit inserted");
        }
        for (int revision = 2; revision <= 3; revision++) {
          try (NodeReadOnlyTrx later = session.beginNodeReadOnlyTrx(revision)) {
            assertTrue(later.moveTo(first));
            assertValue(later, model, "1");
            assertTrue(later.moveTo(second));
            assertValue(later, model, "2");
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void recreatedResourceStartsEmpty(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      final long key;
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        key = insertValueAndCommit(session, model, "0");
      }
      database.removeResource("resource");
      createInMemoryResource(database);
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource");
          NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        assertEquals(0, session.getMostRecentRevisionNumber(), "a recreated resource holds only its bootstrap");
        assertFalse(reader.moveTo(key), "a recreated resource must not see its predecessor's nodes");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void revertAppendsTheOldRevisionAsTheNewest(final Model model) {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long key = insertValueAndCommit(session, model, "0");
        try (NodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(key));
          setValue(writer, model, "1");
          writer.commit();
        }
        try (NodeTrx writer = session.beginNodeTrx()) {
          writer.revertTo(1);
          writer.commit();
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveTo(key));
          assertValue(reader, model, "0");
        }
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
          assertTrue(reader.moveTo(key));
          assertValue(reader, model, "1");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void everyRevisionOfALongHistoryReadsBackThroughANewSession(final Model model) {
    // Long enough that the resource holds many thousands of pages: a committed revision must stay
    // readable however much was committed after it.
    final int commits = 1_500;
    final long[] keys = new long[commits];
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        keys[0] = insertValueAndCommit(session, model, "0");
        for (int i = 1; i < commits; i++) {
          try (NodeTrx writer = session.beginNodeTrx()) {
            assertTrue(writer.moveTo(keys[i - 1]), "commit " + i + " must see the previous commit's node");
            keys[i] = insertValueAsRightSibling(writer, model, Integer.toString(i));
            writer.commit();
          }
        }
      }
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        assertEquals(commits, session.getMostRecentRevisionNumber());
        for (int revision = 1; revision <= commits; revision++) {
          try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
            final int newest = revision - 1;
            assertTrue(reader.moveTo(keys[newest]), "revision " + revision + " must hold its own node");
            assertValue(reader, model, Integer.toString(newest));
            assertTrue(reader.moveTo(keys[0]), "revision " + revision + " must hold the first node");
            assertValue(reader, model, "0");
            if (revision < commits) {
              assertFalse(reader.moveTo(keys[revision]), "revision " + revision + " must not see a later node");
            }
          }
        }
      }
    }
  }

  /** VER-202 and VER-205 on an IN_MEMORY resource: point-in-time reads and history. */
  @ParameterizedTest
  @EnumSource(Model.class)
  void pointInTimeReadsAndHistorySeeEveryCommittedRevision(final Model model) throws InterruptedException {
    try (Database<? extends ResourceSession<?, ?>> database = openInMemoryResource(model)) {
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long[] keys = new long[3];
        final Instant[] committedBy = new Instant[3];
        keys[0] = insertValueAndCommit(session, model, "0");
        committedBy[0] = afterTheNextMillisecond();
        for (int i = 1; i < 3; i++) {
          try (NodeTrx writer = session.beginNodeTrx()) {
            assertTrue(writer.moveTo(keys[i - 1]));
            keys[i] = insertValueAsRightSibling(writer, model, Integer.toString(i));
            writer.commit();
          }
          committedBy[i] = afterTheNextMillisecond();
        }
        for (int i = 0; i < 3; i++) {
          // The newest revision committed at or before the instant (VER-202).
          final int revision = i + 1;
          assertEquals(revision, session.getRevisionNumber(committedBy[i]));
          try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(committedBy[i])) {
            assertEquals(revision, reader.getRevisionNumber());
            assertTrue(reader.moveTo(keys[i]));
            assertValue(reader, model, Integer.toString(i));
            if (i < 2) {
              assertFalse(reader.moveTo(keys[i + 1]), "a point in time never sees a later commit");
            }
          }
        }
        final List<RevisionInfo> history = session.getHistory();
        // Newest first (VER-205); the empty bootstrap revision 0 is never reported.
        assertEquals(List.of(3, 2, 1), history.stream().map(RevisionInfo::getRevision).toList(),
            "history lists every committed revision newest first");
        for (int i = 1; i < history.size(); i++) {
          assertFalse(history.get(i).getRevisionTimestamp().isAfter(history.get(i - 1).getRevisionTimestamp()));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Model.class)
  void failedCommitDoesNotPublishATimestampEntry(final Model model) throws IOException {
    final Path path = directory.resolve("database");
    if (model == Model.JSON) {
      Databases.createJsonDatabase(new DatabaseConfiguration(path));
    } else {
      Databases.createXmlDatabase(new DatabaseConfiguration(path));
    }
    try (Database<? extends ResourceSession<?, ?>> database = openDatabase(model)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.IN_MEMORY)
                                                              .customCommitTimestamps(true)
                                                              .storeDiffs(false)
                                                              .hashKind(HashType.NONE)
                                                              .buildPathSummary(false)
                                                              .build()));
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        final long key;
        try (NodeTrx writer = session.beginNodeTrx()) {
          if (model == Model.JSON) {
            ((JsonNodeTrx) writer).insertArrayAsFirstChild();
            ((JsonNodeTrx) writer).insertStringValueAsFirstChild("0");
          } else {
            ((XmlNodeTrx) writer).insertElementAsFirstChild(new QNm("root"));
            ((XmlNodeTrx) writer).insertTextAsFirstChild("0");
          }
          key = writer.getNodeKey();
          writer.commit(null, Instant.ofEpochMilli(100));
          final Path catalogue = path.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
                                     .resolve("resource")
                                     .resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                     .resolve("2.xml");
          Files.createDirectory(catalogue);
          try {
            setValue(writer, model, "failed");
            assertThrows(SirixIOException.class, () -> writer.commit(null, Instant.ofEpochMilli(200)));
          } finally {
            Files.delete(catalogue);
          }
          assertEquals(1, session.getMostRecentRevisionNumber());
          assertEquals(1, session.getRevisionNumber(Instant.ofEpochMilli(200)));
          writer.rollback();
          assertTrue(writer.moveTo(key));
          assertValue(writer, model, "0");
          setValue(writer, model, "2");
          writer.commit(null, Instant.ofEpochMilli(300));
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        for (final long timestamp : new long[] {100, 200, 250}) {
          assertEquals(1, session.getRevisionNumber(Instant.ofEpochMilli(timestamp)));
        }
        for (final long timestamp : new long[] {300, 400}) {
          assertEquals(2, session.getRevisionNumber(Instant.ofEpochMilli(timestamp)));
          try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(Instant.ofEpochMilli(timestamp))) {
            assertEquals(2, reader.getRevisionNumber());
            assertTrue(reader.moveTo(key));
            assertValue(reader, model, "2");
          }
        }
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          assertTrue(reader.moveTo(key));
          assertValue(reader, model, "0");
        }
        assertEquals(List.of(2, 1), session.getHistory().stream().map(RevisionInfo::getRevision).toList());
        assertThrows(IllegalArgumentException.class, () -> session.beginNodeReadOnlyTrx(3));
      }
    }
  }

  /** An instant strictly after every commit so far, in a later millisecond than the next one. */
  private static Instant afterTheNextMillisecond() throws InterruptedException {
    final Instant now = Instant.now();
    Thread.sleep(2);
    return now;
  }

  /** Byte-handler pipelines the store must round-trip through: stream-based and segment-based. */
  enum Compression {
    DEFLATE, LZ4
  }

  @ParameterizedTest
  @EnumSource(Compression.class)
  void compressedPagesRoundTripThroughANewSession(final Compression compression) {
    final ByteHandlerPipeline pipeline;
    if (compression == Compression.DEFLATE) {
      pipeline = new ByteHandlerPipeline(new DeflateCompressor());
    } else {
      assumeTrue(FFILz4Compressor.isNativeAvailable(), "liblz4 is not available");
      pipeline = new ByteHandlerPipeline(new FFILz4Compressor());
    }
    final Path path = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<? extends ResourceSession<?, ?>> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.IN_MEMORY)
                                                              .byteHandlerPipeline(pipeline)
                                                              .hashKind(HashType.NONE)
                                                              .build()));
      final long first;
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        first = insertValueAndCommit(session, Model.JSON, "0");
      }
      final long second;
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        try (NodeTrx writer = session.beginNodeTrx()) {
          assertTrue(writer.moveTo(first));
          second = insertValueAsRightSibling(writer, Model.JSON, "1");
          writer.commit();
        }
      }
      try (ResourceSession<?, ?> session = database.beginResourceSession("resource")) {
        assertEquals(2, session.getMostRecentRevisionNumber());
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          assertTrue(reader.moveTo(first));
          assertValue(reader, Model.JSON, "0");
          assertFalse(reader.moveTo(second));
        }
        try (NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveTo(second));
          assertValue(reader, Model.JSON, "1");
        }
      }
    }
  }

  private Database<? extends ResourceSession<?, ?>> openInMemoryResource(final Model model) {
    final Path path = directory.resolve("database");
    if (model == Model.JSON) {
      Databases.createJsonDatabase(new DatabaseConfiguration(path));
    } else {
      Databases.createXmlDatabase(new DatabaseConfiguration(path));
    }
    final Database<? extends ResourceSession<?, ?>> database = openDatabase(model);
    createInMemoryResource(database);
    return database;
  }

  private Database<? extends ResourceSession<?, ?>> openDatabase(final Model model) {
    final Path path = directory.resolve("database");
    return model == Model.JSON
        ? Databases.openJsonDatabase(path)
        : Databases.openXmlDatabase(path);
  }

  private static void createInMemoryResource(final Database<? extends ResourceSession<?, ?>> database) {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                            .storageType(StorageType.IN_MEMORY)
                                                            .storeDiffs(false)
                                                            .hashKind(HashType.NONE)
                                                            .buildPathSummary(false)
                                                            .build()));
  }

  /** Insert one value node as the document's (first) content, commit, and return its node key. */
  private static long insertValueAndCommit(final ResourceSession<?, ?> session, final Model model, final String value) {
    try (NodeTrx writer = session.beginNodeTrx()) {
      if (model == Model.JSON) {
        final JsonNodeTrx json = (JsonNodeTrx) writer;
        json.insertArrayAsFirstChild();
        json.insertStringValueAsFirstChild(value);
      } else {
        final XmlNodeTrx xml = (XmlNodeTrx) writer;
        xml.insertElementAsFirstChild(new QNm("root"));
        xml.insertElementAsFirstChild(new QNm("item"));
        xml.insertTextAsFirstChild(value);
      }
      final long key = writer.getNodeKey();
      assertTrue(key > 0, "the inserted value must be a real data node");
      writer.commit();
      return key;
    }
  }

  private static long insertValueAsRightSibling(final NodeTrx writer, final Model model, final String value) {
    if (model == Model.JSON) {
      ((JsonNodeTrx) writer).insertStringValueAsRightSibling(value);
    } else {
      final XmlNodeTrx xml = (XmlNodeTrx) writer;
      xml.moveToParent();
      xml.insertElementAsRightSibling(new QNm("item"));
      xml.insertTextAsFirstChild(value);
    }
    return writer.getNodeKey();
  }

  private static void setValue(final NodeTrx writer, final Model model, final String value) {
    if (model == Model.JSON) {
      ((JsonNodeTrx) writer).setStringValue(value);
    } else {
      ((XmlNodeTrx) writer).setValue(value);
    }
  }

  private static void assertValue(final NodeReadOnlyTrx trx, final Model model, final String expected) {
    if (model == Model.JSON) {
      assertEquals(expected, ((JsonNodeReadOnlyTrx) trx).getValue());
    } else {
      assertEquals(expected, ((XmlNodeReadOnlyTrx) trx).getValue());
    }
  }
}
