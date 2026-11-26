package io.sirix.format;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.IOStorage;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FMT-402 round-trip fixture: every revision retains its content, author, message, source order and
 * already-increasing timestamp; exporting leaves the source files byte-for-byte unchanged.
 *
 * <p>
 * The serializer reads each revision, the shredder writes it back, and {@code commit(message,
 * timestamp)} carries the commit metadata. This fixture does not exercise timestamp adjustment;
 * spec/FMT-disk-format.md#fmt-402--history-preserving-export-and-import owns the requirement,
 * evidence and remaining gaps.
 */
final class HistoryPreservingExportImportTest {

  private static final String RESOURCE = "resource";

  /** Distinct per-revision content: a dropped or reordered revision cannot pass unnoticed. */
  private static final String[] DOCUMENTS = {"[1]", "[1,2]", "[1,2,3]"};

  private static final String[] MESSAGES = {"first import", "second \"quoted\" import", ""};

  private static final User[] AUTHORS = {new User("alice", UUID.fromString("00000000-0000-0000-0000-00000000a11c")),
      new User("bob", UUID.fromString("00000000-0000-0000-0000-00000000b0b0")),
      new User("alice", UUID.fromString("00000000-0000-0000-0000-00000000a11c"))};

  @TempDir
  Path directory;

  @Test
  void everyRevisionRoundTripsWithContentAuthorMessageAndTimestamp() throws Exception {
    final Path sourcePath = createStore("source");
    final Path targetPath = createStore("target");
    final Instant[] sourceTimestamps = commitTimestamps();
    writeHistory(sourcePath, sourceTimestamps);

    final List<Revision> exported = export(sourcePath);
    assertHistoryMatchesFixture(exported, sourceTimestamps, "export");

    importHistory(targetPath, exported);

    final List<Revision> imported = export(targetPath);
    assertHistoryMatchesFixture(imported, sourceTimestamps, "import");
  }

  @Test
  void exportLeavesTheSourceStoreUnchanged() throws Exception {
    final Path sourcePath = createStore("read-only-source");
    final Instant[] sourceTimestamps = commitTimestamps();
    writeHistory(sourcePath, sourceTimestamps);

    final byte[] dataBefore = Files.readAllBytes(dataFile(sourcePath));
    final byte[] revisionsBefore = Files.readAllBytes(revisionsFile(sourcePath));

    assertHistoryMatchesFixture(export(sourcePath), sourceTimestamps, "export");

    Databases.clearGlobalCaches();
    assertArrayEquals(dataBefore, Files.readAllBytes(dataFile(sourcePath)), "export mutated sirix.data");
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile(sourcePath)), "export mutated sirix.revisions");
  }

  private static void assertHistoryMatchesFixture(final List<Revision> history, final Instant[] sourceTimestamps,
      final String phase) {
    assertEquals(DOCUMENTS.length, history.size(), phase + " dropped a revision");
    for (int index = 0; index < DOCUMENTS.length; index++) {
      final Revision revision = history.get(index);
      final String where = phase + " revision " + (index + 1);
      assertEquals(index + 1, revision.revision(), where + ": revision order changed");
      assertEquals(DOCUMENTS[index], revision.content(), where + ": content changed");
      assertEquals(MESSAGES[index], revision.message(), where + ": commit message changed");
      assertEquals(AUTHORS[index].getName(), revision.author().getName(), where + ": author name changed");
      assertEquals(AUTHORS[index].getId(), revision.author().getId(), where + ": author id changed");
      assertEquals(sourceTimestamps[index], revision.timestamp(), where + ": effective commit timestamp changed");
      if (index > 0) {
        assertTrue(revision.timestamp().isAfter(history.get(index - 1).timestamp()),
            where + ": effective commit timestamps are not strictly increasing");
      }
    }
  }

  // ==================== export / import ====================

  /** Reads every revision of a resource with its content and commit metadata. */
  private static List<Revision> export(final Path databasePath) throws IOException {
    final List<Revision> revisions = new ArrayList<>();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      // getHistory() reports revisions 1..newest, newest first; export walks source order.
      final List<RevisionInfo> history = session.getHistory();
      for (int index = history.size() - 1; index >= 0; index--) {
        final RevisionInfo info = history.get(index);
        revisions.add(new Revision(info.getRevision(), serialize(session, info.getRevision()),
            info.getCommitMessage().orElse(""), info.getRevisionTimestamp(), info.getUser()));
      }
    }
    return revisions;
  }

  /** Loads an exported history into a newly created store, one cold writer per source revision. */
  private static void importHistory(final Path databasePath, final List<Revision> exported) {
    for (int index = 0; index < exported.size(); index++) {
      final Revision revision = exported.get(index);
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath, revision.author());
          final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        replaceContent(wtx, revision.content());
        wtx.commit(revision.message(), revision.timestamp());
      }
    }
  }

  private static void replaceContent(final JsonNodeTrx wtx, final String content) {
    wtx.moveToDocumentRoot();
    if (wtx.hasFirstChild()) {
      wtx.moveToFirstChild();
      wtx.remove();
      wtx.moveToDocumentRoot();
    }
    wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(content), JsonNodeTrx.Commit.NO);
  }

  // ==================== fixtures ====================

  /** A store whose resource accepts the commit timestamps an import has to carry over. */
  private Path createStore(final String name) {
    final Path databasePath = directory.resolve(name);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).customCommitTimestamps(true).build());
    }
    return databasePath;
  }

  /**
   * Already-increasing source timestamps, later than both bootstrap commits, isolate the unchanged
   * timestamp arm of FMT-402. The sleep lets the wall clock pass them before they are supplied to
   * commit; nothing here asserts on elapsed time. FMT-402 owns the timestamp import requirement.
   */
  private static Instant[] commitTimestamps() throws InterruptedException {
    final Instant base = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    final Instant[] timestamps = new Instant[DOCUMENTS.length];
    for (int index = 0; index < timestamps.length; index++) {
      timestamps[index] = base.plusMillis(index + 1L);
    }
    Thread.sleep(25);
    return timestamps;
  }

  private static void writeHistory(final Path databasePath, final Instant[] timestamps) {
    for (int index = 0; index < DOCUMENTS.length; index++) {
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath, AUTHORS[index]);
          final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        replaceContent(wtx, DOCUMENTS[index]);
        wtx.commit(MESSAGES[index], timestamps[index]);
      }
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws IOException {
    try (final StringWriter writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer, revision).build().call();
      return writer.toString();
    }
  }

  private static Path resourcePath(final Path databasePath) {
    return databasePath.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()).resolve(RESOURCE);
  }

  private static Path dataFile(final Path databasePath) {
    return resourcePath(databasePath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                     .resolve(IOStorage.FILENAME);
  }

  private static Path revisionsFile(final Path databasePath) {
    return resourcePath(databasePath).resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                     .resolve(IOStorage.REVISIONS_FILENAME);
  }

  /** One exported revision: its content and the commit metadata an import must carry over. */
  private record Revision(int revision, String content, String message, Instant timestamp, User author) {
  }
}
