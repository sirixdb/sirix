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
 * FMT-402: exporting a 1.0 resource writes every revision with its committed content, commit
 * timestamp, author and message, and importing that export into a newly created 1.0 store keeps
 * the source revision order without dropping a revision. The source stays unchanged.
 *
 * <p>
 * The export/import here is the documented composition of the shipped building blocks — the
 * serializer reads each revision, the shredder writes it back, and {@code commit(message,
 * timestamp)} carries the commit metadata — because no single export/import tool exists yet.
 *
 * <p>
 * Two clauses of FMT-402 are deliberately NOT exercised, because the engine cannot express them at
 * this baseline and a test asserting today's behaviour would pin the gap as correct: the
 * one-microsecond adjustment of a tie or regression (commit timestamps persist at millisecond
 * precision) and the queryable original timestamp of an adjusted revision (no such metadata
 * exists). {@link #theTimestampRuleAdjustsOnlyTiesAndRegressions()} pins the decided arithmetic
 * itself, independent of the engine.
 */
final class HistoryPreservingExportImportTest {

  private static final String RESOURCE = "resource";

  /** Distinct per-revision content: a dropped or reordered revision cannot pass unnoticed. */
  private static final String[] DOCUMENTS = {"[1]", "[1,2]", "[1,2,3]"};

  private static final String[] MESSAGES = {"first import", "second \"quoted\" import", ""};

  private static final User[] AUTHORS = {
      new User("alice", UUID.fromString("00000000-0000-0000-0000-00000000a11c")),
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
    assertEquals(DOCUMENTS.length, exported.size(), "export dropped a revision");

    final Instant[] effective = effectiveTimestamps(sourceTimestamps);
    // This fixture's source timestamps already increase, so the rule must leave every one of them
    // alone; the adjustment branch is pinned separately, by the oracle test.
    assertArrayEquals(sourceTimestamps, effective, "the fixture needs no timestamp adjustment");
    importHistory(targetPath, exported, effective);

    final List<Revision> imported = export(targetPath);
    assertEquals(exported.size(), imported.size(), "import dropped a revision");
    for (int index = 0; index < exported.size(); index++) {
      final Revision source = exported.get(index);
      final Revision destination = imported.get(index);
      final String where = "revision " + destination.revision();
      assertEquals(source.revision(), destination.revision(), where + ": revision order changed");
      assertEquals(source.content(), destination.content(), where + ": content changed");
      assertEquals(source.message(), destination.message(), where + ": commit message changed");
      assertEquals(source.author().getName(), destination.author().getName(), where + ": author name changed");
      assertEquals(source.author().getId(), destination.author().getId(), where + ": author id changed");
      assertEquals(effective[index], destination.timestamp(), where + ": effective commit timestamp changed");
      if (index > 0) {
        assertTrue(destination.timestamp().isAfter(imported.get(index - 1).timestamp()),
            where + ": effective commit timestamps are not strictly increasing");
      }
    }
  }

  @Test
  void exportLeavesTheSourceStoreUnchanged() throws Exception {
    final Path sourcePath = createStore("read-only-source");
    writeHistory(sourcePath, commitTimestamps());

    final byte[] dataBefore = Files.readAllBytes(dataFile(sourcePath));
    final byte[] revisionsBefore = Files.readAllBytes(revisionsFile(sourcePath));

    assertEquals(DOCUMENTS.length, export(sourcePath).size());

    Databases.clearGlobalCaches();
    assertArrayEquals(dataBefore, Files.readAllBytes(dataFile(sourcePath)), "export mutated sirix.data");
    assertArrayEquals(revisionsBefore, Files.readAllBytes(revisionsFile(sourcePath)),
        "export mutated sirix.revisions");
  }

  /**
   * Captain decision 010's arithmetic, as an independent oracle: the first timestamp is kept, a
   * strictly greater one is kept, and a tie or a regression becomes the preceding effective
   * timestamp plus one microsecond. The example is VER-204's.
   */
  @Test
  void theTimestampRuleAdjustsOnlyTiesAndRegressions() {
    final Instant[] source = {micros(1000), micros(1000), micros(999), micros(1005)};
    final Instant[] expected = {micros(1000), micros(1001), micros(1002), micros(1005)};
    assertArrayEquals(expected, effectiveTimestamps(source));
  }

  // ==================== the rule under test ====================

  /**
   * The effective commit timestamps an import must use for a source sequence: the first value
   * unchanged, every later value unchanged when it already increases, and the preceding effective
   * value plus one microsecond when it would tie or go backwards.
   */
  private static Instant[] effectiveTimestamps(final Instant[] source) {
    final Instant[] effective = new Instant[source.length];
    effective[0] = source[0];
    for (int index = 1; index < source.length; index++) {
      effective[index] = source[index].isAfter(effective[index - 1])
          ? source[index]
          : effective[index - 1].plusNanos(1_000L);
    }
    return effective;
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
  private static void importHistory(final Path databasePath, final List<Revision> exported,
      final Instant[] effective) {
    for (int index = 0; index < exported.size(); index++) {
      final Revision revision = exported.get(index);
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath, revision.author());
          final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        replaceContent(wtx, revision.content());
        wtx.commit(revision.message(), effective[index]);
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
   * Commit timestamps for the source history. They sit just after both stores' bootstrap commits
   * because the engine refuses a commit timestamp below the preceding revision's, and the sleep
   * only lets the wall clock pass them — it refuses a timestamp in the future. Nothing here
   * asserts on elapsed time.
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

  private static Instant micros(final long microseconds) {
    return Instant.EPOCH.plusNanos(microseconds * 1_000L);
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
