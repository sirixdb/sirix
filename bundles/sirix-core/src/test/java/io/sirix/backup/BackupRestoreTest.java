package io.sirix.backup;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixUsageException;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip test for {@link BackupManager}: online backup of a multi-resource, multi-revision
 * database, independence of the backup from subsequent commits, restore to a new path with a
 * verification pass, and clean refusal of invalid inputs.
 */
final class BackupRestoreTest {

  private static final String RESOURCE_ONE = "resource-one";
  private static final String RESOURCE_TWO = "resource-two";

  @TempDir
  Path tempDir;

  private Path sourceDb;

  @BeforeEach
  void createSourceDatabase() {
    sourceDb = tempDir.resolve("source-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(sourceDb));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE_ONE).build());
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE_TWO).build());

      // RESOURCE_ONE: three revisions.
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_ONE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\":\"value1\"}"),
                                        JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // object
          wtx.moveToFirstChild(); // object key "key"
          wtx.moveToFirstChild(); // string value "value1"
          wtx.setStringValue("value2");
          wtx.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // object
          wtx.moveToFirstChild(); // object key "key"
          wtx.insertObjectRecordAsRightSibling("extra", new StringValue("data"));
          wtx.commit();
        }
        assertEquals(3, session.getMostRecentRevisionNumber(), "test setup: RESOURCE_ONE revisions");
      }

      // RESOURCE_TWO: two revisions.
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_TWO)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"alpha\"]"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          wtx.moveToFirstChild(); // "alpha"
          wtx.insertStringValueAsRightSibling("beta");
          wtx.commit();
        }
        assertEquals(2, session.getMostRecentRevisionNumber(), "test setup: RESOURCE_TWO revisions");
      }
    }
  }

  @Test
  @DisplayName("backup → mutate original → restore: restored data matches the backup point exactly, original unaffected")
  void backupRestoreRoundTrip() {
    final Path backupDir = tempDir.resolve("backup");
    final Path restoredDir = tempDir.resolve("restored-db");

    // Golden serializations at the backup point.
    final String goldenOneRev1;
    final String goldenOneRev2;
    final String goldenOneRev3;
    final String goldenTwoRev1;
    final String goldenTwoRev2;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb)) {
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_ONE)) {
        goldenOneRev1 = serialize(session, 1);
        goldenOneRev2 = serialize(session, 2);
        goldenOneRev3 = serialize(session, 3);
      }
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_TWO)) {
        goldenTwoRev1 = serialize(session, 1);
        goldenTwoRev2 = serialize(session, 2);
      }
    }
    assertEquals("{\"key\":\"value2\",\"extra\":\"data\"}", goldenOneRev3, "test setup: expected content");

    // Backup.
    final BackupManager.BackupSummary backupSummary = BackupManager.backupDatabase(sourceDb, backupDir);
    assertEquals(2, backupSummary.resourcesCopied());
    assertTrue(backupSummary.totalBytesCopied() > 0);
    assertEquals(3, revisionOf(backupSummary, RESOURCE_ONE));
    assertEquals(2, revisionOf(backupSummary, RESOURCE_TWO));

    // The copy preserves the structure and skips transients.
    assertTrue(Files.isRegularFile(backupDir.resolve("dbsetting.obj")));
    assertTrue(Files.isRegularFile(backupDir.resolve("resources").resolve(RESOURCE_ONE).resolve("data")
                                            .resolve("sirix.data")));
    assertTrue(Files.isRegularFile(backupDir.resolve("resources").resolve(RESOURCE_ONE).resolve("data")
                                            .resolve("sirix.revisions")));
    assertTrue(Files.isRegularFile(backupDir.resolve("resources").resolve(RESOURCE_TWO).resolve("ressetting.obj")));
    assertFalse(Files.exists(backupDir.resolve(".lock")), "the database lock file must not be copied");
    assertNoTransientFiles(backupDir);

    // Mutate the ORIGINAL after the backup — the backup must be independent of it.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_ONE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // object
      wtx.moveToFirstChild(); // object key "key"
      wtx.moveToFirstChild(); // string value "value2"
      wtx.setStringValue("value4");
      wtx.commit();
    }

    // Restore to a NEW path (with the original open-able in the same JVM).
    final BackupManager.BackupSummary restoreSummary = BackupManager.restoreDatabase(backupDir, restoredDir);
    assertEquals(2, restoreSummary.resourcesCopied());
    assertEquals(3, revisionOf(restoreSummary, RESOURCE_ONE));
    assertEquals(2, revisionOf(restoreSummary, RESOURCE_TWO));

    // The restored resource files are byte-identical to the backup.
    assertByteIdentical(backupDir, restoredDir, RESOURCE_ONE, "sirix.data");
    assertByteIdentical(backupDir, restoredDir, RESOURCE_ONE, "sirix.revisions");
    assertByteIdentical(backupDir, restoredDir, RESOURCE_TWO, "sirix.data");
    assertByteIdentical(backupDir, restoredDir, RESOURCE_TWO, "sirix.revisions");

    // The restored database matches the backup point exactly.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(restoredDir)) {
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_ONE)) {
        assertEquals(3, session.getMostRecentRevisionNumber(), "restored RESOURCE_ONE is at the backup point");
        assertEquals(goldenOneRev1, serialize(session, 1));
        assertEquals(goldenOneRev2, serialize(session, 2));
        assertEquals(goldenOneRev3, serialize(session, 3));
      }
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE_TWO)) {
        assertEquals(2, session.getMostRecentRevisionNumber(), "restored RESOURCE_TWO is at the backup point");
        assertEquals(goldenTwoRev1, serialize(session, 1));
        assertEquals(goldenTwoRev2, serialize(session, 2));
      }
    }

    // The original is unaffected: it has the post-backup revision 4 and intact history.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(sourceDb);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE_ONE)) {
      assertEquals(4, session.getMostRecentRevisionNumber(), "original has the post-backup revision");
      assertEquals(goldenOneRev3, serialize(session, 3), "original history is intact");
      final String originalRev4 = serialize(session, 4);
      assertEquals("{\"key\":\"value4\",\"extra\":\"data\"}", originalRev4);
      assertNotEquals(goldenOneRev3, originalRev4, "post-backup commit changed the original only");
    }
  }

  @Test
  @DisplayName("backup of a non-database path fails cleanly")
  void backupRefusesNonDatabasePath() throws IOException {
    final Path notADatabase = tempDir.resolve("not-a-database");
    Files.createDirectories(notADatabase);
    Files.writeString(notADatabase.resolve("some.file"), "hello");

    final Path target = tempDir.resolve("backup-of-junk");
    assertThrows(SirixUsageException.class, () -> BackupManager.backupDatabase(notADatabase, target));
    assertThrows(SirixUsageException.class,
                 () -> BackupManager.backupDatabase(tempDir.resolve("does-not-exist"), target));
    assertFalse(Files.exists(target), "no partial target may be left behind");
  }

  @Test
  @DisplayName("backup refuses a non-empty target (and leaves it untouched)")
  void backupRefusesNonEmptyTarget() throws IOException {
    final Path target = tempDir.resolve("occupied-target");
    Files.createDirectories(target);
    final Path occupant = target.resolve("precious.txt");
    Files.writeString(occupant, "do not delete");

    assertThrows(SirixUsageException.class, () -> BackupManager.backupDatabase(sourceDb, target));
    assertEquals("do not delete", Files.readString(occupant), "refusal must not modify the target");

    // A target inside the database directory is refused, too.
    assertThrows(SirixUsageException.class,
                 () -> BackupManager.backupDatabase(sourceDb, sourceDb.resolve("nested-backup")));
  }

  @Test
  @DisplayName("restore refuses a non-empty target (and leaves it untouched)")
  void restoreRefusesNonEmptyTarget() throws IOException {
    final Path backupDir = tempDir.resolve("backup");
    BackupManager.backupDatabase(sourceDb, backupDir);

    final Path target = tempDir.resolve("occupied-restore-target");
    Files.createDirectories(target);
    final Path occupant = target.resolve("precious.txt");
    Files.writeString(occupant, "do not delete");

    assertThrows(SirixUsageException.class, () -> BackupManager.restoreDatabase(backupDir, target));
    assertEquals("do not delete", Files.readString(occupant), "refusal must not modify the target");
  }

  @Test
  @DisplayName("restore of a non-backup path fails cleanly")
  void restoreRefusesNonBackupSource() throws IOException {
    final Path notABackup = tempDir.resolve("not-a-backup");
    Files.createDirectories(notABackup);
    Files.writeString(notABackup.resolve("random.bin"), "junk");

    final Path target = tempDir.resolve("restore-of-junk");
    assertThrows(SirixUsageException.class, () -> BackupManager.restoreDatabase(notABackup, target));
    assertFalse(Files.exists(target), "no partial target may be left behind");
  }

  private static int revisionOf(final BackupManager.BackupSummary summary, final String resourceName) {
    return summary.resources()
                  .stream()
                  .filter(resource -> resource.resourceName().equals(resourceName))
                  .findFirst()
                  .orElseThrow(() -> new AssertionError(resourceName + " missing in summary: " + summary))
                  .mostRecentRevision();
  }

  private static String serialize(final JsonResourceSession session, final int revision) {
    try (final StringWriter writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer, revision).build().call();
      return writer.toString();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void assertByteIdentical(final Path backupDir, final Path restoredDir, final String resource,
      final String fileName) {
    final Path relative = Path.of("resources", resource, "data", fileName);
    try {
      assertEquals(-1L, Files.mismatch(backupDir.resolve(relative), restoredDir.resolve(relative)),
                   relative + " must be byte-identical between backup and restore");
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void assertNoTransientFiles(final Path root) {
    try (final Stream<Path> paths = Files.walk(root)) {
      paths.filter(Files::isRegularFile).forEach(file -> {
        final String name = file.getFileName().toString();
        assertFalse(name.contains(".tmp"), "transient temp file copied: " + file);
        assertNotEquals(".commit", name, "in-flight commit marker copied: " + file);
        assertNotEquals(".lock", name, "lock file copied: " + file);
      });
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
