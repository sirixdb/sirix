package io.sirix.diff.service;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.LogWrapper;
import io.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

/**
 * JSON import using the FMSE algorithm. Shreds the new JSON revision into a temporary SirixDB
 * resource, then diffs it against the old revision using {@link JsonFMSE}.
 */
public final class JsonFMSEImport {

  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(JsonFMSEImport.class));

  /**
   * Shred new JSON revision as a temporal resource.
   *
   * @param resNewRev path to the new JSON file
   * @param newRev    path for the temporary SirixDB database
   * @throws SirixIOException if sirix fails to shred the file
   */
  public void shredder(final Path resNewRev, final Path newRev) {
    assert resNewRev != null;
    assert newRev != null;

    final var conf = new DatabaseConfiguration(newRev);
    Databases.removeDatabase(newRev);
    Databases.createJsonDatabase(conf);

    try (final var db = Databases.openJsonDatabase(newRev)) {
      db.createResource(
          new ResourceConfiguration.Builder("shredded")
              .buildPathSummary(true)
              .useDeweyIDs(true)
              .build());
      try (final var resMgr = db.beginResourceSession("shredded");
          final var wtx = resMgr.beginNodeTrx()) {
        final var jsonReader = JsonShredder.createFileReader(resNewRev);
        final var shredder = new JsonShredder.Builder(wtx, jsonReader,
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }
  }

  /**
   * Import JSON data: diff the old SirixDB resource against a new JSON file.
   *
   * @param resOldRev path to the existing SirixDB database (old revision)
   * @param resNewRev path to the new JSON file
   */
  public void jsonDataImport(final Path resOldRev, final Path resNewRev) {
    try {
      final var newRevTarget = Files.createTempDirectory(resNewRev.getFileName().toString());
      if (Files.exists(newRevTarget)) {
        SirixFiles.recursiveRemove(newRevTarget);
      }
      shredder(requireNonNull(resNewRev), newRevTarget);

      try (final var databaseOld = Databases.openJsonDatabase(resOldRev);
          final var resMgrOld = databaseOld.beginResourceSession("shredded");
          final var wtx = resMgrOld.beginNodeTrx();
          final var databaseNew = Databases.openJsonDatabase(newRevTarget);
          final var resourceNew = databaseNew.beginResourceSession("shredded");
          final var rtx = resourceNew.beginNodeReadOnlyTrx();
          final var fmse = JsonFMSE.createInstance()) {
        fmse.diff(wtx, rtx);
      }
    } catch (final SirixException | IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }
}
