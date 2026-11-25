package io.sirix.index.path.summary;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** VER-201: path-summary readers expose the selected revision's exact stored microsecond. */
@Isolated
final class PathSummaryReaderTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"JSON,FILE_CHANNEL", "XML,FILE_CHANNEL", "JSON,MEMORY_MAPPED", "XML,MEMORY_MAPPED", "JSON,IN_MEMORY",
      "XML,IN_MEMORY"})
  void revisionTimestampsPreserveMicroseconds(final DatabaseType model, final StorageType storage) throws Exception {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(directory);
    if (model == DatabaseType.JSON) {
      Databases.createJsonDatabase(configuration);
    } else {
      Databases.createXmlDatabase(configuration);
    }
    final Instant first = Instant.parse("2021-03-04T05:06:07.000100Z");
    final Instant second = Instant.parse("2021-03-04T05:06:07.000101Z");
    try (final Database<?> database = model == DatabaseType.JSON
        ? Databases.openJsonDatabase(directory)
        : Databases.openXmlDatabase(directory)) {
      database.createResource(
          ResourceConfiguration.newBuilder("history").storageType(storage).customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("history")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.commit(null, first);
          writer.commit(null, second);
        }
        try (final var historical = session.openPathSummary(1); final var current = session.openPathSummary()) {
          assertEquals(first, historical.getRevisionTimestamp());
          assertEquals(second, current.getRevisionTimestamp());
        }
      }
    }
  }
}
