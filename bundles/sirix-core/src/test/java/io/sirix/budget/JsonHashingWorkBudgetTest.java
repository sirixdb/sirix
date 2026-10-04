package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonHashingWorkProbe;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Appending a forest must repair all new roots without rehashing the unchanged array prefix.
 * Observed for prefixes 16/4096: 18 reads, one old child, three new writes. Mutation checks:
 * rescanning siblings reads 81/12321 records (16/4096 old children); stopping after one new root
 * writes one identity instead of three. Both mutants fail their respective budgets.
 */
@Isolated
final class JsonHashingWorkBudgetTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(ints = {16, 4096})
  void rollingAppendVisitsOnlyNewRecordsAndTheBoundary(final int prefix) throws Exception {
    final Path path = directory.resolve("source");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.ROLLING)
                                                   .useDeweyIDs(false)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[" + "0,".repeat(prefix - 1) + "0]"),
            JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource");
        final var writer = session.beginNodeTrx();
        final var probe = new JsonHashingWorkProbe(writer, writer.getMaxNodeKey())) {
      assertTrue(writer.moveTo(1));
      final var reads =
          WorkCounter.alwaysOn("hashRecordReads", "one record read during hash maintenance", probe::reads);
      final var oldKeys =
          WorkCounter.alwaysOn("hashPrefixKeys", "one distinct pre-existing array child read", probe::prefixKeys);
      final var newKeys =
          WorkCounter.alwaysOn("hashNewKeysWritten", "one distinct inserted record hashed", probe::newKeysWritten);
      final var captured = WorkCapture.of(reads, oldKeys, newKeys)
                                      .call(() -> writer.insertSubtreeAsLastChild(
                                          JsonShredder.createStringReader("[1,2,3]"), JsonNodeTrx.Commit.NO,
                                          JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES));
      captured.work()
              .assertBetween(reads, 3, 40, "append hashing grew with the unchanged prefix")
              .assertExactly(oldKeys, 1, "only the old last child's sibling link changes")
              .assertExactly(newKeys, 3, "every inserted root must contribute its own hash");
      assertTrue(writer.moveTo(1));
      assertEquals(prefix + 3, writer.getChildCount());
      assertEquals(prefix + 3, writer.getDescendantCount());
      writer.commit();
    }
  }
}
