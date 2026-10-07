package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.stream.Stream;

import static io.sirix.budget.EngineWorkCounters.DIFF_BOOKKEEPING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R8: each move uses three keyed operations, never scans the growing pending-insert map. Restoring
 * a scan costs 1648/25024/395008 operations for 32/128/512 moves; all six fixtures fail.
 */
@Isolated
final class JsonDiffBookkeepingWorkBudgetTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(32, 128, 512).flatMap(count -> Stream.of(false, true).map(dewey -> Arguments.of(count, dewey)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void siblingReorderingUsesBoundedKeyedOperations(final int count, final boolean dewey) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.ROLLING)
                                                   .useDeweyIDs(dewey)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[-1]"), JsonNodeTrx.Commit.NO);
        writer.commit();
        assertTrue(writer.moveTo(1));
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("[" + "0,".repeat(count - 1) + "0]"),
            JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES, JsonNodeTrx.SkipRootToken.YES);
        final var work = WorkCapture.of(DIFF_BOOKKEEPING).run(() -> {
          for (int index = 0; index < count; index++) {
            assertTrue(writer.moveTo(1));
            writer.moveSubtreeToFirstChild(index + 3);
          }
        });
        work.assertBetween(DIFF_BOOKKEEPING, count, 4L * count,
            "pending-insert bookkeeping must not scan all earlier inserts for each move");
        assertTrue(writer.moveTo(1));
        assertEquals(count + 1, writer.getChildCount());
        assertTrue(writer.moveToFirstChild());
        for (int index = count - 1; index >= 0; index--) {
          assertEquals(index + 3, writer.getNodeKey());
          assertTrue(writer.moveToRightSibling());
        }
        assertEquals(2, writer.getNodeKey());
        writer.commit();
      }
    }
  }
}
