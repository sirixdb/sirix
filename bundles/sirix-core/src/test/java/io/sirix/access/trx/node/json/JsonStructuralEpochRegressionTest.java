package io.sirix.access.trx.node.json;

import io.sirix.access.Databases;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static io.sirix.access.trx.node.json.JsonIdentityImportTest.create;
import static io.sirix.access.trx.node.json.JsonStructuralHashInvariantTest.assertGraph;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source-only minimized failures found by the generated identity replay oracle. */
@Isolated
final class JsonStructuralEpochRegressionTest {
  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return JsonIdentityImportTest.configurations();
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void sparseRemoveAndMovesAcrossWriterReplacement(final VersioningType versioning, final HashType hash,
      final boolean dewey) {
    for (final AfterCommitState mode : List.of(AfterCommitState.KEEP_OPEN, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH,
        AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
      for (final int threshold : new int[] {0, 3}) {
        for (int position = 0; position < 3; position++) {
          final Path path = directory.resolve(mode + "-" + threshold + "-" + position);
          long removed;
          long moved;
          long destination;
          try (final var database = create(path, versioning, hash, dewey);
              final var source = database.beginResourceSession("resource");
              final var writer = source.beginNodeTrx(threshold, mode)) {
            try {
              writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,0,{\"x\":[1,{\"y\":true}]}]"),
                  JsonNodeTrx.Commit.NO);
              writer.commit();
              writer.getStorageEngineReader().getActualRevisionRootPage().setMaxNodeKeyInDocumentIndex(4_000_000_000L);
              writer.commit();
              assertTrue(writer.moveTo(1));
              writer.insertObjectAsFirstChild();
              removed = writer.getNodeKey();
              writer.insertObjectRecordAsFirstChild("moved", new NumberValue(1));
              moved = writer.getNodeKey();
              assertTrue(writer.moveTo(1));
              writer.insertObjectAsLastChild();
              destination = writer.getNodeKey();
              if (position == 0) {
                writer.moveSubtreeToFirstChild(moved);
              } else {
                writer.insertObjectRecordAsFirstChild("anchor", new NumberValue(2));
                // Arrange the move at the threshold after the anchor has been created.
                writer.commit();
                writer.setNumberValue(3);
                writer.setNumberValue(4);
                writer.setNumberValue(5);
                if (position == 1) {
                  writer.moveSubtreeToLeftSibling(moved);
                } else {
                  writer.moveSubtreeToRightSibling(moved);
                }
              }
              assertTrue(writer.moveTo(moved));
              assertEquals(destination, writer.getParentKey());
              assertTrue(writer.moveTo(removed));
              writer.remove();
              assertGraph(writer, hash);
              writer.commit();
            } catch (final RuntimeException | Error failure) {
              writer.rollback();
              throw failure;
            }
          }
          Databases.clearGlobalCaches();
          try (final var database = Databases.openJsonDatabase(path);
              final var source = database.beginResourceSession("resource")) {
            final int latest = source.getMostRecentRevisionNumber();
            for (int revision = 1; revision <= latest; revision++) {
              try (final var reader = source.beginNodeReadOnlyTrx(revision)) {
                assertGraph(reader, hash);
                if (revision == latest) {
                  assertFalse(reader.moveTo(removed));
                  assertTrue(reader.moveTo(moved));
                  assertEquals(destination, reader.getParentKey());
                }
              }
            }
          }
        }
      }
    }
  }
}
