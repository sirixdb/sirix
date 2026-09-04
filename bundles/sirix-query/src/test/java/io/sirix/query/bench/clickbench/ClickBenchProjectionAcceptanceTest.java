package io.sirix.query.bench.clickbench;

import com.google.gson.stream.JsonReader;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.Reader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClickBenchProjectionAcceptanceTest {

  private static final int ROWS = 4;

  @AfterEach
  void clearProjectionState() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    ProjectionBulkLoad.clearActive();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void acceptsOnlyAColdReopenableOnePassProjection(final VersioningType versioningType, @TempDir final Path directory)
      throws Exception {
    load(directory, ClickBenchProjection.spec(ROWS), versioningType);

    final ClickBenchProjectionAcceptance.Verification verified = ClickBenchProjectionAcceptance.verify(directory, ROWS);

    assertEquals(0, verified.definitionId());
    assertEquals(25, verified.columns());
    assertEquals(ROWS, verified.rows());
    assertTrue(verified.rowGroups() > 0);
    assertTrue(verified.buildRevision() >= 0);
    assertTrue(verified.buildRevision() <= verified.revision());
  }

  @Test
  void rejectsAResourceWithNoProjection(@TempDir final Path directory) throws Exception {
    load(directory, null);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> ClickBenchProjectionAcceptance.verify(directory, ROWS));

    assertTrue(failure.getMessage().contains("expected exactly one persisted projection definition"),
        failure::getMessage);
  }

  @Test
  void rejectsAProjectionWithTheWrongPersistedShape(@TempDir final Path directory) throws Exception {
    load(directory, new ProjectionSpec(ClickBenchProjection.ROOT_PATH, List.of("/[]/WatchID"), List.of("long"), ROWS));

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> ClickBenchProjectionAcceptance.verify(directory, ROWS));

    assertTrue(failure.getMessage().contains("projection field paths/order differ"), failure::getMessage);
  }

  @Test
  void rejectsAProjectionWhoseDescriptorsDoNotContainTheExpectedRows(@TempDir final Path directory) throws Exception {
    load(directory, ClickBenchProjection.spec(ROWS));

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> ClickBenchProjectionAcceptance.verify(directory, ROWS + 1L));

    assertTrue(failure.getMessage().contains("does not match expected source row count"), failure::getMessage);
  }

  private static void load(final Path directory, final ProjectionSpec projection) throws Exception {
    load(directory, projection, VersioningType.FULL);
  }

  private static void load(final Path directory, final ProjectionSpec projection, final VersioningType versioningType)
      throws Exception {
    try (
        BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                 .location(directory)
                                                 .buildPathSummary(true)
                                                 .buildPathStatistics(false)
                                                 .versioningType(versioningType)
                                                 .build();
        Reader source = ClickBenchSource.open("generate:" + ROWS + ":17");
        JsonReader reader = new JsonReader(source)) {
      if (projection == null) {
        store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, reader);
      } else {
        store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, reader, projection);
      }
    }
  }
}
