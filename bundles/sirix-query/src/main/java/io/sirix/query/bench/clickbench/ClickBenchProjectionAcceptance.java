package io.sirix.query.bench.clickbench;

import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Fail-closed persisted-state acceptance check for the ClickBench load-time projection.
 *
 * <p>
 * The load path deliberately does not trust the in-memory builder or its completion banner. A
 * global-dictionary budget breach can abandon the projection while allowing the base-resource load
 * to finish correctly. This verifier therefore opens the committed resource after the writer has
 * closed, validates the catalogue and slot-0 metadata, walks only the tiny row-group descriptors,
 * and asks the normal serving catalogue for a covering handle. It never builds or repairs an index.
 *
 * <p>
 * Both rounds are persistence-only. Static registry/catalogue state is cleared before the first
 * cold reopen and again between rounds, so a builder-installed handle or a decoded catalogue entry
 * cannot make an incomplete store pass.
 */
final class ClickBenchProjectionAcceptance {

  private static final int EXPECTED_COLUMN_COUNT = 25;

  private static final String[] SOURCE_PATH = {"[]"};

  private ClickBenchProjectionAcceptance() {
    throw new AssertionError("no instances");
  }

  /**
   * Verify the ClickBench projection twice from committed storage.
   *
   * @param dbDir store location containing {@code clickbench/hits.jn}
   * @param expectedRows exact source row count, or {@code -1} when it is not known
   * @return the verified persisted shape, suitable for the loader's success banner
   * @throws IllegalStateException if any definition, metadata, descriptor, or covering-handle
   *         invariant fails
   */
  static Verification verify(final Path dbDir, final long expectedRows) {
    Objects.requireNonNull(dbDir, "dbDir");
    if (expectedRows < -1L) {
      throw new IllegalArgumentException("expectedRows must be -1 or non-negative, got " + expectedRows);
    }

    final IndexDef expectedDefinition = ClickBenchProjection.spec(expectedRows).toIndexDef();
    if (ClickBenchProjection.PROJECTED_COLUMNS.size() != EXPECTED_COLUMN_COUNT) {
      throw new IllegalStateException("ClickBench projection declaration changed from the accepted "
          + EXPECTED_COLUMN_COUNT + " columns to " + ClickBenchProjection.PROJECTED_COLUMNS.size()
          + "; review the persisted benchmark shape before loading");
    }

    clearCaches();
    try {
      final Verification first = verifyOnce(dbDir, expectedRows, expectedDefinition, "cold reopen 1");
      clearCaches();
      final Verification second = verifyOnce(dbDir, expectedRows, expectedDefinition, "cold reopen 2");
      if (!first.equals(second)) {
        throw new IllegalStateException(
            "ClickBench projection verification changed across cold reopens: first=" + first + ", second=" + second);
      }
      return second;
    } finally {
      // The load process exits after reporting. Tests and embedded invocations must not retain a
      // potentially huge directory-only handle in process-global caches.
      clearCaches();
    }
  }

  private static Verification verifyOnce(final Path dbDir, final long expectedRows, final IndexDef expectedDefinition,
      final String round) {
    final Path databasePath = dbDir.resolve(ClickBenchSchema.DATABASE);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE)) {
      final int revision = session.getMostRecentRevisionNumber();
      final List<IndexDef> definitions = session.getRtxIndexController(revision)
                                                .getIndexes()
                                                .getIndexDefs()
                                                .stream()
                                                .filter(IndexDef::isProjectionIndex)
                                                .toList();
      if (definitions.size() != 1) {
        throw failure(round, "expected exactly one persisted projection definition, found " + definitions.size());
      }

      final IndexDef actualDefinition = definitions.getFirst();
      verifyDefinition(round, expectedDefinition, actualDefinition);

      final String expectedRoot = expectedDefinition.getProjectionRootPath().toString();
      final String[] expectedPaths =
          expectedDefinition.getProjectionFields().stream().map(Object::toString).toArray(String[]::new);
      final String[] expectedNames = ClickBenchProjection.PROJECTED_COLUMNS.toArray(String[]::new);
      final byte[] expectedKinds = expectedColumnKinds(expectedDefinition);

      final ProjectionIndexMetadata metadata;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final byte[] raw =
            ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), actualDefinition.getID(), 0L);
        if (raw == null) {
          throw failure(round,
              "projection definition #" + actualDefinition.getID() + " has no persisted slot-0 metadata");
        }
        try {
          metadata = ProjectionIndexMetadata.parse(raw);
        } catch (final IllegalStateException corrupt) {
          throw failure(round, "projection definition #" + actualDefinition.getID() + " has corrupt slot-0 metadata",
              corrupt);
        }
      }

      if (metadata == null) {
        throw failure(round, "projection definition #" + actualDefinition.getID()
            + " has an unsupported or incomplete slot-0 metadata payload");
      }
      if (metadata.isStale()) {
        throw failure(round,
            "projection definition #" + actualDefinition.getID() + " is stale (" + metadata.staleReason() + ")");
      }
      if (!metadata.matches(expectedRoot, expectedPaths, expectedKinds)) {
        throw failure(round, "persisted metadata root/paths/kinds do not match ClickBenchProjection.spec()");
      }
      if (!Arrays.equals(expectedNames, metadata.fieldNames())) {
        throw failure(round, "persisted metadata field names are not the ordered ClickBench projection: expected "
            + Arrays.toString(expectedNames) + ", actual " + Arrays.toString(metadata.fieldNames()));
      }
      if (metadata.rowGroupCount() <= 0) {
        throw failure(round, "persisted projection has no row groups");
      }
      if (metadata.buildRevision() < 0 || metadata.buildRevision() > revision) {
        throw failure(round,
            "invalid projection build revision " + metadata.buildRevision() + " for resource revision " + revision);
      }

      final String resourceKey = session.getResourceConfig().getResource().toString();
      final long descriptorRows =
          ProjectionIndexCatalog.countRowsFromDescriptors(session, resourceKey, revision, SOURCE_PATH);
      if (descriptorRows <= 0L) {
        throw failure(round, "projection descriptor walk is unusable or empty (rows=" + descriptorRows + ')');
      }
      if (expectedRows > 0L && descriptorRows != expectedRows) {
        throw failure(round, "projection descriptor row count " + descriptorRows
            + " does not match expected source row count " + expectedRows);
      }

      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, resourceKey, revision, SOURCE_PATH, expectedNames);
      if (handle == null) {
        throw failure(round,
            "normal catalogue lookup cannot serve all " + EXPECTED_COLUMN_COUNT + " ClickBench fields");
      }
      if (handle.defId() != actualDefinition.getID()) {
        throw failure(round,
            "covering lookup returned definition #" + handle.defId() + " instead of #" + actualDefinition.getID());
      }
      if (!expectedRoot.equals(handle.rootPath())) {
        throw failure(round, "covering handle root is " + handle.rootPath() + ", expected " + expectedRoot);
      }
      if (!Arrays.equals(expectedNames, handle.fieldNames())) {
        throw failure(round, "covering handle fields differ from the persisted declaration");
      }
      if (handle.rowGroupCount() <= 0 || handle.rowGroupCount() != metadata.rowGroupCount()) {
        throw failure(round, "covering handle row-group count " + handle.rowGroupCount()
            + " differs from metadata count " + metadata.rowGroupCount());
      }
      if (handle.validFromRevision() < 0 || handle.validFromRevision() > revision) {
        throw failure(round, "covering handle is not valid at resource revision " + revision + " (validFrom="
            + handle.validFromRevision() + ')');
      }

      return new Verification(revision, actualDefinition.getID(), expectedNames.length, metadata.rowGroupCount(),
          descriptorRows, metadata.buildRevision());
    }
  }

  private static void verifyDefinition(final String round, final IndexDef expected, final IndexDef actual) {
    if (actual.getID() != expected.getID()) {
      throw failure(round, "projection definition ID is #" + actual.getID() + ", expected #" + expected.getID());
    }
    if (!actual.getProjectionRootPath().toString().equals(expected.getProjectionRootPath().toString())) {
      throw failure(round,
          "projection root is " + actual.getProjectionRootPath() + ", expected " + expected.getProjectionRootPath());
    }
    final List<String> actualPaths = actual.getProjectionFields().stream().map(Object::toString).toList();
    final List<String> expectedPaths = expected.getProjectionFields().stream().map(Object::toString).toList();
    if (!actualPaths.equals(expectedPaths)) {
      throw failure(round,
          "projection field paths/order differ: expected " + expectedPaths + ", actual " + actualPaths);
    }
    if (!actual.getProjectionFieldTypes().equals(expected.getProjectionFieldTypes())) {
      throw failure(round, "projection field kinds/order differ: expected " + expected.getProjectionFieldTypes()
          + ", actual " + actual.getProjectionFieldTypes());
    }
  }

  private static byte[] expectedColumnKinds(final IndexDef definition) {
    final byte[] kinds = new byte[definition.getProjectionFieldTypes().size()];
    for (int i = 0; i < kinds.length; i++) {
      kinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(definition.getProjectionFieldTypes().get(i),
          definition.getProjectionFields().get(i));
    }
    return kinds;
  }

  private static IllegalStateException failure(final String round, final String detail) {
    return new IllegalStateException("ClickBench projection acceptance failed during " + round + ": " + detail);
  }

  private static IllegalStateException failure(final String round, final String detail, final Throwable cause) {
    return new IllegalStateException("ClickBench projection acceptance failed during " + round + ": " + detail, cause);
  }

  private static void clearCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  /** Persisted projection facts emitted by the loader only after both cold rounds agree. */
  record Verification(int revision, int definitionId, int columns, int rowGroups, long rows, int buildRevision) {
  }
}
