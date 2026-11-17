package io.sirix.diff;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clause B9: the hash-based diff shortcut is the resource's decision, not the caller's.
 *
 * <p>
 * A resource created without hashes stores none, so every record reads back hash 0. If
 * {@code AbstractDiff} were to take the {@code optimizedDiff} route on such a resource it would
 * compare zero against zero, conclude {@code SAMEHASH} and report an unchanged document — silently
 * losing every difference. The guard therefore sits on the resource configuration: the default of
 * {@code DiffFactory.Builder.hashKind} comes from the diffed resource, and {@code AbstractDiff}
 * forces {@link HashType#NONE} when the resource stores no hashes, whatever the caller asked for.
 *
 * <p>
 * Both directions are checked here, because only one of them is safe to honour: a caller may force
 * the full comparison on a hashed resource ({@code hashKind(NONE)}), but may not force the shortcut
 * on a hashless one.
 */
final class DiffFactoryHashKindTest {

  private static final String RESOURCE = "resource";

  private static final String DOCUMENT = "{\"a\":[1,2,3],\"b\":{\"c\":\"x\"},\"e\":\"keep\"}";

  @TempDir
  private Path directory;

  /** One diff callback, as a comparable record. */
  private record Emitted(DiffType type, long newNodeKey, long oldNodeKey, int newDepth, int oldDepth) {
  }

  private static final class Recorder implements DiffObserver {
    private final List<Emitted> emitted = new ArrayList<>();
    private boolean done;

    @Override
    public void diffListener(final DiffType diffType, final long newNodeKey, final long oldNodeKey,
        final DiffDepth depth) {
      emitted.add(new Emitted(diffType, newNodeKey, oldNodeKey, depth.getNewDepth(), depth.getOldDepth()));
    }

    @Override
    public void diffDone() {
      done = true;
    }
  }

  @Test
  void builderDefaultsToTheResourcesOwnHashKind() {
    try (final var database = database("hashless", HashType.NONE);
        final var session = database.beginResourceSession(RESOURCE)) {
      assertSame(HashType.NONE, new DiffFactory.Builder<JsonNodeReadOnlyTrx, JsonNodeTrx>(session, 1, 1,
          DiffOptimized.HASHED, Set.of(new Recorder())).hashKind);
    }
    try (final var database = database("hashed", HashType.ROLLING);
        final var session = database.beginResourceSession(RESOURCE)) {
      assertSame(HashType.ROLLING, new DiffFactory.Builder<JsonNodeReadOnlyTrx, JsonNodeTrx>(session, 1, 1,
          DiffOptimized.HASHED, Set.of(new Recorder())).hashKind);
    }
  }

  /**
   * A caller that asks for the hash shortcut on a hashless resource must still get the full
   * comparison. Without the guard this emits a single {@code SAMEHASH} and loses the update.
   */
  @Test
  void hashShortcutIsRefusedOnAHashlessResource() {
    try (final var database = database("hashless", HashType.NONE);
        final var session = database.beginResourceSession(RESOURCE)) {
      twoRevisions(session);
      final List<Emitted> forced = diff(session, DiffOptimized.HASHED, HashType.ROLLING);
      final List<Emitted> plain = diff(session, DiffOptimized.NO, HashType.NONE);
      assertEquals(plain, forced, "forcing hashKind(ROLLING) on a hashless resource changed the diff");
      assertFalse(forced.stream().anyMatch(e -> e.type() == DiffType.SAMEHASH),
          "the hash shortcut was taken on a resource that stores no hashes");
      assertTrue(forced.stream().anyMatch(e -> e.type() == DiffType.UPDATED), "the update must be reported");
    }
  }

  /** The permitted direction: forcing the full comparison on a hashed resource still works. */
  @Test
  void fullComparisonCanStillBeForcedOnAHashedResource() {
    try (final var database = database("hashed", HashType.ROLLING);
        final var session = database.beginResourceSession(RESOURCE)) {
      twoRevisions(session);
      final List<Emitted> optimized = diff(session, DiffOptimized.HASHED, HashType.ROLLING);
      final List<Emitted> full = diff(session, DiffOptimized.HASHED, HashType.NONE);
      final List<Emitted> plain = diff(session, DiffOptimized.NO, HashType.ROLLING);
      assertEquals(plain, full, "hashKind(NONE) must force the full comparison even when HASHED is requested");
      assertFalse(full.stream().anyMatch(e -> e.type() == DiffType.SAMEHASH),
          "hashKind(NONE) must suppress the shortcut");
      assertTrue(optimized.stream().anyMatch(e -> e.type() == DiffType.SAMEHASH),
          "the hashed resource must actually take the shortcut, else this proves nothing");
      assertTrue(full.stream().anyMatch(e -> e.type() == DiffType.UPDATED));
      assertTrue(optimized.stream().anyMatch(e -> e.type() == DiffType.UPDATED));
    }
  }

  private static List<Emitted> diff(final JsonResourceSession session, final DiffOptimized optimized,
      final HashType hashKind) {
    final Recorder recorder = new Recorder();
    DiffFactory.invokeJsonDiff(
        new DiffFactory.Builder<JsonNodeReadOnlyTrx, JsonNodeTrx>(session, 2, 1, optimized, Set.of(recorder)).hashKind(
            hashKind).skipSubtrees(true));
    assertTrue(recorder.done, "the diff must signal completion");
    return recorder.emitted;
  }

  private static void twoRevisions(final JsonResourceSession session) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
      writer.commit();
      assertTrue(writer.moveToDocumentRoot());
      assertTrue(writer.moveToFirstChild());
      assertTrue(writer.moveToFirstChild());
      while (writer.getName() == null || !"e".equals(writer.getName().getLocalName())) {
        assertTrue(writer.moveToRightSibling(), "the document must contain the record \"e\"");
      }
      writer.setStringValue("changed");
      writer.commit();
    }
  }

  private Database<JsonResourceSession> database(final String name, final HashType hashType) {
    final Path path = directory.resolve(name);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openJsonDatabase(path);
    database.createResource(
        ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).hashKind(hashType).build());
    return database;
  }
}
