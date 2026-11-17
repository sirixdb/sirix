package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonHashingWorkProbe;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invariant I3 of the acceptance contract: turning node hashes off removes commit work, it never
 * adds any.
 *
 * <p>
 * The same edit — append one leaf to a 4096-element array, then commit — is driven once on a
 * {@code ROLLING} resource and once on a hashless one, with {@link JsonHashingWorkProbe} counting
 * the record reads and writes that hash maintenance itself performs. The hashless commit must do
 * none of that work, and the hashed commit must do some, or the counter would prove nothing. The
 * stored bytes are compared too: a hashless record omits the hash field, so the resource cannot
 * grow.
 *
 * <p>
 * Counted work only — no wall clock, so the bound holds on any machine. Mutation checks: making
 * {@code AbstractNodeTrxImpl} run hash maintenance for {@code HashType.NONE} breaks the zero
 * bounds; making it skip maintenance for {@code ROLLING} breaks the {@code assertAtLeast} bounds
 * that keep this test honest.
 */
@Isolated
final class CommitHashWorkBudgetTest {

  private static final String RESOURCE = "resource";

  private static final int ELEMENTS = 4096;

  @TempDir
  private Path directory;

  @Test
  void aHashlessCommitDoesNoHashMaintenance() throws Exception {
    final Work hashed = commitWork(HashType.ROLLING);
    final Work hashless = commitWork(HashType.NONE);

    assertTrue(hashed.reads() > 0,
        "the hashed arm must read records for hash maintenance, else the counters prove nothing");
    assertTrue(hashed.writes() > 0, "the hashed arm must rewrite the ancestor records it rehashes");

    assertEquals(0L, hashless.reads(), "a hashless commit read records for hash maintenance");
    assertEquals(0L, hashless.writes(), "a hashless commit rewrote records for hash maintenance");

    assertTrue(hashless.reads() <= hashed.reads() && hashless.writes() <= hashed.writes(),
        "dropping hash maintenance must not add commit work");
    assertTrue(hashless.bytes() <= hashed.bytes(), "a hashless resource stores no hash field, so it cannot be larger: "
        + hashless.bytes() + " > " + hashed.bytes());
  }

  /**
   * What one commit cost: hash-maintenance record reads and writes, and the bytes on disk after it.
   */
  private record Work(long reads, long writes, long bytes) {
  }

  private Work commitWork(final HashType hashType) throws Exception {
    final Path path = directory.resolve("store-" + hashType.name().toLowerCase(Locale.ROOT));
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(hashType)
                                                   .useDeweyIDs(false)
                                                   .build());
      try (final var session = database.beginResourceSession(RESOURCE); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(array()), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    final long reads;
    final long writes;
    try (final var database = Databases.openJsonDatabase(path);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final var writer = session.beginNodeTrx();
        final var probe = new JsonHashingWorkProbe(writer, writer.getMaxNodeKey())) {
      final var readCounter =
          WorkCounter.alwaysOn("hashRecordReads", "one record read during hash maintenance", probe::reads);
      final var writeCounter =
          WorkCounter.alwaysOn("hashRecordWrites", "one record rewritten during hash maintenance", probe::writes);
      final var work = WorkCapture.of(readCounter, writeCounter).run(() -> {
        // An append, not a value update: hash maintenance for an insert runs through
        // JsonHashingMutation, which is the writer JsonHashingWorkProbe counts. A value update
        // repairs the ancestor chain elsewhere, so the probe saw nothing and this test's own
        // honesty check ("the hashed arm must read records") correctly refused to pass.
        assertTrue(writer.moveTo(1), "the array must exist");
        writer.insertNumberValueAsLastChild(-1);
        writer.commit();
      });
      reads = work.of(readCounter);
      writes = work.of(writeCounter);
    }
    Databases.clearGlobalCaches();
    return new Work(reads, writes, bytes(path));
  }

  private static String array() {
    final StringBuilder builder = new StringBuilder("[");
    for (int index = 0; index < ELEMENTS; index++) {
      if (index != 0) {
        builder.append(',');
      }
      builder.append(index);
    }
    return builder.append(']').toString();
  }

  private static long bytes(final Path path) throws IOException {
    try (final Stream<Path> files = Files.walk(path)) {
      return files.sorted(Comparator.naturalOrder()).filter(Files::isRegularFile).mapToLong(file -> {
        try {
          return Files.size(file);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      }).sum();
    }
  }
}
