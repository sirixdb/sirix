package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Correctness gate for the low-latency preallocated-commit profile
 * ({@code -Dsirix.commit.preallocated=true}) on the FILE_CHANNEL backend.
 *
 * <p>The profile preallocates the data/revisions files so per-commit writes never extend
 * {@code i_size}, then commits with {@code fdatasync} ({@code force(false)}) instead of {@code fsync}
 * ({@code force(true)}) — on a non-growing file this skips the ext4/xfs metadata-journal commit that
 * dominates the per-commit latency. The logical write frontier is derived from the durable revision
 * graph (the last revision root), NOT from the preallocation-inflated physical file size.
 *
 * <p>Integrity is checked the unambiguous way — serialize the resource to JSON and compare the
 * preallocated path byte-for-byte against the legacy path — rather than via a single scalar that
 * could pass vacuously.
 */
final class PreallocatedCommitTest {

  private static final String PREALLOC_PROP = "sirix.commit.preallocated";
  private static final String CHUNK_PROP = "sirix.commit.preallocChunkBytes";
  private static final String RESOURCE = "prealloc-resource";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(PREALLOC_PROP);
    System.clearProperty(CHUNK_PROP);
    JsonTestHelper.deleteEverything();
  }

  private static Path dataFilePath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
        .resolve("sirix.data");
  }

  private static Path commitMarkerPath(final Path databasePath, final String resourceName) {
    return databasePath
        .resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile())
        .resolve(resourceName)
        .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
        .resolve(".commit");
  }

  /** Build a JSON array literal {@code [0,1,...,n-1]}. */
  private static String jsonArray(final int n) {
    final StringBuilder sb = new StringBuilder(n * 4 + 2);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(i);
    }
    sb.append(']');
    return sb.toString();
  }

  private static ResourceConfiguration.Builder fileChannelResource(final String name) {
    return ResourceConfiguration.newBuilder(name)
        .storeDiffs(false)
        .hashKind(HashType.NONE)
        .buildPathSummary(false)
        .versioningApproach(VersioningType.FULL)
        .storageType(StorageType.FILE_CHANNEL);
  }

  private static int latestRevision(final JsonResourceSession session) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      return rtx.getRevisionNumber();
    }
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    final StringWriter writer = new StringWriter();
    new JsonSerializer.Builder(session, writer, revision).build().call();
    return writer.toString();
  }

  /**
   * Build the same history on {@code resource} under the given preallocation setting — insert a
   * sizable array, a couple of empty commits, a session reopen, then one more commit — and return the
   * serialized JSON of the latest revision.
   */
  private String buildHistoryAndSerializeLatest(final Database<JsonResourceSession> db, final String resource,
      final boolean preallocated) throws Exception {
    System.setProperty(PREALLOC_PROP, Boolean.toString(preallocated));
    db.createResource(fileChannelResource(resource).build());

    try (final JsonResourceSession session = db.beginResourceSession(resource)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonArray(2_000)));
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.commit();
      }
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.commit();
      }
    }
    // Reopen: a fresh writer must re-derive the frontier from the durable revision graph and append.
    try (final JsonResourceSession session = db.beginResourceSession(resource)) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.commit();
      }
    }
    try (final JsonResourceSession session = db.beginResourceSession(resource)) {
      return serialize(session, latestRevision(session));
    }
  }

  @Test
  @DisplayName("preallocated profile yields byte-identical JSON to legacy across commits + reopen")
  void preallocated_matchesLegacy_acrossCommitsAndReopen() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final String legacyJson;
    final String preallocJson;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      legacyJson = buildHistoryAndSerializeLatest(db, "legacy-res", false);
      preallocJson = buildHistoryAndSerializeLatest(db, "prealloc-res", true);
    }

    assertTrue(preallocJson.length() > 2_000,
        "sanity: serialized output must be substantial, not empty (got " + preallocJson.length() + " chars)");
    assertEquals(legacyJson, preallocJson,
        "preallocated commit must produce logically-identical data to the legacy path");
  }

  @Test
  @DisplayName("preallocated profile reuses its preallocation across reopens (no compounding)")
  void preallocated_doesNotCompoundAcrossReopens() throws Exception {
    // 8 MiB chunk so the first commit preallocates a clearly-measurable region. The bug this guards:
    // per-transaction writers re-deriving the frontier from an inflated channel.size() would stack a
    // fresh 8 MiB chunk on EVERY reopen (48 MiB+ after several commits). The derive-from-revision-graph
    // frontier must reuse the existing preallocation, so the file size must stay flat.
    final long chunk = 8L * 1024 * 1024;
    System.setProperty(CHUNK_PROP, Long.toString(chunk));
    System.setProperty(PREALLOC_PROP, "true");

    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    final Path dataFile = dataFilePath(dbPath, RESOURCE);

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(fileChannelResource(RESOURCE).build());
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonArray(100)));
          wtx.commit();
        }
      }
      final long afterFirst = Files.size(dataFile);

      for (int i = 0; i < 8; i++) {
        try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.commit();
          }
        }
      }
      final long afterMany = Files.size(dataFile);

      assertEquals(afterFirst, afterMany,
          "preallocation must be reused across reopens, not compounded; file grew from " + afterFirst
              + " to " + afterMany + " bytes over 8 tiny commits");
      assertTrue(afterMany <= 2 * chunk,
          "preallocated file must stay bounded to ~1 chunk for a tiny resource; was " + afterMany);
    }
  }

  @Test
  @DisplayName("preallocated profile: crash-tail garbage is recovered on reopen, committed data preserved")
  void preallocated_crashTail_recoveredOnReopen() throws Exception {
    System.setProperty(PREALLOC_PROP, "true");
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(fileChannelResource(RESOURCE).build());

      final int targetRevision;
      final String preCrashJson;
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonArray(500)));
          wtx.commit();
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
        targetRevision = latestRevision(session);
        preCrashJson = serialize(session, targetRevision);
      }
      assertTrue(preCrashJson.length() > 500, "sanity: pre-crash serialization is substantial");

      // Forge a crash mid-commit: garbage tail past the last good revision + the .commit marker.
      final Path dataFile = dataFilePath(dbPath, RESOURCE);
      final byte[] garbage = new byte[8192];
      Arrays.fill(garbage, (byte) 0xCC);
      try (final var ch = java.nio.channels.FileChannel.open(dataFile,
          StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
        ch.write(java.nio.ByteBuffer.wrap(garbage));
      }
      Files.createFile(commitMarkerPath(dbPath, RESOURCE));

      // Reopen + a new write trx triggers writer.truncateTo(...), which must reset the tracked frontier
      // so the following preallocated writes land at the recovered data end, not past the garbage.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }

      // The pre-crash revision must serialize byte-identically after recovery.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        assertEquals(preCrashJson, serialize(session, targetRevision),
            "the last pre-crash revision must be byte-identical after preallocated-profile recovery");
      }
    }
  }

  /**
   * Gated micro-benchmark (enable with {@code -Dsirix.benchmark.commit=true}): commit throughput of
   * the FileChannel backend, legacy {@code fsync}-on-growing-file vs preallocated {@code fdatasync}.
   * Measures the real per-commit path (serialize + hash + barrier), so the speedup is smaller than
   * the barrier-only microbenchmark but reflects the actual end-to-end gain.
   */
  @Test
  @DisplayName("[benchmark, gated] FileChannel commit throughput: legacy vs preallocated")
  void commitThroughput_legacyVsPreallocated() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(Boolean.getBoolean("sirix.benchmark.commit"),
        "gated benchmark; enable with -Dsirix.benchmark.commit=true");

    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    final int warmup = 300;
    final int n = 3_000;
    final double legacy;
    final double prealloc;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      legacy = measureCommitsPerSecond(db, "bench-legacy", false, warmup, n);
      prealloc = measureCommitsPerSecond(db, "bench-prealloc", true, warmup, n);
    }
    System.out.printf("[BENCH] FileChannel commit throughput: legacy=%.0f/s  preallocated=%.0f/s  speedup=%.2fx%n",
        legacy, prealloc, prealloc / legacy);
  }

  private double measureCommitsPerSecond(final Database<JsonResourceSession> db, final String resource,
      final boolean preallocated, final int warmup, final int n) throws Exception {
    System.setProperty(PREALLOC_PROP, Boolean.toString(preallocated));
    try {
      db.createResource(fileChannelResource(resource).build());
      try (final JsonResourceSession session = db.beginResourceSession(resource)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(jsonArray(10)));
          wtx.commit();
        }
        for (int i = 0; i < warmup; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.commit();
          }
        }
        final long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
          try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.commit();
          }
        }
        final long t1 = System.nanoTime();
        return n / ((t1 - t0) / 1e9);
      }
    } finally {
      System.clearProperty(PREALLOC_PROP);
    }
  }
}
