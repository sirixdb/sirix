package io.sirix.access;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Database;
import io.sirix.api.RevisionInfo;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the open-path caches added for request-scoped (open per request) usage:
 * <ol>
 *   <li>database-id claim stability — sequential opens of the same directory must keep the one
 *       persisted id (global cache keys depend on it),</li>
 *   <li>the {@code dbsetting.obj} deserialize cache — fresh configuration objects per call,
 *       invalidated by {@code serialize()},</li>
 *   <li>the global revision-info cache — {@code getHistory()} answers identically across two
 *       separate sessions of the same resource.</li>
 * </ol>
 */
final class DatabaseOpenCachingTest {

  private static final String RESOURCE = "open-caching-resource";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("two sequential opens of the same database see the same persisted database id")
  void sequentialOpensKeepDatabaseId() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final long persistedId = DatabaseConfiguration.deserialize(dbPath).getDatabaseId();
    assertTrue(persistedId > 0, "creation must mint a positive database id");

    final long firstOpenId;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      firstOpenId = database.getDatabaseConfig().getDatabaseId();
    }
    final long secondOpenId;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      secondOpenId = database.getDatabaseConfig().getDatabaseId();
    }

    assertEquals(persistedId, firstOpenId, "first open must use the persisted id");
    assertEquals(firstOpenId, secondOpenId, "reopen must neither re-mint nor re-key the id");
    assertEquals(persistedId,
                 DatabaseConfiguration.deserialize(dbPath).getDatabaseId(),
                 "the persisted id must be unchanged after both opens");
  }

  @Test
  @DisplayName("concurrent opens of the same database neither re-mint nor re-key the id")
  void concurrentOpensKeepDatabaseId() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    final long persistedId = DatabaseConfiguration.deserialize(dbPath).getDatabaseId();

    final List<Long> openIds = openConcurrently(dbPath, 8);
    for (final long id : openIds) {
      assertEquals(persistedId, id, "every concurrent open must see the one persisted id");
    }
  }

  @Test
  @DisplayName("concurrent opens of a legacy id-0 database mint exactly one id")
  void concurrentOpensOfLegacyDatabaseMintSingleId() throws Exception {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    // Simulate a config written before database ids existed: id resolution happens at open.
    final DatabaseConfiguration legacy = DatabaseConfiguration.deserialize(dbPath);
    legacy.setDatabaseId(0);
    DatabaseConfiguration.serialize(legacy);

    final List<Long> openIds = openConcurrently(dbPath, 8);
    final long mintedId = openIds.get(0);
    assertTrue(mintedId > 0, "open must mint a positive id for a legacy config");
    for (final long id : openIds) {
      assertEquals(mintedId, id, "racing opens must not give the same directory two live ids");
    }
    assertEquals(mintedId,
                 DatabaseConfiguration.deserialize(dbPath).getDatabaseId(),
                 "the minted id must be the persisted one");
  }

  private static List<Long> openConcurrently(final Path dbPath, final int threads) throws Exception {
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final CyclicBarrier barrier = new CyclicBarrier(threads);
      final List<Future<Long>> futures = new ArrayList<>(threads);
      for (int i = 0; i < threads; i++) {
        futures.add(pool.submit(() -> {
          barrier.await(30, TimeUnit.SECONDS);
          try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
            return database.getDatabaseConfig().getDatabaseId();
          }
        }));
      }
      final List<Long> ids = new ArrayList<>(threads);
      for (final Future<Long> future : futures) {
        ids.add(future.get(30, TimeUnit.SECONDS));
      }
      return ids;
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  @DisplayName("deserialize returns fresh equal configs and observes serialize() immediately")
  void deserializeCacheReturnsFreshConfigsAndInvalidatesOnSerialize() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    final DatabaseConfiguration first = DatabaseConfiguration.deserialize(dbPath);
    final DatabaseConfiguration second = DatabaseConfiguration.deserialize(dbPath);
    assertNotSame(first, second, "every call must construct a fresh configuration object");
    assertEquals(first.getDatabaseId(), second.getDatabaseId());
    assertEquals(first.getMaxResourceID(), second.getMaxResourceID());
    assertEquals(first.getDatabaseType(), second.getDatabaseType());
    assertEquals(first.getMaxSegmentAllocationSize(), second.getMaxSegmentAllocationSize());

    // Mutating a returned instance must not poison the cache (values are cached, not the object).
    second.setMaximumResourceID(second.getMaxResourceID() + 999);
    assertEquals(first.getMaxResourceID(),
                 DatabaseConfiguration.deserialize(dbPath).getMaxResourceID(),
                 "an unserialized mutation must not be visible to other callers");

    // A persisted change must be observed by the very next deserialize (explicit invalidation
    // makes this robust to coarse mtime granularity).
    first.setMaximumResourceID(first.getMaxResourceID() + 7);
    DatabaseConfiguration.serialize(first);
    assertEquals(first.getMaxResourceID(),
                 DatabaseConfiguration.deserialize(dbPath).getMaxResourceID(),
                 "deserialize after serialize() must return the persisted change");
  }

  @Test
  @DisplayName("getHistory across two separate sessions returns equal revision infos")
  void historyAcrossSeparateSessionsIsEqual() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"v\":1}"), JsonNodeTrx.Commit.NO);
          wtx.commit("first");
        }
        try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // object
          wtx.moveToFirstChild(); // object key "v"
          wtx.moveToFirstChild(); // number value 1
          wtx.setNumberValue(2);
          wtx.commit("second");
        }
      }
    }

    final List<RevisionInfo> firstHistory = readHistoryInFreshSession(dbPath);
    final List<RevisionInfo> secondHistory = readHistoryInFreshSession(dbPath);

    assertEquals(2, firstHistory.size(), "two committed revisions expected");
    assertEquals(firstHistory.size(), secondHistory.size());
    for (int i = 0; i < firstHistory.size(); i++) {
      final RevisionInfo fresh = firstHistory.get(i);
      final RevisionInfo cached = secondHistory.get(i);
      assertEquals(fresh.getRevision(), cached.getRevision());
      assertEquals(fresh.getRevisionTimestamp(), cached.getRevisionTimestamp());
      assertEquals(fresh.getCommitMessage(), cached.getCommitMessage());
      assertEquals(fresh.getUser().getName(), cached.getUser().getName());
    }
    // Newest-first ordering with the commit messages we wrote.
    assertEquals("second", firstHistory.get(0).getCommitMessage().orElseThrow());
    assertEquals("first", firstHistory.get(1).getCommitMessage().orElseThrow());
  }

  /** Opens a fresh database object + session per call so nothing session-scoped can be reused. */
  private static List<RevisionInfo> readHistoryInFreshSession(final Path dbPath) {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return session.getHistory();
    }
  }
}
