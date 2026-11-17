package io.sirix.access;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Node hashes are off unless a caller asks for them (report §10 call 2, answered E2.(a)).
 *
 * <p>
 * Clauses B1, B2, B6 and invariant I1 of the acceptance contract: the builder default is
 * {@link HashType#NONE}, the {@code sirix.hashType} property still overrides it, a resource created
 * with hashes keeps them across a reopen, and a hashless resource stores no hash at all.
 */
final class HashDefaultOffTest {

  @TempDir
  private Path directory;

  private static final String DOCUMENT = "{\"name\":\"Jane\",\"born\":\"1987-03-14\"}";

  @Test
  void builderDefaultsToNone() {
    assertSame(HashType.NONE, ResourceConfiguration.DEFAULT_HASH_TYPE,
        "the 1.0 default is no node hashing; see ResourceConfiguration.DEFAULT_HASH_TYPE");
    assertSame(HashType.NONE, ResourceConfiguration.newBuilder("resource").build().hashType,
        "a resource built without hashKind(...) must not maintain hashes");
  }

  @Test
  void explicitHashKindStillWins() {
    assertSame(HashType.ROLLING,
        ResourceConfiguration.newBuilder("resource").hashKind(HashType.ROLLING).build().hashType);
    assertSame(HashType.POSTORDER,
        ResourceConfiguration.newBuilder("resource").hashKind(HashType.POSTORDER).build().hashType);
  }

  /**
   * B2. The property is read once at class-initialization time, so the resolution itself is what a
   * test can check: unset means off, a named kind means that kind, and a misspelling is refused
   * rather than silently falling back.
   */
  @Test
  void systemPropertyStillOverridesTheDefault() {
    assertSame(HashType.NONE, ResourceConfiguration.defaultHashType(null));
    assertSame(HashType.ROLLING, ResourceConfiguration.defaultHashType("ROLLING"));
    assertSame(HashType.ROLLING, ResourceConfiguration.defaultHashType("rolling"));
    assertSame(HashType.POSTORDER, ResourceConfiguration.defaultHashType("POSTORDER"));
    assertSame(HashType.NONE, ResourceConfiguration.defaultHashType("NONE"));
    assertThrows(IllegalArgumentException.class, () -> ResourceConfiguration.defaultHashType("bogus"));
    assertThrows(IllegalArgumentException.class, () -> ResourceConfiguration.defaultHashType(""));
    assertEquals("sirix.hashType", ResourceConfiguration.HASH_TYPE_PROPERTY);
  }

  /**
   * I1. A hashless resource stores no hash for its containers — which is the residue that matters,
   * because a container's hash is the aggregate that depends on its descendants' values.
   *
   * <p>
   * A primitive leaf is different: {@code getHash()} <em>derives</em> a hash from the record's own
   * content when none is stored (see {@code StringNode#getHash}), so it answers non-zero even here.
   * That derivation is computed from the value a caller already holds, so it stores nothing and
   * aggregates nothing; only the container's stored hash can confirm a guess at a deleted descendant.
   */
  @Test
  void noneResourceStoresNoHash() {
    final Path path = directory.resolve("hashless");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build());
      try (final var session = database.beginResourceSession("resource")) {
        assertSame(HashType.NONE, session.getResourceConfig().hashType);
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveToFirstChild());
          assertEquals(0L, reader.getHash(), "the container of a hashless resource must store no hash");
          assertEquals(0L, reader.getDescendantCount(),
              "descendant counts are stored only with hashes, so a hashless container keeps none");
        }
      }
    }
  }

  /**
   * B6. No migration: the hash kind lives in the resource settings, so a store created with rolling
   * hashes still has them after the default changed — and still maintains them on a later commit.
   */
  @Test
  void persistedRollingSurvivesTheNewDefault() {
    final Path path = directory.resolve("hashed");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.ROLLING)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    final long firstHash;
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession("resource")) {
      assertSame(HashType.ROLLING, session.getResourceConfig().hashType,
          "the persisted hash kind must be read back, not replaced by the new default");
      try (final var reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        firstHash = reader.getHash();
        assertNotEquals(0L, firstHash, "a resource created with rolling hashes must still carry them");
      }
      // And hashing keeps running on a commit made after the default changed.
      try (final var writer = session.beginNodeTrx()) {
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        writer.setObjectKeyName("renamed");
        writer.commit();
      }
      try (final var reader = session.beginNodeReadOnlyTrx()) {
        assertTrue(reader.moveToFirstChild());
        assertNotEquals(0L, reader.getHash());
        assertNotEquals(firstHash, reader.getHash(), "the rolling hash must still be maintained on commit");
      }
    }
  }
}
