package io.sirix.access.trx.node.json;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.JsonDiffIntegrity;
import io.sirix.io.StorageType;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hashes and hash-pruned diffs must survive leaf updates, structural edits, and cold revision
 * reads.
 */
@Isolated
final class JsonLeafHashPersistenceTest {
  private static final String RESOURCE = "leaves";

  @TempDir
  Path directory;

  static Stream<Arguments> configurations() {
    return Stream.of(VersioningType.values())
                 .flatMap(versioning -> Stream.of(HashType.ROLLING, HashType.POSTORDER)
                                              .map(hash -> Arguments.of(versioning, hash)));
  }

  @ParameterizedTest
  @MethodSource("configurations")
  void hashesAndDiffsSurviveColdReconstruction(final VersioningType versioning, final HashType hash) throws Exception {
    final Path path = directory.resolve("database");
    final List<long[]> hashes = new ArrayList<>(6);
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .hashKind(hash)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .useDeweyIDs(true)
                                                   .storeDiffs(true)
                                                   .build());
      try (final var session = database.beginResourceSession(RESOURCE); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(
                "{\"left\":{\"number\":10,\"string\":\"old\",\"boolean\":true,\"null\":null},\"right\":{}}"),
            JsonNodeTrx.Commit.NO);
        commit(writer, hashes, hash);

        moveToField(writer, "number");
        writer.setNumberValue(10.0d);
        moveToField(writer, "string");
        writer.setStringValue("Grüße 🌲");
        moveToField(writer, "boolean");
        writer.setBooleanValue(false);
        moveToField(writer, "null");
        writer.setObjectKeyName("renamedNull");
        commit(writer, hashes, hash);

        moveToField(writer, "boolean");
        final long moved = writer.getNodeKey();
        moveToField(writer, "right");
        writer.moveSubtreeToFirstChild(moved);
        commit(writer, hashes, hash);

        // Beyond double's exact integer range, then a scaled decimal and a large integer.
        for (final Number number : List.of(9_007_199_254_740_993L, new BigDecimal("12345678901234567890.00100"),
            new BigInteger("123456789012345678901234567890"))) {
          moveToField(writer, "number");
          writer.setNumberValue(number);
          commit(writer, hashes, hash);
        }
        for (int revision = 2; revision <= hashes.size(); revision++) {
          assertDiffIntegrity(session, revision);
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(path);
        final var session = database.beginResourceSession(RESOURCE)) {
      for (int revision = 1; revision <= hashes.size(); revision++) {
        try (final var reader = session.beginNodeReadOnlyTrx(revision)) {
          JsonStructuralHashInvariantTest.assertGraph(reader, hash);
          assertArrayEquals(hashes.get(revision - 1), captureHashes(reader), "revision " + revision);
        }
        if (revision > 1) {
          assertDiffIntegrity(session, revision);
        }
      }
    }
  }

  private static void commit(final JsonNodeTrx writer, final List<long[]> hashes, final HashType hash) {
    JsonStructuralHashInvariantTest.assertGraph(writer, hash);
    final long[] before = captureHashes(writer);
    writer.commit();
    JsonStructuralHashInvariantTest.assertGraph(writer, hash);
    assertArrayEquals(before, captureHashes(writer));
    hashes.add(before);
  }

  private static long[] captureHashes(final JsonNodeReadOnlyTrx reader) {
    final long[] hashes = new long[Math.toIntExact(reader.getMaxNodeKey()) + 1];
    for (int key = 0; key < hashes.length; key++) {
      if (reader.moveTo(key)) {
        hashes[key] = reader.getHash();
      }
    }
    return hashes;
  }

  private static void moveToField(final JsonNodeTrx writer, final String name) {
    for (long key = 1, max = writer.getMaxNodeKey(); key <= max; key++) {
      if (writer.moveTo(key) && writer.isObjectKey() && name.equals(writer.getName().getLocalName())) {
        return;
      }
    }
    throw new AssertionError("No field named " + name);
  }

  private static void assertDiffIntegrity(final JsonResourceSession session, final int revision) throws Exception {
    final Path file = session.getResourceConfig()
                             .getResource()
                             .resolve(ResourceConfiguration.ResourcePaths.UPDATE_OPERATIONS.getPath())
                             .resolve("diffFromRev" + (revision - 1) + "toRev" + revision + ".json");
    final JsonObject stored = JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    JsonDiffIntegrity.validate(stored);
    assertFalse(stored.getAsJsonArray("diffs").isEmpty());
    final JsonObject generated = JsonParser
                                           .parseString(new BasicJsonDiff("database").generateDiff(session,
                                               revision - 1, revision, 0, Long.MAX_VALUE))
                                           .getAsJsonObject();
    assertTrue(generated.getAsJsonArray("diffs").size() > 0, "hash-pruned diff must detect each changed revision");
    assertEquals(revision, generated.get("new-revision").getAsInt());
  }
}
