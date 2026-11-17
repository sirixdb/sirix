package io.sirix.diff;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.BasicJsonDiff;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clause B8 of the acceptance contract: the diff a caller sees must not depend on whether the
 * resource stores node hashes.
 *
 * <p>
 * With hashes on, {@code AbstractDiff} may take the {@code optimizedDiff} route and emit
 * {@code SAMEHASH} for a subtree whose stored hash is unchanged; without hashes it compares every
 * node. Both routes must report exactly the same differences. The test drives the same edit history
 * onto a {@code ROLLING} resource and a {@code NONE} resource and compares the serialized diff of
 * every revision pair, byte for byte — a differential test, so neither route is trusted as the
 * oracle.
 *
 * <p>
 * {@link #randomHistoriesAgree(long)} is the explorative arm: it generates an edit script from a
 * seeded random walk over the hashless resource and replays the identical script on the hashed one,
 * checking the node kind at every recorded key so a structural divergence is caught before the
 * diffs are compared.
 */
final class JsonDiffHashEquivalenceTest {

  /** Both resources carry the same names, so the serialized diff differs only where the diff does. */
  private static final String DATABASE = "equivalence";

  private static final String RESOURCE = "resource";

  private static final String BASE_DOCUMENT =
      "{\"a\":[1,2,3],\"b\":{\"c\":\"x\",\"d\":[true,null]},\"e\":\"keep\",\"f\":42}";

  @TempDir
  private Path directory;

  /**
   * A recorded edit: move to {@code nodeKey}, which must be of {@code kind}, then apply {@code op}.
   */
  private record Edit(long nodeKey, NodeKind kind, Op op, String payload) {
  }

  private enum Op {
    SET_STRING, SET_NUMBER, SET_BOOLEAN, RENAME, INSERT_NUMBER_FIRST_CHILD, INSERT_STRING_LAST_CHILD, REMOVE
  }

  @Test
  void handWrittenHistoryAgrees() {
    assertDiffsAgree(JsonDiffHashEquivalenceTest::handWrittenHistory);
  }

  @ParameterizedTest
  @ValueSource(longs = {1L, 2L, 3L, 20260915L, -7L})
  void randomHistoriesAgree(final long seed) {
    final List<List<Edit>> script = new ArrayList<>();
    assertDiffsAgree(session -> {
      if (script.isEmpty()) {
        recordRandomHistory(session, new Random(seed), script);
      } else {
        replay(session, script);
      }
    });
  }

  /**
   * Runs {@code history} on a hashed and on a hashless resource and asserts that every revision
   * pair's diff is identical. Also asserts that the two resources really did take different routes:
   * the hashed one stores hashes, the hashless one does not.
   */
  private void assertDiffsAgree(final Consumer<JsonResourceSession> history) {
    final int revisions;
    final List<String> hashed = new ArrayList<>();
    final List<String> hashless = new ArrayList<>();
    try (final var hashedDatabase = database("hashed", HashType.ROLLING);
        final var hashlessDatabase = database("hashless", HashType.NONE)) {
      try (final var session = hashlessDatabase.beginResourceSession(RESOURCE)) {
        assertSame(HashType.NONE, session.getResourceConfig().hashType);
        history.accept(session);
        revisions = session.getMostRecentRevisionNumber();
        assertTrue(revisions > 1, "a diff needs at least two revisions");
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveToFirstChild());
          assertEquals(0L, reader.getHash(), "the hashless arm must store no hash");
        }
        collectDiffs(session, revisions, hashless);
      }
      try (final var session = hashedDatabase.beginResourceSession(RESOURCE)) {
        assertSame(HashType.ROLLING, session.getResourceConfig().hashType);
        history.accept(session);
        assertEquals(revisions, session.getMostRecentRevisionNumber(), "both arms must build the same history");
        try (final var reader = session.beginNodeReadOnlyTrx()) {
          assertTrue(reader.moveToFirstChild());
          assertNotEquals(0L, reader.getHash(), "the hashed arm must store a hash, else it proves nothing");
        }
        collectDiffs(session, revisions, hashed);
      }
    }
    assertEquals(hashless.size(), hashed.size());
    for (int index = 0; index < hashed.size(); index++) {
      assertEquals(hashless.get(index), hashed.get(index),
          "diff " + index + " differs between a hashed and a hashless resource");
      assertFalse(hashed.get(index).contains("SAMEHASH"), "SAMEHASH must never reach the serialized diff");
    }
  }

  private static void collectDiffs(final JsonResourceSession session, final int revisions, final List<String> into) {
    for (int oldRevision = 1; oldRevision <= revisions; oldRevision++) {
      for (int newRevision = oldRevision; newRevision <= revisions; newRevision++) {
        into.add(new BasicJsonDiff(DATABASE).generateDiff(session, oldRevision, newRevision));
        into.add(new BasicJsonDiff(DATABASE).generateDiff(session, oldRevision, newRevision, 0, 0, false));
      }
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

  /**
   * Five revisions that between them exercise an untouched subtree (the hash shortcut's reason to
   * exist), a value update, an insert, a rename and a delete.
   */
  private static void handWrittenHistory(final JsonResourceSession session) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(BASE_DOCUMENT), JsonNodeTrx.Commit.NO);
      writer.commit();

      // Update one leaf; "b" and its subtree stay untouched.
      assertTrue(moveToObjectRecord(writer, "e"));
      writer.setStringValue("changed");
      writer.commit();

      // Insert into the array "a".
      assertTrue(moveToObjectRecord(writer, "a"));
      writer.insertNumberValueAsFirstChild(0);
      writer.commit();

      // Rename a record.
      assertTrue(moveToObjectRecord(writer, "f"));
      writer.setObjectKeyName("renamed");
      writer.commit();

      // Delete a record with a subtree.
      assertTrue(moveToObjectRecord(writer, "b"));
      writer.remove();
      writer.commit();
    }
  }

  /** Moves to the object record named {@code key} directly under the root object. */
  private static boolean moveToObjectRecord(final JsonNodeTrx writer, final String key) {
    writer.moveToDocumentRoot();
    if (!writer.moveToFirstChild()) {
      return false;
    }
    if (!writer.moveToFirstChild()) {
      return false;
    }
    do {
      final QNm name = writer.getName();
      if (name != null && key.equals(name.getLocalName())) {
        return true;
      }
    } while (writer.moveToRightSibling());
    return false;
  }

  /**
   * Builds a random edit history on {@code session} and records it into {@code script}, one list of
   * edits per revision.
   */
  private static void recordRandomHistory(final JsonResourceSession session, final Random random,
      final List<List<Edit>> script) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(BASE_DOCUMENT), JsonNodeTrx.Commit.NO);
      writer.commit();
      for (int revision = 0; revision < 6; revision++) {
        final List<Edit> edits = new ArrayList<>();
        final int attempts = 1 + random.nextInt(3);
        for (int attempt = 0; attempt < attempts; attempt++) {
          final long key = 1 + (long) random.nextInt((int) writer.getMaxNodeKey());
          if (!writer.moveTo(key)) {
            continue;
          }
          final NodeKind kind = writer.getKind();
          final Op op = chooseOp(kind, random);
          if (op == null) {
            continue;
          }
          final Edit edit = new Edit(key, kind, op, "v" + random.nextInt(1000));
          apply(writer, edit);
          edits.add(edit);
        }
        writer.commit();
        script.add(edits);
      }
    }
  }

  private static void replay(final JsonResourceSession session, final List<List<Edit>> script) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(BASE_DOCUMENT), JsonNodeTrx.Commit.NO);
      writer.commit();
      for (final List<Edit> edits : script) {
        for (final Edit edit : edits) {
          assertTrue(writer.moveTo(edit.nodeKey()), "replay lost node " + edit.nodeKey());
          assertSame(edit.kind(), writer.getKind(), "the two arms diverged structurally at node " + edit.nodeKey());
          apply(writer, edit);
        }
        writer.commit();
      }
    }
  }

  /** The legal operations for a node kind, or {@code null} when this walk step is skipped. */
  private static Op chooseOp(final NodeKind kind, final Random random) {
    final Op[] legal = switch (kind) {
      case STRING_VALUE, OBJECT_NAMED_STRING -> new Op[] {Op.SET_STRING, Op.REMOVE};
      case NUMBER_VALUE, OBJECT_NAMED_NUMBER -> new Op[] {Op.SET_NUMBER, Op.REMOVE};
      case BOOLEAN_VALUE, OBJECT_NAMED_BOOLEAN -> new Op[] {Op.SET_BOOLEAN, Op.REMOVE};
      // RENAME is deliberately NOT offered for a fused CONTAINER record. Renaming one crashes the
      // engine today, independently of this change: ObjectNamedObjectNode.setNameKey dereferences a
      // null ownerPage once the new name key's signed-varint width differs from the old one
      // (NPE at ObjectNamedObjectNode.java:567 via JsonNodeTrxImpl.setObjectKeyName). Reproduced by
      // this test's seeds 1 and 20260915, and equally with hashKind(ROLLING) — a newly created
      // record is bound with both bind(...) and setOwnerPage(...) by JsonNodeFactoryImpl, so the
      // crashing record comes from a bind site that sets the page without an owner. Renaming a
      // fused PRIMITIVE record works and stays covered above.
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY -> new Op[] {Op.REMOVE};
      case OBJECT_NAMED_NULL -> new Op[] {Op.RENAME, Op.REMOVE};
      case ARRAY -> new Op[] {Op.INSERT_NUMBER_FIRST_CHILD, Op.INSERT_STRING_LAST_CHILD};
      default -> null;
    };
    return legal == null
        ? null
        : legal[random.nextInt(legal.length)];
  }

  private static void apply(final JsonNodeTrx writer, final Edit edit) {
    switch (edit.op()) {
      case SET_STRING -> writer.setStringValue(edit.payload());
      case SET_NUMBER -> writer.setNumberValue(edit.payload().length());
      case SET_BOOLEAN -> writer.setBooleanValue(edit.payload().hashCode() % 2 == 0);
      case RENAME -> writer.setObjectKeyName(edit.payload());
      case INSERT_NUMBER_FIRST_CHILD -> writer.insertNumberValueAsFirstChild(edit.payload().length());
      case INSERT_STRING_LAST_CHILD -> writer.insertStringValueAsLastChild(edit.payload());
      case REMOVE -> writer.remove();
    }
  }
}
