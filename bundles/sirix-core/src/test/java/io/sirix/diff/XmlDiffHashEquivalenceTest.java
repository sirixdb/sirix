package io.sirix.diff;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.io.StorageType;
import io.sirix.service.xml.shredder.XmlShredder;
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
 * Clause B8 for the XML side: the differences an observer is told about must not depend on whether
 * the resource stores node hashes.
 *
 * <p>
 * The same edit history is built on a {@code ROLLING} and on a hashless resource, and both the full
 * and the structural XML diff are run over every revision pair. {@code SAMEHASH} is the hashed
 * route's way of saying "this subtree is untouched"; a hashless resource reaches the same verdict
 * as {@code SAME} by comparing the nodes. The comparison therefore normalizes those two verdicts
 * and requires everything else — every insert, delete, update and replace, with its node keys and
 * depths — to match exactly.
 */
final class XmlDiffHashEquivalenceTest {

  private static final String RESOURCE = "resource";

  private static final String DOCUMENT = "<root><a><b>one</b><c>two</c></a><d att=\"x\">three</d><e>keep</e></root>";

  @TempDir
  private Path directory;

  /** One reported difference. */
  private record Emitted(DiffType type, long newNodeKey, long oldNodeKey, int newDepth, int oldDepth) {
  }

  /**
   * Records the differences a diff reports, dropping the "unchanged" verdicts.
   *
   * <p>
   * {@code SAMEHASH} is not a renamed {@code SAME}: it stands for an entire unchanged subtree that
   * the hashed route then skips, so the two routes necessarily emit different numbers of unchanged
   * callbacks. The comparable quantity is the set of differences — which is also what a caller
   * actually sees, since the public diff serializers drop {@code SAME}, {@code SAMEHASH} and
   * {@code REPLACEDOLD} before emitting anything.
   */
  private static final class Recorder implements DiffObserver {
    private final List<Emitted> differences = new ArrayList<>();
    private int sameHashes;
    private int sames;
    private boolean done;

    @Override
    public void diffListener(final DiffType diffType, final long newNodeKey, final long oldNodeKey,
        final DiffDepth depth) {
      if (diffType == DiffType.SAMEHASH) {
        sameHashes++;
        return;
      }
      if (diffType == DiffType.SAME || diffType == DiffType.REPLACEDOLD) {
        sames++;
        return;
      }
      differences.add(new Emitted(diffType, newNodeKey, oldNodeKey, depth.getNewDepth(), depth.getOldDepth()));
    }

    @Override
    public void diffDone() {
      done = true;
    }
  }

  @Test
  void fullAndStructuralDiffsAgreeWithAndWithoutHashes() {
    final List<List<Emitted>> hashed = new ArrayList<>();
    final List<List<Emitted>> hashless = new ArrayList<>();
    int sameHashesSeen = 0;
    try (final var hashedDatabase = database("hashed", HashType.ROLLING);
        final var hashlessDatabase = database("hashless", HashType.NONE)) {
      try (final var session = hashlessDatabase.beginResourceSession(RESOURCE)) {
        assertSame(HashType.NONE, session.getResourceConfig().hashType);
        history(session);
        // Note: an ElementNode DERIVES a hash from its own content when none is stored, so
        // getHash() is not a witness for "this resource stores no hash" on the XML side. The
        // witness used here is behavioural and stronger: the hashless arm must never report
        // SAMEHASH, asserted right below, while the hashed arm must report it at least once.
        sameHashesSeen += collect(session, hashless);
      }
      assertEquals(0, sameHashesSeen, "a hashless resource must never report SAMEHASH");
      try (final var session = hashedDatabase.beginResourceSession(RESOURCE)) {
        assertSame(HashType.ROLLING, session.getResourceConfig().hashType);
        history(session);
        sameHashesSeen += collect(session, hashed);
      }
    }
    assertTrue(sameHashesSeen > 0, "the hashed arm must take the SAMEHASH shortcut at least once");
    assertEquals(hashless.size(), hashed.size());
    for (int index = 0; index < hashed.size(); index++) {
      assertEquals(hashless.get(index), hashed.get(index), "the differences reported for diff " + index
          + " must not depend on whether the resource " + "stores node hashes");
    }
  }

  /** Runs every revision pair through both diff algorithms; returns how often SAMEHASH was seen. */
  private static int collect(final XmlResourceSession session, final List<List<Emitted>> into) {
    final int revisions = session.getMostRecentRevisionNumber();
    assertTrue(revisions > 1, "a diff needs at least two revisions");
    int sameHashes = 0;
    for (int oldRevision = 1; oldRevision <= revisions; oldRevision++) {
      for (int newRevision = oldRevision; newRevision <= revisions; newRevision++) {
        for (final boolean full : new boolean[] {true, false}) {
          final Recorder recorder = new Recorder();
          // skipSubtrees(true) mirrors every public XML diff caller (the jn/xml diff function and
          // the DiffXmlResource example). Without it the hash route still prunes a deleted subtree
          // while the full route walks into it, so the two would report the deletion at different
          // granularities — an artefact of the harness, not a difference a caller can observe.
          final var builder = new DiffFactory.Builder<>(session, newRevision, oldRevision, DiffOptimized.HASHED,
              Set.of(recorder)).isGUI(false).skipSubtrees(true);
          if (full) {
            DiffFactory.invokeFullXmlDiff(builder);
          } else {
            DiffFactory.invokeStructuralXmlDiff(builder);
          }
          assertTrue(recorder.done, "the diff must signal completion");
          sameHashes += recorder.sameHashes;
          into.add(recorder.differences);
        }
      }
    }
    return sameHashes;
  }

  /** Four revisions: an untouched subtree, a text update, an element insert and a delete. */
  private static void history(final XmlResourceSession session) {
    try (final var writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(XmlShredder.createStringReader(DOCUMENT), XmlNodeTrx.Commit.No);
      writer.commit();

      // Update the text of <e>, leaving <a> and its subtree untouched.
      assertTrue(moveToElement(writer, "e"));
      assertTrue(writer.moveToFirstChild());
      writer.setValue("changed");
      writer.commit();

      // Insert a new element under <d>.
      assertTrue(moveToElement(writer, "d"));
      writer.insertElementAsFirstChild(new QNm("f"));
      writer.commit();

      // Delete <c> inside <a>.
      assertTrue(moveToElement(writer, "c"));
      writer.remove();
      writer.commit();
    }
  }

  /** Depth-first search for the first element named {@code name}. */
  private static boolean moveToElement(final XmlNodeTrx writer, final String name) {
    writer.moveToDocumentRoot();
    return descend(writer, name);
  }

  private static boolean descend(final XmlNodeTrx writer, final String name) {
    if (!writer.moveToFirstChild()) {
      return false;
    }
    do {
      final QNm current = writer.getName();
      if (current != null && name.equals(current.getLocalName())) {
        return true;
      }
      final long key = writer.getNodeKey();
      if (descend(writer, name)) {
        return true;
      }
      assertTrue(writer.moveTo(key));
    } while (writer.moveToRightSibling());
    return false;
  }

  private Database<XmlResourceSession> database(final String name, final HashType hashType) {
    final Path path = directory.resolve(name);
    Databases.createXmlDatabase(new DatabaseConfiguration(path));
    final var database = Databases.openXmlDatabase(path);
    database.createResource(
        ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).hashKind(hashType).build());
    return database;
  }
}
