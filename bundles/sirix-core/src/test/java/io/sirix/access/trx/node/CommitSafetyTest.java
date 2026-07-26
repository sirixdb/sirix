package io.sirix.access.trx.node;

import io.sirix.JsonTestHelper;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.exception.SirixUsageException;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for the commit-safety pair #1061/#1062.
 *
 * <p>#1061: a failed commit used to flip the state machine to COMMITTING and zero the modification
 * counter before anything was durable — stranding the transaction (every further operation threw
 * "state is not running") while close() silently discarded the uncommitted work.
 *
 * <p>#1062: compound structural operations (move, replace) internally call public mutators, each
 * of which runs the count-based auto-commit check; crossing maxNodeCount mid-operation durably
 * committed a structurally inconsistent tree (e.g. a moved subtree detached from its old position
 * but not yet re-attached).
 */
public final class CommitSafetyTest {

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @Test
  public void failedCommitKeepsTransactionUsableAndGuardsClose() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final var wtx = manager.beginNodeTrx();
      final var failFirstCommit = new AtomicBoolean(true);
      wtx.addPreCommitHook(rtx -> {
        if (failFirstCommit.getAndSet(false)) {
          throw new IllegalStateException("injected pre-commit failure");
        }
      });

      // Dirty the transaction: remove the first field of the root object.
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root OBJECT
      wtx.moveToFirstChild(); // first field
      wtx.remove();

      // First commit fails before anything is durable.
      final var thrown = assertThrows(IllegalStateException.class, () -> wtx.commit());
      assertEquals("injected pre-commit failure", thrown.getMessage());

      // Nothing was committed.
      assertEquals(1, manager.getMostRecentRevisionNumber());

      // The dirty-close guard still protects the uncommitted work (it used to be defeated
      // because modificationCount was zeroed before the commit was attempted).
      assertThrows(SirixUsageException.class, wtx::close);

      // The transaction is NOT stranded in COMMITTING: further mutations work...
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root OBJECT
      wtx.moveToFirstChild(); // (new) first field
      wtx.remove();

      // ...and a retried commit succeeds and persists everything.
      wtx.commit();
      assertEquals(2, manager.getMostRecentRevisionNumber());
      wtx.close();

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild(); // root OBJECT
        // The original test document has 4 top-level fields; two were removed.
        assertEquals(2, rtx.getChildCount());
      }
    }
  }

  @Test
  public void autoCommitDoesNotSplitJsonReplaceObjectRecordValue() {
    JsonTestHelper.createTestDocument();

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // maxNodeCount = 2: the threshold check runs at the START of each mutator, so with the
      // top-level replace counting 1 and the nested remove() counting 2, the nested INSERT
      // (count 3) crosses the threshold — without the compound-operation guard the auto-commit
      // fires after the field was removed but before its replacement is inserted, durably
      // committing the half-replaced tree.
      try (final var wtx = manager.beginNodeTrx(2)) {
        wtx.moveToFirstChild(); // root OBJECT
        wtx.moveToFirstChild(); // first field
        // Find the fused string field "baz" and replace its value with an object (a different
        // kind, so the same-kind fast path is skipped and the remove + insert path runs).
        while (wtx.getKind() != NodeKind.OBJECT_NAMED_STRING || !"baz".equals(wtx.getName().getLocalName())) {
          assertTrue("test document must contain the string field \"baz\"", wtx.hasRightSibling());
          wtx.moveToRightSibling();
        }
        wtx.replaceObjectRecordValue(ObjectValue.INSTANCE);
        wtx.commit();
      }

      // Every durable revision must contain exactly one "baz" field — never the mid-replace
      // state with the field missing.
      final int mostRecent = manager.getMostRecentRevisionNumber();
      for (int revision = 1; revision <= mostRecent; revision++) {
        try (final var rtx = manager.beginNodeReadOnlyTrx(revision)) {
          rtx.moveToFirstChild(); // root OBJECT
          int bazCount = 0;
          if (rtx.hasFirstChild()) {
            rtx.moveToFirstChild();
            while (true) {
              if (rtx.getKind().name().startsWith("OBJECT_NAMED") && "baz".equals(rtx.getName().getLocalName())) {
                bazCount++;
              }
              if (!rtx.hasRightSibling()) {
                break;
              }
              rtx.moveToRightSibling();
            }
          }
          assertEquals("revision " + revision + " must contain the \"baz\" field exactly once", 1, bazCount);
        }
      }
    }
  }

  @Test
  public void autoCommitDoesNotSplitXmlMoveSubtree() {
    XmlTestHelper.createTestDocument();

    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final long movedNodeKey;
      // maxNodeCount = 1: without the compound-operation guard, the text-merge remove() inside
      // adaptForMove crosses the threshold and durably commits while the moved subtree is
      // detached from its old position but not yet re-attached (an orphaned subtree in a
      // persisted revision).
      try (final var wtx = manager.beginNodeTrx(1)) {
        wtx.moveToFirstChild(); // p:a element
        final long anchorKey = wtx.getNodeKey();
        // First <b> child: has TEXT siblings on both sides ("oops1", "oops2"), which forces the
        // text-node merge (nested remove() + setValue()) during the move.
        wtx.moveToFirstChild(); // text "oops1"
        wtx.moveToRightSibling(); // first <b>
        movedNodeKey = wtx.getNodeKey();
        wtx.moveTo(anchorKey);
        wtx.moveSubtreeToFirstChild(movedNodeKey);
        wtx.commit();
      }

      // The moved element must be reachable from the document root in EVERY durable revision.
      final int mostRecent = manager.getMostRecentRevisionNumber();
      for (int revision = 1; revision <= mostRecent; revision++) {
        try (final var rtx = manager.beginNodeReadOnlyTrx(revision)) {
          rtx.moveToDocumentRoot();
          boolean found = false;
          for (final var axis = new DescendantAxis(rtx, IncludeSelf.YES); axis.hasNext(); ) {
            if (axis.nextLong() == movedNodeKey) {
              found = true;
              break;
            }
          }
          assertTrue("moved node " + movedNodeKey + " must be reachable in revision " + revision, found);
        }
      }
    }
  }
}
