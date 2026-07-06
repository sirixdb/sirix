package io.sirix.diff;

import io.brackit.query.atomic.QNm;
import io.sirix.XmlTestHelper;
import io.sirix.api.Movement;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffFactory.DiffType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for {@code AbstractDiff#fireInserts()} (issue #1066).
 * <p>
 * When the old-revision start node cannot be resolved, the whole new subtree is reported as
 * inserted. The subtree walk must emit non-structural diffs (attributes/namespaces) as
 * {@link DiffType#INSERTED}. Before the fix it passed {@link DiffType#DELETED}, which iterated
 * the OLD cursor's attributes (emitting spurious deletes) and never reported the new subtree's
 * attribute nodes as inserted.
 */
public final class FireInsertsNonStructuralDiffTest {

  /** Simple observed (diff type, new node key) tuple. */
  private record Observed(DiffType diffType, long newNodeKey) {
  }

  /** Observer collecting all emitted (diff type, new node key) tuples. */
  private static final class CollectingObserver implements DiffObserver {

    private final List<Observed> observed = new ArrayList<>();

    private boolean done;

    @Override
    public void diffListener(final DiffType diffType, final long newNodeKey, final long oldNodeKey,
        final DiffDepth depth) {
      observed.add(new Observed(diffType, newNodeKey));
    }

    @Override
    public void diffDone() {
      done = true;
    }

    private boolean contains(final DiffType diffType, final long newNodeKey) {
      for (final Observed tuple : observed) {
        if (tuple.diffType() == diffType && tuple.newNodeKey() == newNodeKey) {
          return true;
        }
      }
      return false;
    }

    private long count(final DiffType diffType) {
      long count = 0;
      for (final Observed tuple : observed) {
        if (tuple.diffType() == diffType) {
          count++;
        }
      }
      return count;
    }
  }

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testFireInsertsReportsAttributesAsInserted() {
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile());
        final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final long subKey;
      final long childKey;
      final long attributeAKey;
      final long attributeBKey;

      try (final XmlNodeTrx wtx = session.beginNodeTrx()) {
        // Revision 1: a root element WITH an attribute (pre-fix the DELETED branch iterated the
        // old cursor, which is parked on this element, and emitted spurious deletes for it).
        wtx.insertElementAsFirstChild(new QNm("root"));
        wtx.insertAttribute(new QNm("oldAttr"), "old", Movement.TOPARENT);
        wtx.commit();

        // Revision 2: insert a subtree whose node keys do not exist in revision 1. The inner
        // element carries attributes.
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root element
        subKey = wtx.insertElementAsFirstChild(new QNm("sub")).getNodeKey();
        childKey = wtx.insertElementAsFirstChild(new QNm("child")).getNodeKey();
        attributeAKey = wtx.insertAttribute(new QNm("a"), "1").getNodeKey();
        wtx.moveToParent();
        attributeBKey = wtx.insertAttribute(new QNm("b"), "2").getNodeKey();
        wtx.moveToParent();
        wtx.commit();
      }

      final CollectingObserver observer = new CollectingObserver();

      // The old start key does not resolve in revision 1 => fireInserts() reports the whole new
      // subtree. oldDepth > 0 keeps fireDeletes() out of the picture (GUI mode is the default).
      DiffFactory.invokeFullXmlDiff(
          new DiffFactory.Builder<XmlNodeReadOnlyTrx, XmlNodeTrx>(session, 2, 1, DiffOptimized.NO, Set.of(observer))
              .newStartKey(subKey)
              .oldStartKey(subKey)
              .oldDepth(1));

      assertTrue("diffDone() must be signalled", observer.done);
      assertTrue("subtree root must be reported INSERTED", observer.contains(DiffType.INSERTED, subKey));
      assertTrue("child element must be reported INSERTED", observer.contains(DiffType.INSERTED, childKey));
      assertTrue("attribute 'a' must be reported INSERTED (pre-fix it was never reported)",
          observer.contains(DiffType.INSERTED, attributeAKey));
      assertTrue("attribute 'b' must be reported INSERTED (pre-fix it was never reported)",
          observer.contains(DiffType.INSERTED, attributeBKey));
      assertEquals("no DELETED tuples may be emitted while walking the inserted subtree", 0,
          observer.count(DiffType.DELETED));
    }
  }
}
