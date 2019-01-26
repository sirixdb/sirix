package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.XdmDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link AllTimeAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class AllTimeAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    try (final XdmNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
      XdmDocumentCreator.createVersioned(wtx);
    }
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testAxis() throws SirixException {
    try (final NodeReadTrx firstReader = holder.getResourceManager().beginReadOnlyTrx(1);
        final NodeReadTrx secondReader = holder.getResourceManager().beginReadOnlyTrx(2);
        final NodeReadTrx thirdReader = holder.getNodeReadTrx()) {
      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader), null) {
        @Override
        protected Iterator<NodeReadTrx> newTargetIterator() {
          return new AllTimeAxis<>(holder.getNodeReadTrx());
        }
      }.test();
    }
  }

  @Test
  public void testAxisWithDeletedNode() throws SirixException {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final NodeReadTrx firstReader = holder.getResourceManager().beginReadOnlyTrx(1);
        final NodeReadTrx secondReader = holder.getResourceManager().beginReadOnlyTrx(2);
        final NodeReadTrx thirdReader = holder.getResourceManager().beginReadOnlyTrx(3);
        final NodeReadTrx fourthReader = holder.getResourceManager().beginReadOnlyTrx(4)) {

      firstReader.moveTo(4);
      secondReader.moveTo(4);
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader, fourthReader), null) {
        @Override
        protected Iterator<NodeReadTrx> newTargetIterator() {
          return new AllTimeAxis<>(fourthReader);
        }
      }.test();
    }
  }
}
