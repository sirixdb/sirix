package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.utils.XdmDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link FutureAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FutureAxisTest {

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
  public void testFutureOrSelfAxis() throws SirixException {
    final NodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadTrx(1);
    final NodeReadTrx secondRtx = holder.getResourceManager().beginNodeReadTrx(2);
    final NodeReadTrx thirdRtx = holder.getNodeReadTrx();

    new IteratorTester<NodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(firstRtx, secondRtx, thirdRtx), null) {
      @Override
      protected Iterator<NodeReadTrx> newTargetIterator() {
        return new FutureAxis<>(firstRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testFutureAxis() throws SirixException {
    final NodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadTrx(1);
    final NodeReadTrx secondRtx = holder.getResourceManager().beginNodeReadTrx(2);
    final NodeReadTrx thirdRtx = holder.getNodeReadTrx();

    new IteratorTester<NodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(secondRtx, thirdRtx),
        null) {
      @Override
      protected Iterator<NodeReadTrx> newTargetIterator() {
        return new FutureAxis<>(firstRtx);
      }
    }.test();
  }

  @Test
  public void testAxisWithDeletedNode() throws SirixException {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final NodeReadTrx thirdReader = holder.getResourceManager().beginNodeReadTrx(3);
        final NodeReadTrx fourthReader = holder.getResourceManager().beginNodeReadTrx(4)) {
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(thirdReader, fourthReader),
          null) {
        @Override
        protected Iterator<NodeReadTrx> newTargetIterator() {
          return new FutureAxis<>(thirdReader, IncludeSelf.YES);
        }
      }.test();
    }
  }
}
