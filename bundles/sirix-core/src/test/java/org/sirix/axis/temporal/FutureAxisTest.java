package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreater;
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
    TestHelper.deleteEverything();
    DocumentCreater.createVersioned(Holder.generateWtx().getWriter());
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testFutureOrSelfAxis() throws SirixException {
    final XdmNodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadTrx(1);
    final XdmNodeReadTrx secondRtx = holder.getResourceManager().beginNodeReadTrx(2);
    final XdmNodeReadTrx thirdRtx = holder.getReader();

    new IteratorTester<XdmNodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(firstRtx, secondRtx, thirdRtx), null) {
      @Override
      protected Iterator<XdmNodeReadTrx> newTargetIterator() {
        return new FutureAxis(firstRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testFutureAxis() throws SirixException {
    final XdmNodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadTrx(1);
    final XdmNodeReadTrx secondRtx = holder.getResourceManager().beginNodeReadTrx(2);
    final XdmNodeReadTrx thirdRtx = holder.getReader();

    new IteratorTester<XdmNodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(secondRtx, thirdRtx), null) {
      @Override
      protected Iterator<XdmNodeReadTrx> newTargetIterator() {
        return new FutureAxis(firstRtx);
      }
    }.test();
  }
}
