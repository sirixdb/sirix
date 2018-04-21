package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link LastAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class LastAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    DocumentCreator.createVersioned(Holder.generateWtx().getXdmNodeWriteTrx());
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testAxis() throws SirixException {
    final XdmNodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadTrx(1);
    final XdmNodeReadTrx thirdRtx = holder.getReader();

    new IteratorTester<XdmNodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(thirdRtx), null) {
      @Override
      protected Iterator<XdmNodeReadTrx> newTargetIterator() {
        return new LastAxis(firstRtx);
      }
    }.test();
  }

}
