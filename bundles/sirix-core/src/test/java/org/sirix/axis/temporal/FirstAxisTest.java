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
 * Test {@link FirstAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxisTest {

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
    final NodeReadTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);

    new IteratorTester<NodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(firstRtx), null) {
      @Override
      protected Iterator<NodeReadTrx> newTargetIterator() {
        return new FirstAxis<>(holder.getNodeReadTrx());
      }
    }.test();
  }
}
