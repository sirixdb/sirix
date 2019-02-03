package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.XdmDocumentCreator;
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
    final NodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final NodeReadOnlyTrx thirdRtx = holder.getNodeReadTrx();

    new IteratorTester<NodeReadOnlyTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(thirdRtx), null) {
      @Override
      protected Iterator<NodeReadOnlyTrx> newTargetIterator() {
        return new LastAxis<>(firstRtx);
      }
    }.test();
  }

}
