package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.Pair;
import org.sirix.utils.XdmDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link NextAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class NextAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
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
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);

    new IteratorTester<Pair<Integer, Long>>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(new Pair<>(secondRtx.getRevisionNumber(), secondRtx.getNodeKey())), null) {
      @Override
      protected Iterator<Pair<Integer, Long>> newTargetIterator() {
        return new NextAxis<>(firstRtx.getResourceManager(), firstRtx);
      }
    }.test();
  }

}
