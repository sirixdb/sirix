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
import org.sirix.utils.XmlDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link PreviousAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PreviousAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
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

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(firstRtx), null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new PreviousAxis<>(secondRtx.getResourceManager(), secondRtx);
      }
    }.test();
  }

}
