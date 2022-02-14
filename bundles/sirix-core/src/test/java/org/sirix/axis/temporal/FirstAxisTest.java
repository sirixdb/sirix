package org.sirix.axis.temporal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.utils.XmlDocumentCreator;

import java.util.Iterator;

/**
 * Test {@link FirstAxis}.
 *
 * @author Johannes Lichtenberger
 */
public final class FirstAxisTest {

  /**
   * Number of iterations.
   */
  private static final int ITERATIONS = 5;

  /**
   * The {@link Holder} instance.
   */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
    }
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(firstRtx), null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new FirstAxis<>(holder.getResourceManager(), holder.getXmlNodeReadTrx());
      }
    }.test();
  }
}
