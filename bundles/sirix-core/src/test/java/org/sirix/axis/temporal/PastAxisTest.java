package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.utils.XdmDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link PastAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PastAxisTest {

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
  public void testPastOrSelfAxis() throws SirixException {
    final NodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final NodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final NodeReadOnlyTrx thirdRtx = holder.getXdmNodeReadTrx();

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(thirdRtx, secondRtx, firstRtx),
        null) {
      @Override
      protected Iterator<NodeReadOnlyTrx> newTargetIterator() {
        return new PastAxis<>(thirdRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testPastAxis() throws SirixException {
    final NodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final NodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final NodeReadOnlyTrx thirdRtx = holder.getXdmNodeReadTrx();

    new IteratorTester<NodeReadOnlyTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(secondRtx, firstRtx),
        null) {
      @Override
      protected Iterator<NodeReadOnlyTrx> newTargetIterator() {
        return new PastAxis<>(thirdRtx);
      }
    }.test();
  }

  @Test
  public void testPastAxisWithRemovedNode() throws SirixException {
    try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      // Revision 4.
      wtx.commit();

      // Revision 5.
      wtx.commit();
    }

    try (final NodeReadOnlyTrx thirdReader = holder.getResourceManager().beginNodeReadOnlyTrx(3);
        final NodeReadOnlyTrx fourthReader = holder.getResourceManager().beginNodeReadOnlyTrx(4);
        final NodeReadOnlyTrx fifthReader = holder.getResourceManager().beginNodeReadOnlyTrx(5)) {
      thirdReader.moveTo(16);
      fourthReader.moveTo(16);
      fifthReader.moveTo(16);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(fifthReader, fourthReader, thirdReader), null) {
        @Override
        protected Iterator<NodeReadOnlyTrx> newTargetIterator() {
          return new PastAxis<>(fifthReader, IncludeSelf.YES);
        }
      }.test();
    }
  }
}
