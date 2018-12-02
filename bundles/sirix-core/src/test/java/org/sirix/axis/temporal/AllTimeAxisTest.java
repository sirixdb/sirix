package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreator;
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
    TestHelper.deleteEverything();
    try (final XdmNodeWriteTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
      DocumentCreator.createVersioned(wtx);
    }
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testAxis() throws SirixException {
    try (final XdmNodeReadTrx firstReader = holder.getResourceManager().beginNodeReadTrx(1);
        final XdmNodeReadTrx secondReader = holder.getResourceManager().beginNodeReadTrx(2);
        final XdmNodeReadTrx thirdReader = holder.getXdmNodeReadTrx()) {
      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader), null) {
        @Override
        protected Iterator<XdmNodeReadTrx> newTargetIterator() {
          return new AllTimeAxis(holder.getXdmNodeReadTrx());
        }
      }.test();
    }
  }

  @Test
  public void testAxisWithDeletedNode() throws SirixException {
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final XdmNodeReadTrx firstReader = holder.getResourceManager().beginNodeReadTrx(1);
        final XdmNodeReadTrx secondReader = holder.getResourceManager().beginNodeReadTrx(2);
        final XdmNodeReadTrx thirdReader = holder.getResourceManager().beginNodeReadTrx(3);
        final XdmNodeReadTrx fourthReader = holder.getResourceManager().beginNodeReadTrx(4)) {

      firstReader.moveTo(4);
      secondReader.moveTo(4);
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader, fourthReader), null) {
        @Override
        protected Iterator<XdmNodeReadTrx> newTargetIterator() {
          return new AllTimeAxis(fourthReader);
        }
      }.test();
    }
  }
}
