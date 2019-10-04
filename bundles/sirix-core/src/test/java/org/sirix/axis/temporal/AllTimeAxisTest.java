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
import org.sirix.exception.SirixException;
import org.sirix.utils.XmlDocumentCreator;

import java.util.Iterator;

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
    XmlTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
    }
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testAxis() throws SirixException {
    try (final XmlNodeReadOnlyTrx firstReader = holder.getResourceManager().beginNodeReadOnlyTrx(1);
        final XmlNodeReadOnlyTrx secondReader = holder.getResourceManager().beginNodeReadOnlyTrx(2);
        final XmlNodeReadOnlyTrx thirdReader = holder.getXdmNodeReadTrx()) {
      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader), null) {
        @Override
        protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
          return new AllTimeAxis<>(holder.getResourceManager(), holder.getXdmNodeReadTrx());
        }
      }.test();
    }
  }

  @Test
  public void testAxisWithDeletedNode() throws SirixException {
    try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx firstReader = holder.getResourceManager().beginNodeReadOnlyTrx(1);
        final XmlNodeReadOnlyTrx secondReader = holder.getResourceManager().beginNodeReadOnlyTrx(2);
        final XmlNodeReadOnlyTrx thirdReader = holder.getResourceManager().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceManager().beginNodeReadOnlyTrx(4)) {

      firstReader.moveTo(4);
      secondReader.moveTo(4);
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(firstReader, secondReader, thirdReader, fourthReader), null) {
        @Override
        protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
          return new AllTimeAxis<>(fourthReader.getResourceManager(), fourthReader);
        }
      }.test();
    }
  }
}
