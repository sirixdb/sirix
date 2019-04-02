package org.sirix.axis.temporal;

import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.utils.Pair;
import org.sirix.utils.XdmDocumentCreator;
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
  public void testFutureOrSelfAxis() throws SirixException {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXdmNodeReadTrx();

    new IteratorTester<Pair<Integer, Long>>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(new Pair<>(firstRtx.getRevisionNumber(), firstRtx.getNodeKey()),
            new Pair<>(secondRtx.getRevisionNumber(), secondRtx.getNodeKey()),
            new Pair<>(thirdRtx.getRevisionNumber(), thirdRtx.getNodeKey())),
        null) {
      @Override
      protected Iterator<Pair<Integer, Long>> newTargetIterator() {
        return new FutureAxis<>(firstRtx.getResourceManager(), firstRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testFutureAxis() throws SirixException {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXdmNodeReadTrx();

    new IteratorTester<Pair<Integer, Long>>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(new Pair<>(secondRtx.getRevisionNumber(), secondRtx.getNodeKey()),
            new Pair<>(thirdRtx.getRevisionNumber(), thirdRtx.getNodeKey())),
        null) {
      @Override
      protected Iterator<Pair<Integer, Long>> newTargetIterator() {
        return new FutureAxis<>(firstRtx.getResourceManager(), firstRtx);
      }
    }.test();
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

    try (final XmlNodeReadOnlyTrx thirdReader = holder.getResourceManager().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceManager().beginNodeReadOnlyTrx(4)) {
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(new Pair<>(thirdReader.getRevisionNumber(), thirdReader.getNodeKey()),
              new Pair<>(fourthReader.getRevisionNumber(), fourthReader.getNodeKey())),
          null) {
        @Override
        protected Iterator<Pair<Integer, Long>> newTargetIterator() {
          return new FutureAxis<>(thirdReader.getResourceManager(), thirdReader, IncludeSelf.YES);
        }
      }.test();
    }
  }
}
