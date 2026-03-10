package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.test.IteratorTester;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;

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
  public void setUp() {
    XmlTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXmlNodeTrx()) {
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
  public void testFutureOrSelfAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, List.of(firstRtx, secondRtx, thirdRtx), () ->
        new FutureAxis<>(firstRtx.getResourceSession(), firstRtx, IncludeSelf.YES)
    ).test();
  }

  @Test
  public void testFutureAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, List.of(secondRtx, thirdRtx), () ->
        new FutureAxis<>(firstRtx.getResourceSession(), firstRtx)
    ).test();
  }

  @Test
  public void testAxisWithDeletedNode() {
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx thirdReader = holder.getResourceSession().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceSession().beginNodeReadOnlyTrx(4)) {
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, List.of(thirdReader, fourthReader), () ->
          new FutureAxis<>(thirdReader.getResourceSession(), thirdReader, IncludeSelf.YES)
      ).test();
    }
  }
}
