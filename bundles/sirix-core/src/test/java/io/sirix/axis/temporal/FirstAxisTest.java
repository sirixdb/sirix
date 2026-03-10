package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.test.IteratorTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;

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
  public void testAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);

    new IteratorTester<>(ITERATIONS, List.of(firstRtx), () ->
        new FirstAxis<>(holder.getResourceSession(), holder.getXmlNodeReadTrx())
    ).test();
  }
}
