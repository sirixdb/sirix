package io.sirix.access.node.xml;

import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.utils.XmlDocumentCreator;

import static org.junit.Assert.assertEquals;

/**
 * Processing instruction/comment test.
 *
 * @author Johannes Lichtenberger
 */
public final class PICommentTest {

  /** {@link Holder} reference. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createPICommentTestDocument();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testPI() throws SirixException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XmlSerializer serializer =
        new XmlSerializer.XmlSerializerBuilder(holder.getResourceSession(), out).emitXMLDeclaration().build();
    serializer.call();
    assertEquals(XmlDocumentCreator.COMMENTPIXML, out.toString());
  }
}
