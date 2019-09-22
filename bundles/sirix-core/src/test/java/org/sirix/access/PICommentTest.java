package org.sirix.access;

import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.utils.XmlDocumentCreator;

/**
 * Processing instruction/comment test.
 *
 * @author Johannes Lichtenberger
 */
public class PICommentTest {

  /** {@link Holder} reference. */
  private Holder mHolder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createPICommentTestDocument();
    mHolder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    mHolder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testPI() throws SirixException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XmlSerializer serializer =
        new XmlSerializer.XmlSerializerBuilder(mHolder.getResourceManager(),
            out).emitXMLDeclaration().build();
    serializer.call();
    Assert.assertEquals(XmlDocumentCreator.COMMENTPIXML, out.toString());
  }
}
