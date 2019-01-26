package org.sirix.access;

import java.io.ByteArrayOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.utils.XdmDocumentCreator;

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
    XdmTestHelper.deleteEverything();
    XdmTestHelper.createPICommentTestDocument();
    mHolder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    mHolder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testPI() throws SirixException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final XMLSerializer serializer =
        new XMLSerializer.XMLSerializerBuilder(mHolder.getResourceManager(),
            out).emitXMLDeclaration().build();
    serializer.call();
    Assert.assertEquals(XdmDocumentCreator.COMMENTPIXML, out.toString());
  }
}
