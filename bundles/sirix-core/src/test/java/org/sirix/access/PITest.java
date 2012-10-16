package org.sirix.access;

import java.io.ByteArrayOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.serialize.XMLSerializer;
import org.sirix.utils.DocumentCreater;

/** Processing instruction test. */
public class PITest {
	
	/** {@link Holder} reference. */
  private Holder mHolder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    TestHelper.createPICommentTestDocument();
    mHolder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    mHolder.close();
    TestHelper.closeEverything();
  }
  
	@Test
	public void testPI() throws SirixException {
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final XMLSerializer serializer = new XMLSerializer.XMLSerializerBuilder(mHolder.getSession(), out).build();
		serializer.call();
		System.out.println(out);
	}
}
