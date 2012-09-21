package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.xml.namespace.QName;

import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreater;

public class PowerFailureTest {
  
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    holder = Holder.generateSession();
  }
  
  @Test
  public void testDelete() throws SirixException {
//    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
//    DocumentCreater.create(wtx);
//    wtx.moveTo(4);
//    System.exit(0);
//    wtx.insertElementAsRightSibling(new QName("blabla"));
//    wtx.moveTo(5);
//    wtx.remove();
//    assertEquals(8, wtx.getNode().getNodeKey());
//    wtx.moveTo(9);
//    wtx.moveTo(4);
//    testDelete(wtx);
//    wtx.commit();
//    testDelete(wtx);
//    wtx.close();
//    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
//    testDelete(rtx);
//    rtx.close();
  }

  private final static void testDelete(final INodeReadTrx pRtx) {
    assertFalse(pRtx.moveTo(5));
    assertTrue(pRtx.moveTo(1));
    assertEquals(5, pRtx.getStructuralNode().getChildCount());
    assertEquals(7, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(8, pRtx.getStructuralNode().getDescendantCount());
  }
}
