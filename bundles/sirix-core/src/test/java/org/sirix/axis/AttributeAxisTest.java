/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.axis;

import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;

public class AttributeAxisTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testIterate() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXdmNodeReadTrx();

    rtx.moveToDocumentRoot();
    AbsAxisTest.testIAxisConventions(new AttributeAxis(rtx), new long[] {});

    rtx.moveTo(1L);
    AbsAxisTest.testIAxisConventions(new AttributeAxis(rtx), new long[] {3L});

    rtx.moveTo(9L);
    AbsAxisTest.testIAxisConventions(new AttributeAxis(rtx), new long[] {10L});

    rtx.moveTo(12L);
    AbsAxisTest.testIAxisConventions(new AttributeAxis(rtx), new long[] {});

    rtx.moveTo(2L);
    AbsAxisTest.testIAxisConventions(new AttributeAxis(rtx), new long[] {});
  }

  @Test
  public void testMultipleAttributes() throws SirixException {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    final long nodeKey = wtx.insertElementAsFirstChild(new QNm("foo")).getNodeKey();
    wtx.insertAttribute(new QNm("foo0"), "0");
    wtx.moveTo(nodeKey);
    wtx.insertAttribute(new QNm("foo1"), "1");
    wtx.moveTo(nodeKey);
    wtx.insertAttribute(new QNm("foo2"), "2");

    Assert.assertEquals(true, wtx.moveTo(nodeKey).hasMoved());

    Assert.assertEquals(true, wtx.moveToAttribute(0).hasMoved());
    Assert.assertEquals("0", wtx.getValue());
    Assert.assertEquals(new QNm("foo0"), wtx.getName());

    Assert.assertEquals(true, wtx.moveToParent().hasMoved());
    Assert.assertEquals(true, wtx.moveToAttribute(1).hasMoved());
    Assert.assertEquals("1", wtx.getValue());
    Assert.assertEquals(new QNm("foo1"), wtx.getName());

    Assert.assertEquals(true, wtx.moveToParent().hasMoved());
    Assert.assertEquals(true, wtx.moveToAttribute(2).hasMoved());
    Assert.assertEquals("2", wtx.getValue());
    Assert.assertEquals(new QNm("foo2"), wtx.getName());

    Assert.assertEquals(true, wtx.moveTo(nodeKey).hasMoved());
    final AbstractAxis axis = new AttributeAxis(wtx);

    Assert.assertEquals(true, axis.hasNext());
    axis.next();
    Assert.assertEquals(nodeKey + 1, wtx.getNodeKey());
    Assert.assertEquals(new QNm("foo0"), wtx.getName());
    Assert.assertEquals("0", wtx.getValue());

    Assert.assertEquals(true, axis.hasNext());
    axis.next();
    Assert.assertEquals(nodeKey + 2, wtx.getNodeKey());
    Assert.assertEquals(new QNm("foo1"), wtx.getName());
    Assert.assertEquals("1", wtx.getValue());

    Assert.assertEquals(true, axis.hasNext());
    axis.next();
    Assert.assertEquals(nodeKey + 3, wtx.getNodeKey());
    Assert.assertEquals(new QNm("foo2"), wtx.getName());
    Assert.assertEquals("2", wtx.getValue());

    wtx.rollback();
    wtx.close();
  }
}
