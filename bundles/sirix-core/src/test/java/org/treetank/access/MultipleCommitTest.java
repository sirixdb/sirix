/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.treetank.access;

import static org.junit.Assert.assertEquals;

import javax.xml.namespace.QName;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.treetank.Holder;
import org.treetank.TestHelper;
import org.treetank.api.IAxis;
import org.treetank.api.INodeReadTrx;
import org.treetank.axis.AbsAxis;
import org.treetank.axis.DescendantAxis;
import org.treetank.axis.PostOrderAxis;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.ElementNode;
import org.treetank.utils.DocumentCreater;

public class MultipleCommitTest {

  private Holder holder;

  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void test() throws AbsTTException {
    Assert.assertEquals(0L, holder.getWtx().getRevisionNumber());

    holder.getWtx().commit();

    holder.getWtx().insertElementAsFirstChild(new QName("foo"));
    assertEquals(1L, holder.getWtx().getRevisionNumber());
    holder.getWtx().moveTo(1);
    assertEquals(new QName("foo"), holder.getWtx().getQNameOfCurrentNode());
    holder.getWtx().abort();

    assertEquals(1L, holder.getWtx().getRevisionNumber());
  }

  @Test
  public void testAutoCommit() throws AbsTTException {
    DocumentCreater.create(holder.getWtx());
    holder.getWtx().commit();

    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.close();
  }

  @Test
  public void testRemove() throws AbsTTException {
    DocumentCreater.create(holder.getWtx());
    holder.getWtx().commit();
    assertEquals(1L, holder.getWtx().getRevisionNumber());

    holder.getWtx().moveToDocumentRoot();
    holder.getWtx().moveToFirstChild();
    holder.getWtx().remove();
    holder.getWtx().commit();
    assertEquals(2L, holder.getWtx().getRevisionNumber());
  }

  @Test
  public void testAttributeRemove() throws AbsTTException {
    DocumentCreater.create(holder.getWtx());
    holder.getWtx().commit();
    holder.getWtx().moveToDocumentRoot();

    final AbsAxis postorderAxis = new PostOrderAxis(holder.getWtx());
    while (postorderAxis.hasNext()) {
      postorderAxis.next();
      if (holder.getWtx().getNode().getKind() == ENode.ELEMENT_KIND
        && ((ElementNode)holder.getWtx().getNode()).getAttributeCount() > 0) {
        for (int i = 0, attrCount = ((ElementNode)holder.getWtx().getNode()).getAttributeCount(); i < attrCount; i++) {
          holder.getWtx().moveToAttribute(i);
          holder.getWtx().remove();
        }
      }
    }
    holder.getWtx().commit();
    holder.getWtx().moveToDocumentRoot();

    int attrTouch = 0;
    final IAxis descAxis = new DescendantAxis(holder.getWtx());
    while (descAxis.hasNext()) {
      descAxis.next();
      if (holder.getWtx().getNode().getKind() == ENode.ELEMENT_KIND) {
        for (int i = 0, attrCount = ((ElementNode)holder.getWtx().getNode()).getAttributeCount(); i < attrCount; i++) {
          if (holder.getWtx().moveToAttribute(i)) {
            attrTouch++;
          } else {
            throw new IllegalStateException("Should never occur!");
          }
        }
      }
    }
    assertEquals(0, attrTouch);

  }
}
