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

package io.sirix.access.node.xml;

import static org.junit.Assert.assertEquals;

import io.sirix.api.Axis;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.node.NodeKind;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.utils.XmlDocumentCreator;

public class MultipleCommitTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void test() throws SirixException {
    assertEquals(1L, holder.getXdmNodeWriteTrx().getRevisionNumber());
    holder.getXdmNodeWriteTrx().commit();

    holder.getXdmNodeWriteTrx().insertElementAsFirstChild(new QNm("foo"));
    assertEquals(2L, holder.getXdmNodeWriteTrx().getRevisionNumber());
    holder.getXdmNodeWriteTrx().moveTo(1);
    assertEquals(new QNm("foo"), holder.getXdmNodeWriteTrx().getName());
    holder.getXdmNodeWriteTrx().rollback();

    assertEquals(2L, holder.getXdmNodeWriteTrx().getRevisionNumber());
  }

  @Test
  public void testAutoCommit() throws SirixException {
    XmlDocumentCreator.create(holder.getXdmNodeWriteTrx());
    holder.getXdmNodeWriteTrx().commit();

    final NodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.close();
  }

  @Test
  public void testRemove() throws SirixException {
    XmlDocumentCreator.create(holder.getXdmNodeWriteTrx());
    holder.getXdmNodeWriteTrx().commit();
    assertEquals(2L, holder.getXdmNodeWriteTrx().getRevisionNumber());

    holder.getXdmNodeWriteTrx().moveToDocumentRoot();
    holder.getXdmNodeWriteTrx().moveToFirstChild();
    holder.getXdmNodeWriteTrx().remove();
    holder.getXdmNodeWriteTrx().commit();
    assertEquals(3L, holder.getXdmNodeWriteTrx().getRevisionNumber());
  }

  @Test
  public void testAttributeRemove() throws SirixException {
    XmlDocumentCreator.create(holder.getXdmNodeWriteTrx());
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().moveToDocumentRoot();

    final AbstractAxis postorderAxis = new PostOrderAxis(holder.getXdmNodeWriteTrx());
    while (postorderAxis.hasNext()) {
      postorderAxis.nextLong();
      if (holder.getXdmNodeWriteTrx().getKind() == NodeKind.ELEMENT
          && holder.getXdmNodeWriteTrx().getAttributeCount() > 0) {
        for (int i = 0, attrCount =
            holder.getXdmNodeWriteTrx().getAttributeCount(); i < attrCount; i++) {
          holder.getXdmNodeWriteTrx().moveToAttribute(i);
          holder.getXdmNodeWriteTrx().remove();
        }
      }
    }
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().moveToDocumentRoot();

    int attrTouch = 0;
    final Axis descAxis = new DescendantAxis(holder.getXdmNodeWriteTrx());
    while (descAxis.hasNext()) {
      descAxis.nextLong();
      if (holder.getXdmNodeWriteTrx().getKind() == NodeKind.ELEMENT) {
        for (int i = 0, attrCount =
            holder.getXdmNodeWriteTrx().getAttributeCount(); i < attrCount; i++) {
          if (holder.getXdmNodeWriteTrx().moveToAttribute(i)) {
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
