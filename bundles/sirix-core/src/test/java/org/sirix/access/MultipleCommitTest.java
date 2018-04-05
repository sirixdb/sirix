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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.Axis;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.DocumentCreator;
import org.sirix.utils.DocumentCreator;

public class MultipleCommitTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void test() throws SirixException {
    assertEquals(1L, holder.getWriter().getRevisionNumber());
    holder.getWriter().commit();

    holder.getWriter().insertElementAsFirstChild(new QNm("foo"));
    assertEquals(2L, holder.getWriter().getRevisionNumber());
    holder.getWriter().moveTo(1);
    assertEquals(new QNm("foo"), holder.getWriter().getName());
    holder.getWriter().rollback();

    assertEquals(2L, holder.getWriter().getRevisionNumber());
  }

  @Test
  public void testAutoCommit() throws SirixException {
    DocumentCreator.create(holder.getWriter());
    holder.getWriter().commit();

    final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx();
    rtx.close();
  }

  @Test
  public void testRemove() throws SirixException {
    DocumentCreator.create(holder.getWriter());
    holder.getWriter().commit();
    assertEquals(2L, holder.getWriter().getRevisionNumber());

    holder.getWriter().moveToDocumentRoot();
    holder.getWriter().moveToFirstChild();
    holder.getWriter().remove();
    holder.getWriter().commit();
    assertEquals(3L, holder.getWriter().getRevisionNumber());
  }

  @Test
  public void testAttributeRemove() throws SirixException {
    DocumentCreator.create(holder.getWriter());
    holder.getWriter().commit();
    holder.getWriter().moveToDocumentRoot();

    final AbstractAxis postorderAxis = new PostOrderAxis(holder.getWriter());
    while (postorderAxis.hasNext()) {
      postorderAxis.next();
      if (holder.getWriter().getKind() == Kind.ELEMENT
          && holder.getWriter().getAttributeCount() > 0) {
        for (int i = 0, attrCount = holder.getWriter().getAttributeCount(); i < attrCount; i++) {
          holder.getWriter().moveToAttribute(i);
          holder.getWriter().remove();
        }
      }
    }
    holder.getWriter().commit();
    holder.getWriter().moveToDocumentRoot();

    int attrTouch = 0;
    final Axis descAxis = new DescendantAxis(holder.getWriter());
    while (descAxis.hasNext()) {
      descAxis.next();
      if (holder.getWriter().getKind() == Kind.ELEMENT) {
        for (int i = 0, attrCount = holder.getWriter().getAttributeCount(); i < attrCount; i++) {
          if (holder.getWriter().moveToAttribute(i).hasMoved()) {
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
