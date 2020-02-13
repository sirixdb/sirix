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

package org.sirix.service.xml.xpath;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.FollowingSiblingAxis;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.ParentAxis;
import org.sirix.axis.SelfAxis;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.xpath.expr.UnionAxis;
import org.sirix.service.xml.xpath.filter.DupFilterAxis;

public class ExpressionSingleTest {

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
  public void testAdd() throws SirixException {
    // Verify.
    final ExpressionSingle builder = new ExpressionSingle();

    // test one axis
    AbstractAxis self = new SelfAxis(holder.getXmlNodeReadTrx());
    builder.add(self);
    assertEquals(builder.getExpr(), self);

    // test 2 axis
    AbstractAxis axis1 = new SelfAxis(holder.getXmlNodeReadTrx());
    AbstractAxis axis2 = new SelfAxis(holder.getXmlNodeReadTrx());
    builder.add(axis1);
    builder.add(axis2);
    assertTrue(builder.getExpr() instanceof NestedAxis);
  }

  @Test
  public void testDup() throws SirixException {
    ExpressionSingle builder = new ExpressionSingle();
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new DescendantAxis(holder.getXmlNodeReadTrx()));
    assertTrue(builder.getExpr() instanceof NestedAxis);

    builder = new ExpressionSingle();
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new DescendantAxis(holder.getXmlNodeReadTrx()));
    assertEquals(true, builder.isOrdered());
    assertTrue(builder.getExpr() instanceof NestedAxis);

    builder = new ExpressionSingle();
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new DescendantAxis(holder.getXmlNodeReadTrx()));
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    assertEquals(false, builder.isOrdered());

    builder = new ExpressionSingle();
    builder = new ExpressionSingle();
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new DescendantAxis(holder.getXmlNodeReadTrx()));
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new ParentAxis(holder.getXmlNodeReadTrx()));
    assertEquals(true, builder.isOrdered());

    builder = new ExpressionSingle();
    builder.add(new ChildAxis(holder.getXmlNodeReadTrx()));
    builder.add(new DescendantAxis(holder.getXmlNodeReadTrx()));
    builder.add(new FollowingSiblingAxis(holder.getXmlNodeReadTrx()));
    assertEquals(false, builder.isOrdered());

    builder = new ExpressionSingle();
    builder.add(
        new UnionAxis(holder.getXmlNodeReadTrx(), new DescendantAxis(holder.getXmlNodeReadTrx()),
                      new ParentAxis(holder.getXmlNodeReadTrx())));
    assertEquals(false, builder.isOrdered());
    assertTrue(builder.getExpr() instanceof DupFilterAxis);

  }
}
