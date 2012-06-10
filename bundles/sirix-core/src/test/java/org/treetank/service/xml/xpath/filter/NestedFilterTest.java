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

package org.treetank.service.xml.xpath.filter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.treetank.Holder;
import org.treetank.TestHelper;
import org.treetank.axis.filter.AttributeFilter;
import org.treetank.axis.filter.ElementFilter;
import org.treetank.axis.filter.IFilterTest;
import org.treetank.axis.filter.ItemFilter;
import org.treetank.axis.filter.NameFilter;
import org.treetank.axis.filter.NestedFilter;
import org.treetank.axis.filter.NodeFilter;
import org.treetank.axis.filter.TextFilter;
import org.treetank.exception.AbsTTException;

public class NestedFilterTest {

  private Holder holder;

  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    TestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.deleteEverything();
  }

  @Test
  public void testIFilterConvetions() throws AbsTTException {

    holder.getRtx().moveTo(9L);
    IFilterTest.testIFilterConventions(new NestedFilter(holder.getRtx(), new ItemFilter(holder.getRtx()),
      new ElementFilter(holder.getRtx()), new NameFilter(holder.getRtx(), "b")), true);
    IFilterTest.testIFilterConventions(new NestedFilter(holder.getRtx(), new ItemFilter(holder.getRtx()),
      new AttributeFilter(holder.getRtx()), new NameFilter(holder.getRtx(), "b")), false);

    holder.getRtx().moveTo(4L);
    IFilterTest.testIFilterConventions(new NestedFilter(holder.getRtx(), new NodeFilter(holder.getRtx()),
      new ElementFilter(holder.getRtx())), false);
    IFilterTest.testIFilterConventions(new NestedFilter(holder.getRtx(), new NodeFilter(holder.getRtx()),
      new TextFilter(holder.getRtx())), true);

    holder.getRtx().moveTo(1L);
    holder.getRtx().moveToAttribute(0);
    IFilterTest.testIFilterConventions(new NestedFilter(holder.getRtx(),
      new AttributeFilter(holder.getRtx()), new NameFilter(holder.getRtx(), "i")), true);

  }
}
