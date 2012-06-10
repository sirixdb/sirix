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

package org.sirix.service.xml.xpath.filter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.axis.filter.IFilterTest;
import org.sirix.axis.filter.WildcardFilter;
import org.sirix.axis.filter.WildcardFilter.EType;
import org.sirix.exception.AbsTTException;

public class WildcardFilterTest {

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
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "b", EType.LOCALNAME), true);
    holder.getRtx().moveToAttribute(0);
    // try {
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "p", EType.PREFIX), true);
    // fail("Expected an Exception, because attributes are not supported.");
    // } catch (IllegalStateException e) {
    // assertThat(e.getMessage(), is("Wildcards are not supported in attribute names yet."));
    //
    // }
    // IFilterTest.testIFilterConventions(new
    // WildcardFilter(holder.getRtx(), "b",
    // true), true);

    // holder.getRtx().moveTo(3L);
    // IFilterTest.testIFilterConventions(new ItemFilter(holder.getRtx()),
    // true);

    holder.getRtx().moveTo(1L);
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "p", EType.PREFIX), true);
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "a", EType.LOCALNAME), true);
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "c", EType.LOCALNAME), false);
    IFilterTest.testIFilterConventions(new WildcardFilter(holder.getRtx(), "b", EType.PREFIX), false);

  }
}
