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
import org.treetank.axis.AbsAxisTest;
import org.treetank.exception.AbsTTException;
import org.treetank.service.xml.xpath.XPathAxis;

/**
 * JUnit-test class to test the functionality of the PredicateAxis.
 * 
 * @author Tina Scherer
 */
public class PredicateFilterAxisTest {

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
  public void testPredicates() throws AbsTTException {

    // Find descendants starting from nodeKey 0L (root).
    holder.getRtx().moveToDocumentRoot();

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "/p:a[@i]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a/b[@p:x]"), new long[] {
      9L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[text()]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[element()]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[node()/text()]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[./node()]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[./node()/node()/node()]"),
      new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[//element()]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[/text()]"), new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[3<4]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[13>=4]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[13.0>=4]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[4 = 4]"), new long[] {
      1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[3=4]"), new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "p:a[3.2 = 3.22]"), new long[] {});

    holder.getRtx().moveTo(1L);

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "child::b[child::c]"), new long[] {
      5L, 9L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "child::*[text() or c]"), new long[] {
      5l, 9L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "child::*[text() or c], /node(), //c"),
      new long[] {
        5l, 9L, 1L, 7L, 11L
      });

  }

}
