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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.xml.NodeFilter;
import org.sirix.axis.filter.xml.TextFilter;
import org.sirix.axis.filter.xml.XmlNameFilter;
import org.sirix.exception.SirixException;

public class NestedAxisTest {

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
  public void testNestedAxisTest() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();

    // Find descendants starting from nodeKey 0L (root).
    rtx.moveToDocumentRoot();

    // XPath expression /p:a/b/text()
    // Part: /p:a
    final Axis childA = new FilterAxis(new ChildAxis(rtx), new XmlNameFilter(rtx, "p:a"));
    // Part: /b
    final Axis childB = new FilterAxis(new ChildAxis(rtx), new XmlNameFilter(rtx, "b"));
    // Part: /text()
    final Axis text = new FilterAxis(new ChildAxis(rtx), new TextFilter(rtx));
    // Part: /p:a/b/text()
    final Axis axis = new NestedAxis(new NestedAxis(childA, childB), text);

    AbsAxisTest.testAxisConventions(axis, new long[] {6L, 12L});
  }

  @Test
  public void testNestedAxisTest2() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();

    // Find descendants starting from nodeKey 0L (root).
    rtx.moveToDocumentRoot();

    // XPath expression /[:a/b/@p:x]
    // Part: /p:a
    final Axis childA = new FilterAxis(new ChildAxis(rtx), new XmlNameFilter(rtx, "p:a"));
    // Part: /b
    final Axis childB = new FilterAxis(new ChildAxis(rtx), new XmlNameFilter(rtx, "b"));
    // Part: /@x
    final Axis attributeX = new FilterAxis(new AttributeAxis(rtx), new XmlNameFilter(rtx, "p:x"));
    // Part: /p:a/b/@p:x
    final Axis axis = new NestedAxis(new NestedAxis(childA, childB), attributeX);

    AbsAxisTest.testAxisConventions(axis, new long[] {10L});

  }

  @Test
  public void testNestedAxisTest3() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();

    // Find desceFndants starting from nodeKey 0L (root).
    rtx.moveToDocumentRoot();

    // XPath expression p:a/node():
    // Part: /p:a
    final Axis childA = new FilterAxis(new ChildAxis(rtx), new XmlNameFilter(rtx, "p:a"));

    // Part: /node()
    final Axis childNode = new FilterAxis(new ChildAxis(rtx), new NodeFilter(rtx));

    // Part: /p:a/node():
    final Axis axis = new NestedAxis(childA, childNode);

    AbsAxisTest.testAxisConventions(axis, new long[] {4L, 5L, 8L, 9L, 13L});

  }
}
