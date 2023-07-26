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

import io.sirix.api.xml.XmlNodeTrx;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.utils.XmlDocumentCreator;

public final class XmlRevertTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.openResourceManager();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void test() throws SirixException {
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    Assert.assertEquals(1L, wtx.getRevisionNumber());
    XmlDocumentCreator.create(wtx);
    Assert.assertEquals(1L, wtx.getRevisionNumber());
    wtx.commit();
    Assert.assertEquals(2L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    Assert.assertEquals(2L, wtx.getRevisionNumber());
    wtx.moveToFirstChild();
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.commit();
    Assert.assertEquals(3L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    Assert.assertEquals(3L, wtx.getRevisionNumber());
    wtx.revertTo(1);
    wtx.commit();
    Assert.assertEquals(4L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    Assert.assertEquals(4L, wtx.getRevisionNumber());
    final long rev3MaxNodeKey = wtx.getMaxNodeKey();
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    Assert.assertEquals(4L, wtx.getRevisionNumber());
    wtx.revertTo(1);
    wtx.moveToFirstChild();
    final long maxNodeKey = wtx.getMaxNodeKey();
    assertEquals(rev3MaxNodeKey, maxNodeKey);
    wtx.insertElementAsFirstChild(new QNm(""));
    Assert.assertEquals(maxNodeKey + 1, wtx.getNodeKey());
    Assert.assertEquals(maxNodeKey + 1, wtx.getMaxNodeKey());
    wtx.commit();
    wtx.close();
  }
}
