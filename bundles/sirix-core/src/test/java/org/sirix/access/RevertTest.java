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
import org.sirix.XdmTestHelper;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.XdmDocumentCreator;

public final class RevertTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    holder = Holder.openResourceManager();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void test() throws SirixException {
    XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx();
    assertEquals(1L, wtx.getRevisionNumber());
    XdmDocumentCreator.create(wtx);
    assertEquals(1L, wtx.getRevisionNumber());
    wtx.commit();
    assertEquals(2L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeWriteTrx();
    assertEquals(2L, wtx.getRevisionNumber());
    wtx.moveToFirstChild();
    wtx.insertElementAsFirstChild(new QNm("bla"));
    wtx.commit();
    assertEquals(3L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeWriteTrx();
    assertEquals(3L, wtx.getRevisionNumber());
    wtx.revertTo(1);
    wtx.commit();
    assertEquals(4L, wtx.getRevisionNumber());
    wtx.close();

    wtx = holder.getResourceManager().beginNodeWriteTrx();
    assertEquals(4L, wtx.getRevisionNumber());
    final long rev3MaxNodeKey = wtx.getMaxNodeKey();
    wtx.close();

    wtx = holder.getResourceManager().beginNodeWriteTrx();
    assertEquals(4L, wtx.getRevisionNumber());
    wtx.revertTo(1);
    wtx.moveToFirstChild();
    final long maxNodeKey = wtx.getMaxNodeKey();
    assertEquals(rev3MaxNodeKey, maxNodeKey);
    wtx.insertElementAsFirstChild(new QNm(""));
    assertEquals(maxNodeKey + 1, wtx.getNodeKey());
    assertEquals(maxNodeKey + 1, wtx.getMaxNodeKey());
    wtx.commit();
    wtx.close();
  }
}
