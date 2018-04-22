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
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreator;

public final class MinimumCommitTest {

  private Holder holder;

  @Before
  public void setUp() {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @After
  public void tearDown() {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void test() {
    assertEquals(1L, holder.getXdmNodeWriteTrx().getRevisionNumber());
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().close();

    holder = Holder.generateWtx();
    assertEquals(2L, holder.getXdmNodeWriteTrx().getRevisionNumber());
    DocumentCreator.create(holder.getXdmNodeWriteTrx());
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().close();

    holder = Holder.generateWtx();
    assertEquals(3L, holder.getXdmNodeWriteTrx().getRevisionNumber());
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().close();

    holder = Holder.generateRtx();
    assertEquals(3L, holder.getReader().getRevisionNumber());
  }

  @Test
  public void testTimestamp() throws SirixException {
    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx()) {
      assertTrue(rtx.getRevisionTimestamp() < (System.currentTimeMillis() + 1));
    }
  }

  @Test
  public void testCommitMessage() {
    try (final XdmNodeWriteTrx wtx = holder.getXdmNodeWriteTrx()) {
      wtx.commit("foo");
      wtx.commit("bar");
      wtx.commit("baz");
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(1)) {
      assertEquals("foo", rtx.getCommitCredentials().getMessage());
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(2)) {
      assertEquals("bar", rtx.getCommitCredentials().getMessage());
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(3)) {
      assertEquals("baz", rtx.getCommitCredentials().getMessage());
    }
  }
}
