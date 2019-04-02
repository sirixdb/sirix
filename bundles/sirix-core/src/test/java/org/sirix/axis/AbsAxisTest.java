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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.Axis;
import org.sirix.exception.SirixException;
import org.sirix.settings.Fixed;

public class AbsAxisTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    XdmTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  public static void testIAxisConventions(final Axis axis, final long[] expectedKeys) {
    // Axis Convention 1.
    final long startKey = axis.asXdmNodeReadTrx().getNodeKey();

    final long[] keys = new long[expectedKeys.length];
    int offset = 0;
    while (axis.hasNext()) {
      axis.next();
      // Axis results.
      assertTrue(offset < expectedKeys.length);
      keys[offset++] = axis.asXdmNodeReadTrx().getNodeKey();

      // Axis Convention 3.
      axis.asXdmNodeReadTrx().moveToDocumentRoot();
    }

    // Axis Convention 5.
    assertEquals(startKey, axis.asXdmNodeReadTrx().getNodeKey());

    // Axis results.
    assertArrayEquals(expectedKeys, keys);
  }

  public static void testAxisConventionsNext(final Axis axis, final long[] expectedKeys) {
    // IAxis Convention 1.
    final long startKey = axis.asXdmNodeReadTrx().getNodeKey();

    final long[] keys = new long[expectedKeys.length];
    int offset = 0;

    try {
      while (axis.next() != Fixed.NULL_NODE_KEY.getStandardProperty()) {
        // Axis results.
        assertTrue(offset < expectedKeys.length);
        keys[offset++] = axis.asXdmNodeReadTrx().getNodeKey();
      }
    } catch (final NoSuchElementException e) {
    }

    // Axis Convention 5.
    assertEquals(startKey, axis.asXdmNodeReadTrx().getNodeKey());

    // Axis results.
    assertArrayEquals(expectedKeys, keys);
  }

  public static void testIterable(final Iterator<Long> axis, final long[] expectedKeys) {
    final long[] keys = new long[expectedKeys.length];
    int offset = 0;
    while (axis.hasNext()) {
      final long key = axis.next();
      // Iterable results.
      assertTrue(offset < expectedKeys.length);
      keys[offset++] = key;
    }

    // Iterable results.
    assertArrayEquals(expectedKeys, keys);
  }

  @Test
  public void testAxisUserExample() throws SirixException {
    final Axis axis = new DescendantAxis(holder.getXdmNodeReadTrx());
    long count = 0L;
    while (axis.hasNext()) {
      axis.next();
      count += 1;
    }
    Assert.assertEquals(10L, count);
  }

}
