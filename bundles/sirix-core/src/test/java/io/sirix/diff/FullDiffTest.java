/*
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

package io.sirix.diff;

import java.io.IOException;
import javax.xml.stream.XMLStreamException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.exception.SirixException;

/**
 * FullDiff test.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class FullDiffTest {

  /** Holder for testing. */
  private Holder holder;

  /** Observer. */
  private DiffObserver observer;

  @Before
  public void setUp() throws SirixException {
    DiffTestHelper.setUp();
    holder = Holder.generateWtxAndResourceWithHashes();
    observer = DiffTestHelper.createMock();
  }

  @After
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testFullDiffFirst() {
    DiffTestHelper.setUpFirst(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyFullDiffFirst(observer);
  }

  @Test
  public void testOptimizedFirst() {
    DiffTestHelper.setUpFirst(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedFullDiffFirst(observer);
  }

  @Test
  public void testFullDiffSecond() throws IOException, XMLStreamException {
    DiffTestHelper.setUpSecond(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffSecond(observer);
  }

  @Test
  public void testFullDiffThird() throws SirixException, IOException {
    DiffTestHelper.setUpThird(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffThird(observer);
  }

  @Test
  public void testFullDiffFourth() throws Exception {
    DiffTestHelper.setUpFourth(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffFourth(observer);
  }

  @Test
  public void testFullDiffFifth() throws Exception {
    DiffTestHelper.setUpFifth(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffFifth(observer);
  }

  @Test
  public void testFullDiffSixth() throws Exception {
    DiffTestHelper.setUpSixth(holder);
    DiffTestHelper.checkFullDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffSixth(observer);
  }
}
