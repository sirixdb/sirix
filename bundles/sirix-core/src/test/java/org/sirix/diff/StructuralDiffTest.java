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

package org.sirix.diff;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.diff.DiffFactory.EDiffOptimized;
import org.sirix.exception.SirixException;

/**
 * Test StructuralDiff.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class StructuralDiffTest {

  private Holder mHolder;

  private DiffObserver mObserver;

  @Before
  public void setUp() throws SirixException {
    DiffTestHelper.setUp();
    mHolder = Holder.generateWtx();
    mObserver = DiffTestHelper.createMock();
  }

  @After
  public void tearDown() throws SirixException {
    TestHelper.closeEverything();
  }

  @Test
  public void testStructuralDiffFirst() throws Exception {
    DiffTestHelper.setUpFirst(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffFirst(mObserver);
  }

  @Test
  public void testOptimizedFirst() throws Exception {
    DiffTestHelper.setUpFirst(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedFirst(mObserver);
  }

  @Test
  public void testStructuralDiffSecond() throws SirixException, InterruptedException, IOException,
    XMLStreamException {
    DiffTestHelper.setUpSecond(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffSecond(mObserver);
  }

  @Test
  public void testStructuralDiffOptimizedSecond() throws SirixException, IOException, XMLStreamException,
    InterruptedException {
    DiffTestHelper.setUpSecond(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedSecond(mObserver);
  }

  @Test
  public void testStructuralDiffThird() throws SirixException, IOException, XMLStreamException,
    InterruptedException {
    DiffTestHelper.setUpThird(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffThird(mObserver);
  }

  @Test
  public void testStructuralDiffOptimizedThird() throws SirixException, IOException, XMLStreamException,
    InterruptedException {
    DiffTestHelper.setUpThird(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedThird(mObserver);
  }

  @Test
  public void testStructuralDiffFourth() throws Exception {
    DiffTestHelper.setUpFourth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffFourth(mObserver);
  }

  @Test
  public void testStructuralDiffOptimizedFourth() throws Exception {
    DiffTestHelper.setUpFourth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedFourth(mObserver);
  }

  @Test
  public void testStructuralDiffFifth() throws Exception {
    DiffTestHelper.setUpFifth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffFifth(mObserver);
  }

  @Test
  public void testStructuralDiffOptimizedFifth() throws Exception {
    DiffTestHelper.setUpFifth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyDiffFifth(mObserver);
  }

  @Test
  public void testStructuralDiffSixth() throws Exception {
    DiffTestHelper.setUpSixth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffSixth(mObserver);
  }

  @Test
  public void testStructuralDiffOptimizedSixth() throws Exception {
    DiffTestHelper.setUpSixth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.HASHED);
    DiffTestHelper.verifyDiffSixth(mObserver);
  }

  @Test
  public void testStructuralDiffSeventh() throws Exception {
    DiffTestHelper.setUpSeventh(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffSeventh(mObserver);
  }

  @Test
  public void testStructuralDiffEighth() throws Exception {
    DiffTestHelper.setUpEighth(mHolder);
    DiffTestHelper.checkStructuralDiff(mHolder, mObserver, EDiffOptimized.NO);
    DiffTestHelper.verifyDiffEighth(mObserver);
  }

}
