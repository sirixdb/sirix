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

package org.sirix.diff;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.exception.SirixException;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * Test StructuralDiff.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class StructuralDiffTest {

  /** {@link Holder} reference. */
  private Holder holder;

  /** {@link DiffObserver} reference. */
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
  public void testStructuralDiffFirst() throws Exception {
    DiffTestHelper.setUpFirst(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyStructuralDiffFirst(observer);
  }

  @Test
  public void testStructuralDiffOptimizedFirst() throws Exception {
    DiffTestHelper.setUpFirst(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedStructuralDiffFirst(observer);
  }

  @Test
  public void testStructuralDiffSecond()
      throws SirixException, InterruptedException, IOException, XMLStreamException {
    DiffTestHelper.setUpSecond(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffSecond(observer);
  }

  @Test
  public void testStructuralDiffOptimizedSecond()
      throws SirixException, IOException, XMLStreamException, InterruptedException {
    DiffTestHelper.setUpSecond(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedSecond(observer);
  }

  @Test
  public void testStructuralDiffThird()
      throws SirixException, IOException, XMLStreamException, InterruptedException {
    DiffTestHelper.setUpThird(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffThird(observer);
  }

  @Test
  public void testStructuralDiffOptimizedThird()
      throws SirixException, IOException, XMLStreamException, InterruptedException {
    DiffTestHelper.setUpThird(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedThird(observer);
  }

  @Test
  public void testStructuralDiffFourth() throws Exception {
    DiffTestHelper.setUpFourth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffFourth(observer);
  }

  @Test
  public void testStructuralDiffOptimizedFourth() throws Exception {
    DiffTestHelper.setUpFourth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyOptimizedFourth(observer);
  }

  @Test
  public void testStructuralDiffFifth() throws Exception {
    DiffTestHelper.setUpFifth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffFifth(observer);
  }

  @Test
  public void testStructuralDiffOptimizedFifth() throws Exception {
    DiffTestHelper.setUpFifth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyDiffFifth(observer);
  }

  @Test
  public void testStructuralDiffSixth() throws Exception {
    DiffTestHelper.setUpSixth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffSixth(observer);
  }

  @Test
  public void testStructuralDiffOptimizedSixth() throws Exception {
    DiffTestHelper.setUpSixth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.HASHED);
    DiffTestHelper.verifyDiffSixth(observer);
  }

  @Test
  public void testStructuralDiffSeventh() throws Exception {
    DiffTestHelper.setUpSeventh(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffSeventh(observer);
  }

  @Test
  public void testStructuralDiffEighth() throws Exception {
    DiffTestHelper.setUpEighth(holder);
    DiffTestHelper.checkStructuralDiff(holder, observer, DiffOptimized.NO);
    DiffTestHelper.verifyDiffEighth(observer);
  }

}
