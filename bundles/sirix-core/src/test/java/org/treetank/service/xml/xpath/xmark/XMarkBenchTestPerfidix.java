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

package org.treetank.service.xml.xpath.xmark;

import org.perfidix.annotation.AfterEachRun;
import org.perfidix.annotation.BeforeEachRun;
import org.perfidix.annotation.Bench;
import org.treetank.exception.AbsTTException;
import org.treetank.exception.TTXPathException;

public class XMarkBenchTestPerfidix {

  private final static XMarkBenchTest xmbt = new XMarkBenchTest();;

  @BeforeEachRun
  public static void setUp() throws Exception {
    XMarkBenchTest.setUp();
  }

  @Bench
  public void testXMark_Q1() {
    try {
      xmbt.xMarkTest_Q1();
    } catch (TTXPathException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Bench
  public void testXMark_Q5() {
    try {
      xmbt.xMarkTest_Q5();
    } catch (TTXPathException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Bench
  public void testXMark_Q6() {
    try {
      xmbt.xMarkTest_Q6();
    } catch (TTXPathException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Bench
  public void testXMark_Q7() {
    try {
      xmbt.xMarkTest_Q7();
    } catch (TTXPathException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  // @Bench
  // public void testXMark_21() {
  // xmbt.xMarkTest_Q21();
  // }
  //
  // @Bench
  // public void testXMark_22() {
  // xmbt.xMarkTest_Q22();
  // }
  //
  // @Bench
  // public void testXMark_23() {
  // xmbt.xMarkTest_Q23();
  // }

  @AfterEachRun
  public static void tearDownTest() throws AbsTTException {
    XMarkBenchTest.tearDown();

  }

}
