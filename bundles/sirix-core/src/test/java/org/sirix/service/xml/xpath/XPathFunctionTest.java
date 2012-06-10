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

package org.sirix.service.xml.xpath;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.axis.AbsAxisTest;
import org.sirix.exception.AbsTTException;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * Performes the XPathFunctionalityTest provided on <a
 * href="http://sole.dimi.uniud.it/~massimo.franceschet/xpathmark/FT.html">
 * XPathMark</a>
 * 
 * @author Tina Scherer, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 */
public class XPathFunctionTest {

  public static final String XML = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "alphabet.xml";

  private Holder holder;

  @Before
  public void setUp() throws Exception {
    TestHelper.deleteEverything();
    XMLShredder.main(XML, PATHS.PATH1.getFile().getAbsolutePath());
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testA_Axes() throws AbsTTException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/*"), new long[] {
      58L, 63L, 77L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/parent::*"), new long[] {
      20L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/descendant::*"), new long[] {
      58L, 63, 67L, 72L, 77L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/descendant-or-self::*"), new long[] {
      53L, 58L, 63, 67L, 72L, 77L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/ancestor::*"), new long[] {
      20L, 1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/ancestor-or-self::*"), new long[] {
      53L, 20L, 1L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/following-sibling::*"), new long[] {
      83L, 97L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/preceding-sibling::*"), new long[] {
      24L, 39L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/following::*"), new long[] {
      83L, 87L, 92L, 97L, 101L, 106L, 111L, 115L, 120L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/preceding::*"), new long[] {
      48L, 43L, 39L, 33L, 28L, 24L, 15L, 10L, 6L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/self::*"), new long[] {
      53L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/@id/parent::*"), new long[] {
      53L
    });

  }

  @Test
  public void testP_Filters() throws AbsTTException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[L]"), new long[] {
      20L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[parent::L]"), new long[] {
      58L, 63L, 77L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[descendant::L]"), new long[] {
      1L, 20L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[descendant-or-self::L]"),
      new long[] {
        1L, 20L, 53L
      });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[ancestor::L]"), new long[] {
      58L, 63L, 77L, 67L, 72L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[ancestor-or-self::L]"), new long[] {
      53L, 58L, 63L, 77L, 67L, 72L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[following-sibling::L]"), new long[] {
      24L, 39L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[preceding-sibling::L]"), new long[] {
      83L, 97L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[following::L]"), new long[] {
      6L, 10L, 15L, 24L, 39L, 28L, 33L, 43L, 48L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[preceding::L]"), new long[] {
      111L, 83L, 97L, 87L, 92L, 101L, 106L, 115L, 120L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[self::L]"), new long[] {
      53L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[@id]"), new long[] {
      1L, 6L, 20L, 111L, 10L, 15L, 24L, 39L, 53L, 83L, 97L, 28L, 33L, 43L, 48L, 58L, 63L, 77L, 67L, 72L, 87L,
      92L, 101L, 106L, 115L, 120L
    });

    holder.getRtx().moveTo(111L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "preceding::node()"), new long[] {
      110L, 106L, 105L, 101L, 97L, 96L, 92L, 91L, 87L, 83L, 82L, 77L, 76L, 72L, 71L, 67L, 63L, 62L, 58L, 57L,
      53L, 52L, 48L, 47L, 43L, 39L, 38L, 33L, 32L, 28L, 24L, 20L, 19L, 15L, 14L, 10L, 6L
    });

    holder.getRtx().moveTo(6L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "following::node()"), new long[] {
      20L, 24L, 28L, 32L, 33L, 38L, 39L, 43L, 47L, 48L, 52L, 53L, 57L, 58L, 62L, 63L, 67L, 71L, 72L, 76L,
      77L, 82L, 83L, 87L, 91L, 92L, 96L, 97L, 101L, 105L, 106L, 110L, 111L, 115L, 119L, 120L, 126L
    });
  }

  @Test
  public void testT_NodeTests() throws AbsTTException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/text()"), new long[] {
      57L, 62L
    });

    // comments are not supported yet
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/comment()"), new long[] {});

    // processing instructions are not supported yet
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/processing-instruction()"),
      new long[] {});

    // processing instructions are not supported yet
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/processing-instruction(\"myPI\")"),
      new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/node()"), new long[] {
      57L, 58L, 62L, 63L, 77L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), " //L/N"), new long[] {
      63L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/*"), new long[] {
      58L, 63L, 77L
    });
  }

  @Test
  public void testQ_Operators() throws AbsTTException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[preceding::Q]"), new long[] {
      111L, 83L, 97L, 87L, 92L, 101L, 106L, 115L, 120L
    });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[child::* and preceding::Q]"),
      new long[] {
        111L, 83L, 97L
      });

    AbsAxisTest.testIAxisConventions(
      new XPathAxis(holder.getRtx(), "//*[fn:not(child::*) and preceding::Q]"), new long[] {
        87L, 92L, 101L, 106L, 115L, 120L
      });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[preceding::L or following::L]"),
      new long[] {
        6L, 111L, 10L, 15L, 24L, 39L, 83L, 97L, 28L, 33L, 43L, 48L, 87L, 92L, 101L, 106L, 115L, 120L
      });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//L/ancestor::* | //L/descendant::*"),
      new long[] {
        20L, 1L, 58L, 63L, 67L, 72L, 77L
      });

    // AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
    // "//*[.=\"happy-go-lucky man\"]"), new long[] { 38L });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[@pre > 12 and @post < 15]"),
      new long[] {
        58L, 63L, 77L, 67L, 72L
      });

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(), "//*[@pre != @post]"), new long[] {
      1L, 6L, 20L, 111L, 10L, 15L, 53L, 28L, 33L, 43L, 48L, 58L, 63L, 77L, 67L, 72L, 87L, 92L, 101L, 106L,
      115L, 120L
    });

    // AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
    // "//*[@pre mod 2 = 0]"), new long[] { 6L, 111L, 15L, 24L, 53L,
    // 83L, 33L, 43L, 63L, 72L, 92L, 101L, 120L });
    //
    // AbsAxisTest
    // .testIAxisConventions(
    // new XPathAxis(
    // holder.getRtx(),
    // "//*[((@post * @post + @pre * @pre) div (@post + @pre)) > ((@post - @pre) * (@post - @pre))] "),
    // new long[] { 6L, 111L, 24L, 39L, 53L, 83L, 97L, 48L,
    // 58L, 63L, 77L, 87L, 92L, 101L, 106L, 115L, 120L });

  }
  //
  // //TODO: functions!
  //
}
