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

package org.sirix.service.xml.xpath.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.perfidix.annotation.AfterEachRun;
import org.perfidix.annotation.BeforeEachRun;
import org.perfidix.annotation.Bench;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.FilterAxis;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.filter.NameFilter;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.xpath.XPathAxis;

public class ConcurrentAxisTest {

  /** XML file name to test. */
  private static final String XMLFILE = "10mb.xml";
  
  /** Path to XML file. */
  private static final String XML = "src" + File.separator + "test"
    + File.separator + "resources" + File.separator + XMLFILE;

  private static Holder holder;

  /**
   * Method is called once before each test. It deletes all states, shreds XML
   * file to database and initializes the required variables.
   * 
   * @throws SirixException
   * 
   * @throws Exception
   */
  @BeforeEachRun
  @Before
  public void setUp() {
    try {
      TestHelper.deleteEverything();
      XMLShredder.main(XML, PATHS.PATH1.getFile().getAbsolutePath());
      holder = Holder.generateRtx();
    } catch (final Exception e) {

    }
  }

  /**
   * Test seriell.
   */
  // @Ignore
  // @SkipBench
  @Bench
  @Test
  public void testSeriellOld() {
    // final String query = "//people/person[@id=\"person3\"]/name";
    // final String query = "count(//location[text() = \"United States\"])";
    final String query = "//regions/africa//location";
    // final String result = "<name>Limor Simone</name>";
    final int resultNumber = 55;
    IAxis axis = null;
    try {
      axis = new XPathAxis(holder.getRtx(), query);
      for (int i = 0; i < resultNumber; i++) {
        assertEquals(true, axis.hasNext());
        axis.next();
      }
      assertEquals(false, axis.hasNext());
    } catch (final SirixXPathException ttExp) {
      fail();
    }
  }

  /**
   * Test seriell.
   */
  @Bench
  @Test
  public void testSeriellNew() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final IAxis axis =
      new NestedAxis(new NestedAxis(new FilterAxis(new DescendantAxis(holder
        .getRtx(), EIncludeSelf.YES),
        new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
        new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
          "africa"))), new FilterAxis(new DescendantAxis(holder.getRtx(),
        EIncludeSelf.YES), new NameFilter(holder.getRtx(), "location")));

    for (int i = 0; i < resultNumber; i++) {
      assertEquals(true, axis.hasNext());
      axis.next();
    }
    assertEquals(false, axis.hasNext());
  }

  /**
   * Test concurrent.
   * 
   * @throws SirixException
   * 
   * @throws SirixXPathException
   */
  @Bench
  @Test
  public final void testConcurrent() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    try {
      final INodeReadTrx firstConcurrRtx =
        holder.getSession().beginNodeReadTrx();
       final INodeReadTrx secondConcurrRtx =
       holder.getSession().beginNodeReadTrx();
       final INodeReadTrx thirdConcurrRtx =
       holder.getSession().beginNodeReadTrx();
      final INodeReadTrx firstRtx = holder.getSession().beginNodeReadTrx();
      final INodeReadTrx secondRtx = holder.getSession().beginNodeReadTrx();
      final INodeReadTrx thirdRtx = holder.getSession().beginNodeReadTrx();
      final IAxis axis =
        new NestedAxis(new NestedAxis(new ConcurrentAxis(firstConcurrRtx,
          new FilterAxis(new DescendantAxis(firstRtx, EIncludeSelf.YES),
            new NameFilter(firstRtx, "regions"))), new ConcurrentAxis(
              secondConcurrRtx, new FilterAxis(new ChildAxis(secondRtx),
            new NameFilter(secondRtx, "africa")))), new ConcurrentAxis(
              thirdConcurrRtx, new FilterAxis(new DescendantAxis(thirdRtx,
            EIncludeSelf.YES), new NameFilter(thirdRtx, "location"))));
      
      for (int i = 0; i < resultNumber; i++) {
        assertEquals(true, axis.hasNext());
        axis.next();
      }
      assertEquals(false, axis.hasNext());
    } catch (final SirixException e) {
      fail();
    }
  }

  /**
   * Test concurrent.
   * 
   * @throws SirixXPathException
   */
  @Bench
  @Test
  public final void testPartConcurrentDescAxis1() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    try {
      final INodeReadTrx firstConcurrRtx =
        holder.getSession().beginNodeReadTrx();
      final IAxis axis =
        new NestedAxis(new NestedAxis(new ConcurrentAxis(firstConcurrRtx,
          new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
            new NameFilter(holder.getRtx(), "regions"))), new FilterAxis(
          new ChildAxis(firstConcurrRtx), new NameFilter(firstConcurrRtx,
            "africa"))), new FilterAxis(new DescendantAxis(firstConcurrRtx,
          EIncludeSelf.YES), new NameFilter(firstConcurrRtx, "location")));

      for (int i = 0; i < resultNumber; i++) {
        assertEquals(true, axis.hasNext());
        axis.next();
      }
      assertEquals(false, axis.hasNext());
    } catch (final SirixException e) {
      fail();
    }
  }

  /**
   * Test concurrent.
   * 
   * @throws SirixXPathException
   */
  @Bench
  @Test
  public final void testPartConcurrentDescAxis2() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    try {
      final INodeReadTrx firstConcurrRtx =
        holder.getSession().beginNodeReadTrx();
      final IAxis axis =
        new NestedAxis(new NestedAxis(new FilterAxis(new DescendantAxis(
          firstConcurrRtx, EIncludeSelf.YES), new NameFilter(firstConcurrRtx,
          "regions")), new FilterAxis(new ChildAxis(firstConcurrRtx),
          new NameFilter(firstConcurrRtx, "africa"))), new ConcurrentAxis(
          firstConcurrRtx, new FilterAxis(new DescendantAxis(holder.getRtx(),
            EIncludeSelf.YES), new NameFilter(holder.getRtx(), "location"))));

      for (int i = 0; i < resultNumber; i++) {
        assertEquals(true, axis.hasNext());
        axis.next();
      }
      assertEquals(axis.hasNext(), false);
    } catch (final SirixException e) {
      fail();
    }

  }

  /*
   * ##########################################################################
   * ###############
   */

  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testSeriellNew2() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 1;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // IAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item"))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail"))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testConcurrent2() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 1;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // IAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item")))), new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox")))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail")))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent2() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 1;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // IAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item")))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail"))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /*
  // * ##########################################################################
  // * ###############
  // */
  //
  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testSeriellNew3() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "regions")), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "item"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testCompleteConcurrent3() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "regions"))),
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "item")))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox")))), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent3Axis1() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "regions"))),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item"))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent3Axis2() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "regions")), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item")))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent3Axis1and2() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "regions"))),
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "item")))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent3Axis1and3() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "regions"))),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item"))), new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox")))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public final void testPartConcurrent3Axis2and4() {
  // /* query: //regions//item/mailbox/mail */
  // final int resultNumber = 2139; // 10mb xmark
  // // final int resultNumber = 20946; // 100mb xmark
  // // final int resultNumber = 208497; // 1000mb xmark
  //
  // final IAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "regions")), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item")))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /*
  // * ##########################################################################
  // * ###############
  // */
  //
  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testSeriellNew4() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item"))), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "mailbox"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail"))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testConcurrent4() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item")))), new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox")))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail")))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testConcurrent4ChildAxis() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "item"))), new ConcurrentAxis(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mailbox")))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "mail")))), new PredicateFilterAxis(
  // holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testConcurrent4DescAxis1() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "item"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))), new PredicateFilterAxis(holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testConcurrent4DescAxis2() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "item")))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))), new PredicateFilterAxis(holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @SkipBench
  // @Ignore
  // @Test
  // public final void testConcurrent4DescAxises() {
  // /* query: //regions//item/mailbox/mail[date="02/24/2000"] */
  // final int resultNumber = 22;
  //
  // long date =
  // holder.getRtx().getItemList().addItem(
  // new AtomicValue(TypedValue.getBytes("02/24/2000"), holder.getRtx()
  // .keyForName("xs:string")));
  // AbsAxis literal = new LiteralExpr(holder.getRtx(), date);
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "item")))),
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new ChildAxis(holder
  // .getRtx()), new NameFilter(holder.getRtx(), "mailbox")))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))), new PredicateFilterAxis(holder.getRtx(),
  // new NestedAxis(new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "date")), new GeneralComp(holder
  // .getRtx(), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new TextFilter(holder.getRtx())), literal, CompKind.EQ))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /*
  // * ##########################################################################
  // * ###############
  // */
  //
  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testSeriellNew5() {
  // /* query: //description//listitem/text */
  // final int resultNumber = 5363;
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new FilterAxis(new DescendantAxis(holder
  // .getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "description")), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "listitem"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "text")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testConcurrent5() {
  // /* query: //description//listitem/text */
  // final int resultNumber = 5363;
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "description"))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "listitem")))),
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new ChildAxis(holder
  // .getRtx()), new NameFilter(holder.getRtx(), "text"))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testConcurrentPart5Axis1() {
  // /* query: //description//listitem/text */
  // final int resultNumber = 5363;
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "description"))), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "listitem"))), new FilterAxis(new ChildAxis(holder
  // .getRtx()), new NameFilter(holder.getRtx(), "text")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testConcurrentPart5Axis2() {
  // /* query: //description//listitem/text */
  // final int resultNumber = 5363;
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "description"))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "listitem")))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "text")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /*
  // * ##########################################################################
  // * ###############
  // */
  //
  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testSeriellNew6() {
  // /* query: //regions//item/mailbox/mail */
  // // final int resultNumber = 20946; //100mb xmark
  // final int resultNumber = 544; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "africa"))), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "item"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testConcurrent6() {
  // /* query: //regions//item/mailbox/mail */
  // // final int resultNumber = 20946; //100mb xmark
  // final int resultNumber = 544; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "africa")))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "item")))), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mailbox")))), new ConcurrentAxis(holder.getRtx(),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail"))));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testPartConcurrent6Axis1() {
  // /* query: //regions//item/mailbox/mail */
  // // final int resultNumber = 20946; //100mb xmark
  // final int resultNumber = 544; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "africa"))), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "item"))), new FilterAxis(new ChildAxis(holder
  // .getRtx()), new NameFilter(holder.getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testPartConcurrent6Axis2() {
  // /* query: //regions//item/mailbox/mail */
  // // final int resultNumber = 20946; //100mb xmark
  // final int resultNumber = 544; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new FilterAxis(new DescendantAxis(holder.getRtx(), EIncludeSelf.YES),
  // new NameFilter(holder.getRtx(), "regions")), new FilterAxis(
  // new ChildAxis(holder.getRtx()), new NameFilter(holder.getRtx(),
  // "africa"))), new ConcurrentAxis(holder.getRtx(), new FilterAxis(
  // new DescendantAxis(holder.getRtx(), EIncludeSelf.YES), new NameFilter(
  // holder.getRtx(), "item")))), new FilterAxis(new ChildAxis(holder
  // .getRtx()), new NameFilter(holder.getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /**
  // * Test concurrent.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Ignore
  // @SkipBench
  // @Test
  // public final void testPartConcurrent6Axis1and2() {
  // /* query: //regions//item/mailbox/mail */
  // // final int resultNumber = 20946; //100mb xmark
  // final int resultNumber = 544; // 1000mb xmark
  //
  // final AbsAxis axis =
  // new NestedAxis(new NestedAxis(new NestedAxis(new NestedAxis(
  // new ConcurrentAxis(holder.getRtx(), new FilterAxis(new DescendantAxis(
  // holder.getRtx(), EIncludeSelf.YES), new NameFilter(holder.getRtx(),
  // "regions"))), new FilterAxis(new ChildAxis(holder.getRtx()),
  // new NameFilter(holder.getRtx(), "africa"))), new ConcurrentAxis(
  // holder.getRtx(), new FilterAxis(new DescendantAxis(holder.getRtx(),
  // EIncludeSelf.YES), new NameFilter(holder.getRtx(), "item")))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mailbox"))),
  // new FilterAxis(new ChildAxis(holder.getRtx()), new NameFilter(holder
  // .getRtx(), "mail")));
  //
  // for (int i = 0; i < resultNumber; i++) {
  // assertEquals(true, axis.hasNext());
  // axis.next();
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }

  /*
   * ##########################################################################
   * ###############
   */

  /**
   * Close all connections.
   * 
   * @throws SirixException
   */
  @AfterEachRun
  @After
  public void tearDown() {
    try {
      holder.close();
      TestHelper.closeEverything();
    } catch (final SirixException e) {
    }
  }

}
