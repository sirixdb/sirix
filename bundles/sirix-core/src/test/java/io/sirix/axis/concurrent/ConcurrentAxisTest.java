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

package io.sirix.axis.concurrent;

import io.sirix.api.Axis;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.NestedAxis;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.xml.XmlNameFilter;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.service.xml.xpath.XPathAxis;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/** Test {@link ConcurrentAxis}. */
public final class ConcurrentAxisTest {

  /** DEBUG FLAG: Enable with -Dsirix.debug.leak.diagnostics=true */
  private static final boolean DEBUG_LEAK_DIAGNOSTICS = 
    true; // TEMP
    //Boolean.getBoolean("sirix.debug.leak.diagnostics");

  /** XML file name to test. */
  private static final String XMLFILE = "10mb.xml";

  /** Path to XML file. */
  private static final Path XML = Paths.get("src", "test", "resources", XMLFILE);

  private Holder holder;

  /**
   * Method is called once before each test. It deletes all states, shreds XML file to database and
   * initializes the required variables.
   *
   */
  @BeforeEach
  public void setUp() {
    try {
      // Reset diagnostics (but NOT ALL_LIVE_PAGES to track cumulative leaks)
      if (DEBUG_LEAK_DIAGNOSTICS) {
        io.sirix.page.KeyValueLeafPage.PAGES_CREATED.set(0);
        io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.set(0);
        io.sirix.page.KeyValueLeafPage.PAGES_BY_TYPE.clear();
        io.sirix.page.KeyValueLeafPage.PAGES_CLOSED_BY_TYPE.clear();
      }
      
      XmlTestHelper.deleteEverything();
      XmlShredder.main(XML.toAbsolutePath().toString(), PATHS.PATH1.getFile().toAbsolutePath().toString());
      holder = Holder.generateRtx();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Close all connections.
   *
   */
  @AfterEach
  public void tearDown() {
    try {
      holder.close();
      XmlTestHelper.closeEverything();
      
      if (DEBUG_LEAK_DIAGNOSTICS) {
        // DIAGNOSTIC: Print page statistics
        System.err.println("\n========== PAGE STATISTICS ==========");
        System.err.println("PAGES_CREATED: " + io.sirix.page.KeyValueLeafPage.PAGES_CREATED.get());
        System.err.println("PAGES_CLOSED: " + io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.get());
        System.err.println("UNCLOSED LEAK: " + (io.sirix.page.KeyValueLeafPage.PAGES_CREATED.get() - io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.get()));
        
        System.err.println("\nPages by type (CREATED / CLOSED / LEAKED):");
        io.sirix.page.KeyValueLeafPage.PAGES_BY_TYPE.forEach((type, created) -> {
          long closed = io.sirix.page.KeyValueLeafPage.PAGES_CLOSED_BY_TYPE.getOrDefault(type, new java.util.concurrent.atomic.AtomicLong(0)).get();
          long leaked = created.get() - closed;
          System.err.println("  " + type + ": " + created.get() + " / " + closed + " / " + leaked);
        });
        
        System.err.println("\n=== LEAKED PAGE ANALYSIS ===");
        System.err.println("Live pages still in memory: " + io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.size());
        
        // Check how many leaked pages are in cache vs swizzled only
        try {
          var bufMgr = holder.getXmlNodeReadTrx().getPageTrx().getBufferManager();
          long inRecordCache = io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.stream()
            .filter(p -> bufMgr.getRecordPageCache().asMap().containsValue(p))
            .count();
          long inFragmentCache = io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.stream()
            .filter(p -> bufMgr.getRecordPageFragmentCache().asMap().containsValue(p))
            .count();
          
          System.err.println("  In RecordPageCache: " + inRecordCache);
          System.err.println("  In RecordPageFragmentCache: " + inFragmentCache);
          System.err.println("  Only swizzled (not in cache): " + (io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.size() - inRecordCache - inFragmentCache));
          
          // Group by type and check pin status
          var leakedByType = new java.util.HashMap<IndexType, java.util.List<KeyValueLeafPage>>();
          io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.forEach(page -> {
            leakedByType.computeIfAbsent(page.getIndexType(), _ -> new java.util.ArrayList<>()).add(page);
          });
          
          leakedByType.forEach((type, pages) -> {
            long pinned = pages.stream().filter(p -> p.getPinCount() > 0).count();
            System.err.println("  " + type + ": " + pages.size() + " leaked (" + pinned + " still pinned)");
            
            // Sample first 5 leaked pages for detailed analysis
            pages.stream().limit(5).forEach(page -> {
              System.err.println("    Page " + page.getPageKey() + ": pinCount=" + page.getPinCount() + 
                               ", closed=" + page.isClosed() +
                               ", pinByTrx=" + page.getPinCountByTransaction());
            });
          });
          
          System.err.println("\nLeak Analysis:");
          long notClosed = io.sirix.page.KeyValueLeafPage.PAGES_CREATED.get() - io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.get();
          long stillLive = io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES.size();
          long finalized = io.sirix.page.KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
          long missing = notClosed - stillLive - finalized;
          
          System.err.println("  Not closed: " + notClosed);
          System.err.println("  Still in ALL_LIVE_PAGES: " + stillLive);
          System.err.println("  Finalized without close: " + finalized);
          System.err.println("  MISSING (accounting gap): " + missing);
        } catch (Exception ex) {
          System.err.println("  (Detailed analysis skipped - transaction already closed)");
        }
        System.err.println("=====================================\n");
      }
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Test seriell.
   */
  // @Ignore
  // @SkipBench
  // @Bench
  @Test
  public void testSeriellOld() {
    // final String query = "//people/person[@id=\"person3\"]/name";
    // final String query = "count(//location[text() = \"United States\"])";
    final String query = "//regions/africa//location";
    // final String result = "<name>Limor Simone</name>";
    final int resultNumber = 55;
    final Axis axis = new XPathAxis(holder.getXmlNodeReadTrx(), query);
    for (int i = 0; i < resultNumber; i++) {
      assertTrue(axis.hasNext());
      axis.nextLong();
    }
    assertFalse(axis.hasNext());
  }

  /**
   * Test seriell.
   */
  // @Bench
  @Test
  public void testSeriellNew() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var axis = new NestedAxis(new NestedAxis(
        new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
            new XmlNameFilter(holder.getXmlNodeReadTrx(), "regions")),
        new FilterAxis<>(new ChildAxis(holder.getXmlNodeReadTrx()), new XmlNameFilter(holder.getXmlNodeReadTrx(), "africa"))),
                                    new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
            new XmlNameFilter(holder.getXmlNodeReadTrx(), "location")));

    for (int i = 0; i < resultNumber; i++) {
      assertTrue(axis.hasNext());
      axis.nextLong();
    }
    assertFalse(axis.hasNext());
  }

  /**
   * Test concurrent.
   *
   */
  @RepeatedTest(20)
  public void testConcurrent() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var firstConcurrRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var secondConcurrRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var thirdConcurrRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var thirdRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final Axis axis =
        new NestedAxis(
            new NestedAxis(
                new ConcurrentAxis<>(firstConcurrRtx,
                    new FilterAxis<>(new DescendantAxis(firstRtx, IncludeSelf.YES),
                        new XmlNameFilter(firstRtx, "regions"))),
                new ConcurrentAxis<>(secondConcurrRtx,
                    new FilterAxis<>(new ChildAxis(secondRtx), new XmlNameFilter(secondRtx, "africa")))),
            new ConcurrentAxis<>(thirdConcurrRtx,
                new FilterAxis<>(new DescendantAxis(thirdRtx, IncludeSelf.YES), new XmlNameFilter(thirdRtx, "location"))));

    for (int i = 0; i < resultNumber; i++) {
      assertTrue(axis.hasNext());
      axis.nextLong();
    }
    assertFalse(axis.hasNext());
  }

  /**
   * Test concurrent.
   *
   */
  // @Bench
  @RepeatedTest(10)
  public void testPartConcurrentDescAxis1() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var firstConcurrRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var axis = new NestedAxis(
        new NestedAxis(
            new ConcurrentAxis<>(firstConcurrRtx,
                new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
                    new XmlNameFilter(holder.getXmlNodeReadTrx(), "regions"))),
            new FilterAxis<>(new ChildAxis(firstConcurrRtx), new XmlNameFilter(firstConcurrRtx, "africa"))),
        new FilterAxis<>(new DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
            new XmlNameFilter(firstConcurrRtx, "location")));

    for (int i = 0; i < resultNumber; i++) {
      assertTrue(axis.hasNext());
      axis.nextLong();
    }
    assertFalse(axis.hasNext());
  }

  /**
   * Test concurrent.
   *
   */
  // @Bench
  @RepeatedTest(10)
  public void testPartConcurrentDescAxis2() {
    /* query: //regions/africa//location */
    final int resultNumber = 55;
    final var firstConcurrRtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    final var axis = new NestedAxis(
        new NestedAxis(
            new FilterAxis<>(new DescendantAxis(firstConcurrRtx, IncludeSelf.YES),
                new XmlNameFilter(firstConcurrRtx, "regions")),
            new FilterAxis<>(new ChildAxis(firstConcurrRtx), new XmlNameFilter(firstConcurrRtx, "africa"))),
        new ConcurrentAxis<>(firstConcurrRtx,
            new FilterAxis<>(new DescendantAxis(holder.getXmlNodeReadTrx(), IncludeSelf.YES),
                new XmlNameFilter(holder.getXmlNodeReadTrx(), "location"))));

    for (int i = 0; i < resultNumber; i++) {
      assertTrue(axis.hasNext());
      axis.nextLong();
    }
    assertFalse(axis.hasNext());
  }

  /*
   * ########################################################################## ###############
   */

  // /**
  // * Test seriell.
  // *
  // * @throws TTXPathException
  // */
  // @Bench
  // @Test
  // public void testSeriellNew2() {
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
  // axis.nextLong()
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
  // public void testConcurrent2() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent2() {
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
  // axis.nextLong()
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /*
  // *
  // ##########################################################################
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
  // public void testSeriellNew3() {
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
  // axis.nextLong()
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
  // public void testCompleteConcurrent3() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent3Axis1() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent3Axis2() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent3Axis1and2() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent3Axis1and3() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent3Axis2and4() {
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
  // axis.nextLong()
  // }
  // assertEquals(false, axis.hasNext());
  // }
  //
  // /*
  // *
  // ##########################################################################
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
  // public void testSeriellNew4() {
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
  // axis.nextLong()
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
  // public void testConcurrent4() {
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
  // axis.nextLong()
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
  // public void testConcurrent4ChildAxis() {
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
  // axis.nextLong()
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
  // public void testConcurrent4DescAxis1() {
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
  // axis.nextLong()
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
  // public void testConcurrent4DescAxis2() {
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
  // axis.nextLong()
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
  // public void testConcurrent4DescAxises() {
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
  // axis.nextLong()
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /*
  // *
  // ##########################################################################
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
  // public void testSeriellNew5() {
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
  // axis.nextLong()
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
  // public void testConcurrent5() {
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
  // axis.nextLong()
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
  // public void testConcurrentPart5Axis1() {
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
  // axis.nextLong()
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
  // public void testConcurrentPart5Axis2() {
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
  // axis.nextLong()
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }
  //
  // /*
  // *
  // ##########################################################################
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
  // public void testSeriellNew6() {
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
  // axis.nextLong()
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
  // public void testConcurrent6() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent6Axis1() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent6Axis2() {
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
  // axis.nextLong()
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
  // public void testPartConcurrent6Axis1and2() {
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
  // axis.nextLong()
  // }
  // assertEquals(axis.hasNext(), false);
  //
  // }

  /*
   * ########################################################################## ###############
   */

}
