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

package io.sirix.settings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.XmlTestHelper;

/**
 * Test revisioning.
 */
public class VersioningTest {

  /**
   * {@link Database} instance.
   */
  private Database<XmlResourceSession> database;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    Databases.createXmlDatabase(new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile()));
    database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile());
  }

  @After
  public void tearDown() {
    database.close();
  }

  @Test
  public void testIncremental() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.INCREMENTAL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test();
  }

  @Test
  public void testIncremental1() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.INCREMENTAL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test1();
  }

  @Test
  public void testIncremental2() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.INCREMENTAL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test2();
  }

  @Test
  public void testDifferential() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.DIFFERENTIAL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test();
  }

  @Test
  public void testDifferential1() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.DIFFERENTIAL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test1();
  }

  @Test
  public void testFull() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.FULL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test();
  }

  @Test
  public void testFull1() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.FULL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test1();
  }

  @Test
  public void testFull2() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.FULL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test2();
  }

  @Test
  public void testFull3() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.FULL)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    testModifySamePageMultipleTimes();
  }


  @Test
  public void testSlidingSnapshot() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test();
  }

  @Test
  public void testSlidingSnapshot1() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test1();
  }

  @Test
  public void testSlidingSnapshot2() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    test2();
  }

  @Test
  public void testSlidingSnapshot3() {
    database.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                                                     .hashKind(HashType.NONE)
                                                                                     .maxNumberOfRevisionsToRestore(3)
                                                                                     .build());
    testModifySamePageMultipleTimes();
  }

  /**
   * Test revisioning.
   *
   * @throws SirixException if anything in Sirix fails
   */
  public void testModifySamePageMultipleTimes() {
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final XmlNodeTrx wtx = manager.beginNodeTrx()) {
      for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
        wtx.insertElementAsFirstChild(new QNm("rev1"));
      }
      wtx.commit();
      setNames(wtx, "rev2");
      setNames(wtx, "rev3");
      setNames(wtx, "rev4");
      setNames(wtx, "rev5");
      setNames(wtx, "rev6");
      setNames(wtx, "rev7");
      assertions(manager, 1);
      assertions(manager, 2);
      assertions(manager, 3);
      assertions(manager, 4);
      assertions(manager, 5);
      assertions(manager, 6);
      assertions(manager, 7);
    }
  }

  private static void assertions(XmlResourceSession manager, int revision) {
    try (XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(revision)) {
      for (int i = 1; i < Constants.NDP_NODE_COUNT - 1; i++) {
        rtx.moveTo(i);
        assertEquals(new QNm("rev" + revision), rtx.getName());
      }
    }
  }

  private static void setNames(XmlNodeTrx wtx, String rev2) {
    for (int i = 1; i < Constants.NDP_NODE_COUNT - 1; i++) {
      wtx.moveTo(i);
      wtx.setName(new QNm(rev2));
    }
    wtx.commit();
  }

  /**
   * Test revisioning.
   *
   * @throws SirixException if anything in Sirix fails
   */
  public void test() {
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      try (final XmlNodeTrx wtx = manager.beginNodeTrx()) {
        for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
          wtx.insertElementAsFirstChild(new QNm("foo"));
        }
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), Constants.NDP_NODE_COUNT - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT << 1) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 3) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT << 2) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 5) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 6) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 7) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT << 3) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 9) - 1);
        fillNodePage(wtx);
        wtx.commit();
        Assert.assertEquals(wtx.getNodeKey(), (Constants.NDP_NODE_COUNT * 10) - 1);
        try (final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
          for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
            assertTrue(rtx.moveToFirstChild());
          }
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
          move(rtx);
        }
      }
    }
  }

  /**
   * Test revisioning.
   *
   * @throws SirixException if anything in Sirix fails
   */
  public void test1() {
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      XmlNodeTrx wtx = manager.beginNodeTrx();
      for (int i = 0; i < Constants.NDP_NODE_COUNT - 1; i++) {
        wtx.insertElementAsFirstChild(new QNm("foo"));
      }
      wtx.commit();
      Assert.assertEquals(wtx.getNodeKey(), Constants.NDP_NODE_COUNT - 1);
      wtx.close();
      wtx = manager.beginNodeTrx();
      setBaaaz(wtx);
      setFooBar(wtx);
      setFoooo(wtx);
      wtx.moveTo(Constants.NDP_NODE_COUNT - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT << 1) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT * 3) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT << 2) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT * 5) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT * 6) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT * 7) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT << 3) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      wtx = manager.beginNodeTrx();
      wtx.moveTo((Constants.NDP_NODE_COUNT * 9) - 1);
      fillNodePage(wtx);
      wtx.commit();
      wtx.close();
      try (final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToFirstChild());
        assertEquals(new QNm("foobar"), rtx.getName());
        assertTrue(rtx.moveToFirstChild());
        assertEquals(new QNm("foooo"), rtx.getName());
        for (int i = 0; i < Constants.NDP_NODE_COUNT - 4; i++) {
          assertTrue(rtx.moveToFirstChild());
        }
        assertEquals(new QNm("baaaz"), rtx.getName());
      }
    }
  }

  /**
   * Test revisioning.
   *
   * @throws SirixException if anything in Sirix fails
   */
  public void test2() {
    try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      XmlNodeTrx wtx = manager.beginNodeTrx();
      wtx.insertElementAsFirstChild(new QNm("foo"));
      wtx.commit();
      wtx.insertElementAsFirstChild(new QNm("foo"));
      wtx.commit();
      wtx.insertElementAsFirstChild(new QNm("foo"));
      wtx.commit();
      wtx.insertElementAsFirstChild(new QNm("foo"));
      wtx.commit();
      wtx.insertElementAsFirstChild(new QNm("foo"));
      wtx.commit();
      wtx.close();
      try (final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.moveToFirstChild());
      }
    }
  }

  /**
   * Set the second {@link QNm} in the first node page.
   *
   * @param wtx {@link XmlNodeTrx} instance
   * @throws SirixException if inserting elements fails
   */
  private void setFoooo(final XmlNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.setName(new QNm("foooo"));
  }

  /**
   * Set the first {@link QNm} in the first node page.
   *
   * @param wtx {@link XmlNodeTrx} instance
   * @throws SirixException if inserting elements fails
   */
  private void setFooBar(final XmlNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.setName(new QNm("foobar"));
  }

  /**
   * Set the last {@link QNm} in the first node page.
   *
   * @param wtx {@link XmlNodeTrx} instance
   * @throws SirixException if inserting elements fails
   */
  private void setBaaaz(final XmlNodeTrx wtx) {
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    for (int i = 0; i < Constants.NDP_NODE_COUNT - 3; i++) {
      wtx.moveToFirstChild();
    }
    wtx.setName(new QNm("baaaz"));
  }

  /**
   * Fill node page.
   *
   * @param wtx {@link XmlNodeTrx} instance
   * @throws SirixException if inserting elements fails
   */
  private void fillNodePage(final XmlNodeTrx wtx) {
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      wtx.insertElementAsFirstChild(new QNm("foo"));
    }
  }

  /**
   * Move through all nodes in a node page.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} instance
   * @throws SirixException if movement fails
   */
  private void move(final XmlNodeReadOnlyTrx rtx) {
    for (int i = 0; i < Constants.NDP_NODE_COUNT; i++) {
      assertTrue(rtx.moveToFirstChild());
    }
  }

  // // @Test(expected = AssertionError.class)
  // @Test
  // public void testFulldumpCombinePages() {
  // final NodePage[] pages = new NodePage[1];
  // pages[0] = getNodePage(1, 0, 128, 0);
  // // pages[1] = getNodePage(0, 0, 128, 0);
  //
  // final NodePage page = ERevisioning.FULL.combineNodePages(pages,
  // ResourceConfiguration.VERSIONSTORESTORE);
  //
  // for (int j = 0; j < page.getNodes().length; j++) {
  // assertEquals(pages[0].getNode(j), page.getNode(j));
  // }
  //
  // }
  //
  // @Test
  // public void testDifferentialCombinePages() {
  // final NodePage[] pages = prepareNormal(2);
  // final NodePage page =
  // ERevisioning.DIFFERENTIAL.combineNodePages(pages,
  // ResourceConfiguration.VERSIONSTORESTORE);
  //
  // for (int j = 0; j < 32; j++) {
  // assertEquals(pages[0].getNode(j), page.getNode(j));
  // }
  // for (int j = 32; j < page.getNodes().length; j++) {
  // assertEquals(pages[1].getNode(j), page.getNode(j));
  // }
  //
  // }
  //
  // @Test
  // public void testIncrementalCombinePages() {
  // final NodePage[] pages = prepareNormal(4);
  // final NodePage page =
  // ERevisioning.INCREMENTAL.combineNodePages(pages, 4);
  // checkCombined(pages, page);
  // }
  //
  // private static NodePage[] prepareNormal(final int length) {
  // final NodePage[] pages = new NodePage[length];
  // pages[pages.length - 1] = getNodePage(0, 0, 128, 0);
  // for (int i = 0; i < pages.length - 1; i++) {
  // pages[i] = getNodePage(pages.length - i - 1, i * 32, (i * 32) + 32, 0);
  // }
  // return pages;
  // }
  //
  // // private static NodePage[] prepareOverlapping(final int length) {
  // // final NodePage[] pages = new NodePage[length];
  // // final int[] borders = new int[4];
  // // pages[pages.length - 1] = getNodePage(0, 0, 128);
  // // for (int i = 0; i < pages.length - 1; i++) {
  // // borders[i] = random.nextInt(32) + ((i) * 32);
  // // pages[i] = getNodePage(pages.length - i, borders[i], (i * 32) + 32);
  // // }
  // // return pages;
  // //
  // // }
  //
  // private static void checkCombined(final NodePage[] toCheck, final NodePage
  // page) {
  // for (int i = 0; i < 4; i++) {
  // for (int j = i * 32; j < (i * 32) + 32; j++) {
  // assertEquals(toCheck[i].getNode(j), page.getNode(j));
  // }
  // }
  // }

}
