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

package io.sirix.access.node.xml;

import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.brackit.query.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public final class HashTest {

  private final static String NAME1 = "a";
  private final static String NAME2 = "b";

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  @Test
  public void testPostorderInsertRemove() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testPostorderDeep() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testDeepTree(wtx);
  }

  @Test
  public void testPostorderSetter() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testSetter(wtx);
  }

  @Test
  public void testRollingInsertRemove() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testRollingDeep() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testDeepTree(wtx);
  }

  @Test
  public void testRollingSetter() throws SirixException {
    final XmlNodeTrx wtx = createWtx();
    testSetter(wtx);
  }

  /**
   * Inserting nodes and removing them.
   *
   * <pre>
   * -a (1)
   *  '-test (5)
   *  '-a (6)
   *    '-attr(7)
   *    '-a (8)
   *      '-attr (9)
   *  '-text (2)
   *  '-a (3(x))
   *    '-attr(4(x))
   * </pre>
   */
  private void testHashTreeWithInsertAndRemove(final XmlNodeTrx wtx) {

    // inserting a element as root
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    final long rootKey = wtx.getNodeKey();
    final long firstRootHash = wtx.getHash();

    // inserting a text as second child of root
    wtx.moveTo(rootKey);
    wtx.insertTextAsFirstChild(NAME1);
    wtx.moveToParent();
    final long secondRootHash = wtx.getHash();

    // inserting a second element on level 2 under the only element
    wtx.moveToFirstChild();
    wtx.insertElementAsRightSibling(new QNm(NAME2));
    wtx.insertAttribute(new QNm(NAME2), NAME1);
    wtx.moveTo(rootKey);
    final long thirdRootHash = wtx.getHash();

    // Checking that all hashes are different
    assertNotEquals(firstRootHash, secondRootHash);
    assertNotEquals(firstRootHash, thirdRootHash);
    assertNotEquals(secondRootHash, thirdRootHash);

    // removing the second element
    wtx.moveToFirstChild();
    wtx.moveToRightSibling();
    wtx.remove();
    wtx.moveTo(rootKey);
    assertEquals(secondRootHash, wtx.getHash());

    // adding additional element for showing that hashes are computed incrementally
    wtx.insertTextAsFirstChild(NAME1);
    wtx.insertElementAsRightSibling(new QNm(NAME1));
    wtx.insertAttribute(new QNm(NAME1), NAME2);
    wtx.moveToParent();
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.insertAttribute(new QNm(NAME2), NAME1);

    wtx.moveTo(rootKey);
    wtx.moveToFirstChild();
    wtx.remove();
    wtx.remove();

    wtx.moveTo(rootKey);
    assertEquals(firstRootHash, wtx.getHash());
  }

  @Ignore
  private void testDeepTree(final XmlNodeTrx wtx) throws SirixException {

    wtx.insertElementAsFirstChild(new QNm(NAME1));
    final long oldHash = wtx.getHash();

    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.insertElementAsFirstChild(new QNm(NAME2));
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.insertElementAsFirstChild(new QNm(NAME2));
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.remove();
    wtx.insertElementAsFirstChild(new QNm(NAME2));
    wtx.insertElementAsFirstChild(new QNm(NAME2));
    wtx.insertElementAsFirstChild(new QNm(NAME1));

    wtx.moveTo(1);
    wtx.moveToFirstChild();
    wtx.remove();
    assertEquals(oldHash, wtx.getHash());
  }

  @Ignore
  private void testSetter(final XmlNodeTrx wtx) throws SirixException {

    // Testing node inheritance
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    final long hashRoot1 = wtx.getHash();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    final long hashLeaf1 = wtx.getHash();
    wtx.setName(new QNm(NAME2));
    final long hashLeaf2 = wtx.getHash();
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    final long hashRoot2 = wtx.getHash();
    assertNotEquals(hashRoot1, hashRoot2);
    assertNotEquals(hashLeaf1, hashLeaf2);
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.setName(new QNm(NAME1));
    final long hashLeaf3 = wtx.getHash();
    assertEquals(hashLeaf1, hashLeaf3);
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    final long hashRoot3 = wtx.getHash();
    assertEquals(hashRoot1, hashRoot3);

    // Testing root inheritance
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.setName(new QNm(NAME2));
    final long hashRoot4 = wtx.getHash();
    assertNotEquals(hashRoot4, hashRoot2);
    assertNotEquals(hashRoot4, hashRoot1);
    assertNotEquals(hashRoot4, hashRoot3);
    assertNotEquals(hashRoot4, hashLeaf1);
    assertNotEquals(hashRoot4, hashLeaf2);
    assertNotEquals(hashRoot4, hashLeaf3);
  }

  private XmlNodeTrx createWtx() throws SirixException {
    final var database = XmlTestHelper.getDatabaseWithRollingHashesEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
    final XmlNodeTrx wTrx = manager.beginNodeTrx();
    return wTrx;
  }

  @After
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
  }

}
