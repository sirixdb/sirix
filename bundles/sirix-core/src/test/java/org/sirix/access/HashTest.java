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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import java.math.BigInteger;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.XdmTestHelper;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;

public class HashTest {

  private final static String NAME1 = "a";
  private final static String NAME2 = "b";

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
  }

  @Test
  public void testPostorderInsertRemove() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.POSTORDER);
    testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testPostorderDeep() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.POSTORDER);
    testDeepTree(wtx);
  }

  @Test
  public void testPostorderSetter() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.POSTORDER);
    testSetter(wtx);
  }

  @Test
  public void testRollingInsertRemove() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.ROLLING);
    testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testRollingDeep() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.ROLLING);
    testDeepTree(wtx);
  }

  @Test
  public void testRollingSetter() throws SirixException {
    final XmlNodeTrx wtx = createWtx(HashType.ROLLING);
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
   *
   * @param wtx
   */
  @Ignore
  private void testHashTreeWithInsertAndRemove(final XmlNodeTrx wtx) {

    // inserting a element as root
    wtx.insertElementAsFirstChild(new QNm(NAME1));
    final long rootKey = wtx.getNodeKey();
    final BigInteger firstRootHash = wtx.getHash();

    // inserting a text as second child of root
    wtx.moveTo(rootKey);
    wtx.insertTextAsFirstChild(NAME1);
    wtx.moveToParent();
    final BigInteger secondRootHash = wtx.getHash();

    // inserting a second element on level 2 under the only element
    wtx.moveToFirstChild();
    wtx.insertElementAsRightSibling(new QNm(NAME2));
    wtx.insertAttribute(new QNm(NAME2), NAME1);
    wtx.moveTo(rootKey);
    final BigInteger thirdRootHash = wtx.getHash();

    // Checking that all hashes are different
    assertFalse(firstRootHash.equals(secondRootHash));
    assertFalse(firstRootHash.equals(thirdRootHash));
    assertFalse(secondRootHash.equals(thirdRootHash));

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
    final BigInteger oldHash = wtx.getHash();

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
    final BigInteger hashRoot1 = wtx.getHash();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    final BigInteger hashLeaf1 = wtx.getHash();
    wtx.setName(new QNm(NAME2));
    final BigInteger hashLeaf2 = wtx.getHash();
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    final BigInteger hashRoot2 = wtx.getHash();
    assertFalse(hashRoot1.equals(hashRoot2));
    assertFalse(hashLeaf1.equals(hashLeaf2));
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.setName(new QNm(NAME1));
    final BigInteger hashLeaf3 = wtx.getHash();
    assertEquals(hashLeaf1, hashLeaf3);
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    final BigInteger hashRoot3 = wtx.getHash();
    assertEquals(hashRoot1, hashRoot3);

    // Testing root inheritance
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.setName(new QNm(NAME2));
    final BigInteger hashRoot4 = wtx.getHash();
    assertFalse(hashRoot4.equals(hashRoot2));
    assertFalse(hashRoot4.equals(hashRoot1));
    assertFalse(hashRoot4.equals(hashRoot3));
    assertFalse(hashRoot4.equals(hashLeaf1));
    assertFalse(hashRoot4.equals(hashLeaf2));
    assertFalse(hashRoot4.equals(hashLeaf3));
  }

  private XmlNodeTrx createWtx(final HashType kind) throws SirixException {
    final var database = XdmTestHelper.getDatabase(XdmTestHelper.PATHS.PATH1.getFile());
    database.createResource(new ResourceConfiguration.Builder(XdmTestHelper.RESOURCE).build());
    final XmlResourceManager manager = database.openResourceManager(XdmTestHelper.RESOURCE);
    final XmlNodeTrx wTrx = manager.beginNodeTrx();
    return wTrx;
  }

  @After
  public void tearDown() throws SirixException {
    XdmTestHelper.closeEverything();
  }

}
