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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.xml.namespace.QName;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.settings.EFixed;

public class HashTest {

  private final static String NAME1 = "a";
  private final static String NAME2 = "b";

  @Before
  public void setUp() throws AbsTTException {
      TestHelper.deleteEverything();

  }

  @Test
  public void testPostorderInsertRemove() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Postorder);
      testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testPostorderDeep() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Postorder);
      testDeepTree(wtx);
  }

  @Test
  public void testPostorderSetter() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Postorder);
      testSetter(wtx);
  }

  @Test
  public void testRollingInsertRemove() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Rolling);
      testHashTreeWithInsertAndRemove(wtx);
  }

  @Test
  public void testRollingDeep() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Rolling);
      testDeepTree(wtx);
  }

  @Test
  public void testRollingSetter() throws AbsTTException {
      final INodeWriteTrx wtx = createWtx(EHashKind.Rolling);
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
   * @throws TTException
   */
  @Ignore
  private void testHashTreeWithInsertAndRemove(final INodeWriteTrx wtx) throws AbsTTException {

      // inserting a element as root
      wtx.insertElementAsFirstChild(new QName(NAME1));
      final long rootKey = wtx.getNode().getNodeKey();
      final long firstRootHash = wtx.getNode().getHash();

      // inserting a text as second child of root
      wtx.moveTo(rootKey);
      wtx.insertTextAsFirstChild(NAME1);
      wtx.moveTo(wtx.getNode().getParentKey());
      final long secondRootHash = wtx.getNode().getHash();

      // inserting a second element on level 2 under the only element
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.insertElementAsRightSibling(new QName(NAME2));
      wtx.insertAttribute(new QName(NAME2), NAME1);
      wtx.moveTo(rootKey);
      final long thirdRootHash = wtx.getNode().getHash();

      // Checking that all hashes are different
      assertFalse(firstRootHash == secondRootHash);
      assertFalse(firstRootHash == thirdRootHash);
      assertFalse(secondRootHash == thirdRootHash);

      // removing the second element
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.moveTo(((IStructNode)wtx.getNode()).getRightSiblingKey());
      wtx.remove();
      wtx.moveTo(rootKey);
      assertEquals(secondRootHash, wtx.getNode().getHash());

      // adding additional element for showing that hashes are computed
      // incrementilly
      wtx.insertTextAsFirstChild(NAME1);
      wtx.insertElementAsRightSibling(new QName(NAME1));
      wtx.insertAttribute(new QName(NAME1), NAME2);
      wtx.moveToParent();
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.insertAttribute(new QName(NAME2), NAME1);

      wtx.moveTo(rootKey);
      wtx.moveToFirstChild();
      wtx.remove();
      wtx.remove();

      wtx.moveTo(rootKey);
      assertEquals(firstRootHash, wtx.getNode().getHash());
  }

  @Ignore
  private void testDeepTree(final INodeWriteTrx wtx) throws AbsTTException {

      wtx.insertElementAsFirstChild(new QName(NAME1));
      final long oldHash = wtx.getNode().getHash();

      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.insertElementAsFirstChild(new QName(NAME2));
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.insertElementAsFirstChild(new QName(NAME2));
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.remove();
      wtx.insertElementAsFirstChild(new QName(NAME2));
      wtx.insertElementAsFirstChild(new QName(NAME2));
      wtx.insertElementAsFirstChild(new QName(NAME1));

      wtx.moveTo(1);
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.remove();
      assertEquals(oldHash, wtx.getNode().getHash());
  }

  @Ignore
  private void testSetter(final INodeWriteTrx wtx) throws AbsTTException {

      // Testing node inheritance
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.insertElementAsFirstChild(new QName(NAME1));
      wtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      final long hashRoot1 = wtx.getNode().getHash();
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      final long hashLeaf1 = wtx.getNode().getHash();
      wtx.setQName(new QName(NAME2));
      final long hashLeaf2 = wtx.getNode().getHash();
      wtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      final long hashRoot2 = wtx.getNode().getHash();
      assertFalse(hashRoot1 == hashRoot2);
      assertFalse(hashLeaf1 == hashLeaf2);
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.setQName(new QName(NAME1));
      final long hashLeaf3 = wtx.getNode().getHash();
      assertEquals(hashLeaf1, hashLeaf3);
      wtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      final long hashRoot3 = wtx.getNode().getHash();
      assertEquals(hashRoot1, hashRoot3);

      // Testing root inheritance
      wtx.moveTo(EFixed.DOCUMENT_NODE_KEY.getStandardProperty());
      wtx.moveTo(((IStructNode)wtx.getNode()).getFirstChildKey());
      wtx.setQName(new QName(NAME2));
      final long hashRoot4 = wtx.getNode().getHash();
      assertFalse(hashRoot4 == hashRoot2);
      assertFalse(hashRoot4 == hashRoot1);
      assertFalse(hashRoot4 == hashRoot3);
      assertFalse(hashRoot4 == hashLeaf1);
      assertFalse(hashRoot4 == hashLeaf2);
      assertFalse(hashRoot4 == hashLeaf3);
  }

  private INodeWriteTrx createWtx(final EHashKind kind) throws AbsTTException {
      final IDatabase database = TestHelper.getDatabase(TestHelper.PATHS.PATH1.getFile());
      database.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH1
          .getConfig()).build());
      final ISession session =
          database.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build());
      final INodeWriteTrx wTrx = session.beginNodeWriteTrx();
      return wTrx;
  }

  @After
  public void tearDown() throws AbsTTException {
      TestHelper.closeEverything();
  }

}
