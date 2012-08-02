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

package org.sirix.api;

import java.io.IOException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;

import org.sirix.access.EMove;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.exception.TTUsageException;
import org.sirix.index.path.PathSummary;
import org.sirix.index.value.AVLTree;
import org.sirix.node.TextNode;
import org.sirix.node.TextReferences;
import org.sirix.node.TextValue;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * <h1>IWriteTransaction</h1>
 * 
 * <h2>Description</h2>
 * 
 * <p>
 * Interface to access nodes based on the
 * Key/ParentKey/FirstChildKey/LeftSiblingKey/RightSiblingKey/ChildCount/DescendantCount encoding. This
 * encoding keeps the children ordered but has no knowledge of the global node ordering. The underlying tree
 * is accessed in a cursor-like fashion.
 * </p>
 * 
 * <p>
 * Each commit at least adds <code>10kB</code> to the sirix file. It is thus recommended to work with the auto
 * commit mode only committing after a given amount of node modifications or elapsed time. For very
 * update-intensive data, a value of one million modifications and ten seconds is recommended. Note that this
 * might require to increment the available heap.
 * </p>
 * 
 * <h2>Convention</h2>
 * 
 * <p>
 * <ol>
 * <li>Only a single thread accesses the single INodeWriteTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor: <code>INodeWriteTransaction.getKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after modifying the cursor: <code>(IWriteTransaction.insertX() == m &&
 *       IWriteTransaction.getKey() == m)</code>.</li>
 * </ol>
 * </p>
 * 
 * <h2>User Example</h2>
 * 
 * <p>
 * 
 * <pre>
 * // Without auto commit.
 * final INodeWriteTransaction wtx = session.beginNodeWriteTrx();
 * wtx.insertElementAsFirstChild(&quot;foo&quot;);
 * // Explicit forced commit.
 * wtx.commit();
 * wtx.close();
 * 
 * // With auto commit after every 10th modification.
 * final INodeWriteTransaction wtx =
 *   session.beginNodeWriteTrx(10, TimeUnit.MINUTES, 0);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * // Implicit commit.
 * wtx.close();
 * 
 * // With auto commit after every minute.
 * final INodeWriteTransaction wtx =
 *   session.beginNodeWriteTrx(0, TimeUnit.MINUTES, 1);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * // Implicit commit.
 * wtx.close();
 * 
 * // With auto commit after every 10th modification and every second.
 * final INodeWriteTransaction wtx =
 *   session.beginNodeWriteTrx(10, TimeUnit.SECONDS, 1);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * // Implicit commit.
 * wtx.close();
 * </pre>
 * 
 * </p>
 * 
 * <h2>Developer Example</h2>
 * 
 * <p>
 * 
 * <pre>
 *   public final void someIWriteTransactionMethod() {
 *     // This must be called to make sure the transaction is not closed.
 *     assertNotClosed();
 *     // This must be called to track the modifications.
 *     mModificationCount++;
 *     ...
 *   }
 * </pre>
 * 
 * </p>
 */
public interface INodeWriteTrx extends INodeReadTrx {

  // --- Node Modifiers
  // --------------------------------------------------------

  /**
   * Copy subtree from another {@code database/resource/revision} and insert as right sibling of the current
   * node.
   * 
   * @param pRtx
   *          read transaction reference which implements the {@link INodeReadTrx} interface
   * @return the transaction instance
   * @throws AbsTTException
   *           if anything in sirix fails
   * @throws NullpointerException
   *           if {@code pRtx} is {@code null}
   */
  INodeWriteTrx copySubtreeAsFirstChild(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException;

  /**
   * Copy subtree from another {@code database/resource/revision} and insert as left sibling of the current
   * node.
   * 
   * @param pRtx
   *          read transaction reference which implements the {@link INodeReadTrx} interface
   * @return the transaction instance
   * @throws AbsTTException
   *           if anything in sirix fails
   * @throws NullpointerException
   *           if {@code pRtx} is {@code null}
   */
  INodeWriteTrx copySubtreeAsLeftSibling(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException;

  /**
   * Copy subtree from another {@code database/resource/revision} and insert as right sibling of the current
   * node.
   * 
   * @param pRtx
   *          read transaction reference which implements the {@link INodeReadTrx} interface
   * @return the transaction instance
   * @throws AbsTTException
   *           if anything in sirix fails
   * @throws NullpointerException
   *           if {@code pRtx} is {@code null}
   */
  INodeWriteTrx copySubtreeAsRightSibling(@Nonnull INodeReadTrx pRtx)
    throws AbsTTException;

  /**
   * Replace a node with another node or subtree, depending whether the replaced node is an {@code element}-
   * or a {@code text-}node.
   * 
   * @param pXML
   *          an XML representation
   * @return the transaction instance
   * @throws IOException
   *           if an I/O error occured
   * @throws XMLStreamException
   *           if {@code pXML} is not well formed
   * @throws NullpointerException
   *           if {@code pXML} is {@code null}
   * @throws AbsTTException
   *           if anything in Sirix fails
   */
  INodeWriteTrx replaceNode(@Nonnull String pXML) throws AbsTTException,
    IOException, XMLStreamException;

  /**
   * Replace a node with another node or subtree, depending whether the replaced node is an {@code element}-
   * or a {@code text-}node.
   * 
   * @param pNode
   *          a node from another resource
   * @return the transaction instance
   * @throws AbsTTException
   *           if anything went wrong
   */
  INodeWriteTrx replaceNode(@Nonnull INodeReadTrx pRtx) throws AbsTTException;

  /**
   * Move a subtree rooted at {@code pToKey} to the first child of the current node.
   * 
   * @param pFromKey
   *          root node key of the subtree to move
   * @return the transaction instance
   * @throws AbsTTException
   *           if move adaption fails
   * @throws IllegalArgumentException
   *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or {@code pFromKey == currentNodeKey}
   * @throws NullPointerException
   *           if {@code nodeToMove} does not exist, that is the node which is denoted by it's node key
   *           {@code pFromKey}
   */
  INodeWriteTrx moveSubtreeToFirstChild(@Nonnegative long pFromKey)
    throws AbsTTException;

  /**
   * Move a subtree rooted at {@code pFromKey} to the right sibling of the current node. In case of the moved
   * node is a text-node the value of the current node is prepended to the moved node and deleted afterwards.
   * In this case the transaction is moved to the moved node.
   * 
   * @param pFromKey
   *          root node key of the subtree to move
   * @return the transaction instance
   * @throws AbsTTException
   *           if move adaption fails
   * @throws IllegalArgumentException
   *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or {@code pFromKey == currentNodeKey}
   * @throws NullPointerException
   *           if {@code nodeToMove} does not exist, that is the node which is denoted by it's node key
   *           {@code pFromKey}
   */
  INodeWriteTrx moveSubtreeToRightSibling(long pFromKey) throws AbsTTException;

  /**
   * Move a subtree rooted at {@code pFromKey} to the left sibling of the current node. In case of the moved
   * node is a text-node the value of the current node is prepended to the moved node and deleted afterwards.
   * In this case the transaction is moved to the moved node.
   * 
   * @param pFromKey
   *          root node key of the subtree to move
   * @return the transaction instance
   * @throws AbsTTException
   *           if move adaption fails
   * @throws IllegalArgumentException
   *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or {@code pFromKey == currentNodeKey}
   * @throws NullPointerException
   *           if {@code nodeToMove} does not exist, that is the node which is denoted by it's node key
   *           {@code pFromKey}
   */
  INodeWriteTrx moveSubtreeToLeftSibling(@Nonnegative long pFromKey)
    throws AbsTTException;

  /**
   * Insert new element node as first child of currently selected node. The
   * cursor is moved to the inserted node.
   * 
   * @param pName
   *          {@link QName} of node to insert
   * @throws AbsTTException
   *           if element node couldn't be inserted as first child
   * @throws NullPointerException
   *           if {@code pQName} is {@code null}
   * @return the transaction instance
   */
  INodeWriteTrx insertElementAsFirstChild(@Nonnull QName pName)
    throws AbsTTException;

  /**
   * Insert new element node as left sibling of currently selected node. The
   * cursor is moved to the inserted node.
   * 
   * @param pName
   *          {@link QName} of node to insert
   * @throws AbsTTException
   *           if element node couldn't be inserted as first child
   * @throws NullPointerException
   *           if {@code pQName} is {@code null}
   * @return the transaction instance
   */
  INodeWriteTrx insertElementAsLeftSibling(@Nonnull QName pQName)
    throws AbsTTException;

  /**
   * Insert new element node as right sibling of currently selected node. The
   * transaction is moved to the inserted node.
   * 
   * @param pQName
   *          {@link QName} of the new node
   * @throws AbsTTException
   *           if element node couldn't be inserted as right sibling
   * @return the transaction instance
   */
  INodeWriteTrx insertElementAsRightSibling(@Nonnull QName pQName)
    throws AbsTTException;

  /**
   * Insert new text node as first child of currently selected node. The
   * cursor is moved to the inserted node. If the result would be two
   * adjacent {@link TextNode}s the value is appended with a single whitespace
   * character prepended at first.
   * 
   * @param pValue
   *          value of node to insert
   * @throws AbsTTException
   *           if text node couldn't be inserted as first child
   * @throws NullPointerException
   *           if {@code pValue} is {@code null}
   * @return the transaction instance
   */
  INodeWriteTrx insertTextAsFirstChild(@Nonnull String pValue)
    throws AbsTTException;

  /**
   * Insert new text node as left sibling of currently selected node. The
   * transaction is moved to the inserted node.
   * 
   * @param pValue
   *          value of node to insert
   * @throws AbsTTException
   *           if text node couldn't be inserted as right sibling
   * @throws NullPointerException
   *           if {@code pValue} is {@code null}
   * @return the transaction instance
   */
  INodeWriteTrx insertTextAsLeftSibling(@Nonnull String pValue)
    throws AbsTTException;

  /**
   * Insert new text node as right sibling of currently selected node. The
   * transaction is moved to the inserted node.
   * 
   * @param pValue
   *          value of node to insert
   * @throws AbsTTException
   *           if text node couldn't be inserted as right sibling
   * @throws NullPointerException
   *           if {@code pValue} is {@code null}
   * @return the transaction instance
   */
  INodeWriteTrx insertTextAsRightSibling(@Nonnull String pValue)
    throws AbsTTException;

  /**
   * Insert attribute in currently selected node. The cursor is moved to the
   * inserted node.
   * 
   * @param pName
   *          {@link QName} reference
   * @param pValue
   *          value of inserted node
   * @throws AbsTTException
   *           if attribute couldn't be inserted.
   * @return the transaction instance
   */
  INodeWriteTrx insertAttribute(@Nonnull QName pName, @Nonnull String pValue)
    throws AbsTTException;

  /**
   * Insert attribute in currently selected node. The cursor is moved depending on the value of {@code pMove}.
   * 
   * @param pName
   *          {@link QName} reference
   * @param pValue
   *          value of inserted node
   * @throws AbsTTException
   *           if attribute couldn't be inserted.
   * @return the transaction instance
   */
  INodeWriteTrx insertAttribute(@Nonnull QName pName, @Nonnull String pValue,
    @Nonnull EMove pMove) throws AbsTTException;

  /**
   * Insert namespace declaration in currently selected node. The cursor is
   * moved to the inserted node.
   * 
   * @param pName
   *          {@link QName} reference
   * @throws AbsTTException
   *           if attribute couldn't be inserted.
   * @return the current transaction
   */
  INodeWriteTrx insertNamespace(@Nonnull QName pName) throws AbsTTException;

  /**
   * Insert namespace declaration in currently selected node. The cursor is moved depending on the value of
   * {@code pMove}.
   * 
   * @param pName
   *          {@link QName} reference
   * @return the current transaction
   * @throws AbsTTException
   *           if attribute couldn't be inserted.
   */
  INodeWriteTrx insertNamespace(@Nonnull QName pQName, @Nonnull EMove pMove)
    throws AbsTTException;

/**
   * Insert a subtree.
   * 
   * @param pReader
   *            {@link XMLEventReader} instance maybe derived from {@link XMLShredder#createStringReader(String)}, {@link XMLShredder#createFileReader(java.io.File)} or {@link XMLShredder#createQueueReader(java.util.Queue).
   * @param pInsert
   *            insert position
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws AbsTTException
   *          if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException
   *          if subtree is inserted as right sibling of a root-node or document-node
   * @throws NullPointerException
   *          if {@code pReader} or {@code pInsert} is {@code null}
   */
  INodeWriteTrx insertSubtree(@Nonnull XMLEventReader pReader,
    @Nonnull EInsert pInsert) throws AbsTTException;

  /**
   * Remove currently selected node. This does automatically remove
   * descendants. If two adjacent {@link TextNode}s would be the result after
   * the remove, the value of the former right sibling is appended to the left sibling {@link TextNode} and
   * removed afterwards.
   * 
   * The cursor is located at the former right sibling. If there was no right
   * sibling, it is located at the former left sibling. If there was no left
   * sibling, it is located at the former parent.
   * 
   * @throws AbsTTException
   *           if node couldn't be removed
   */
  void remove() throws AbsTTException;

  // --- Node Setters
  // -----------------------------------------------------------

  /**
   * Set QName of node.
   * 
   * @param pName
   *          new qualified name of node
   * @throws TTIOException
   *           if can't set Name in node
   * @throws NullPointerException
   *           if {@code pName} is {@code null}
   */
  void setQName(@Nonnull QName pName) throws AbsTTException;

  // /**
  // * Set URI of node.
  // *
  // * @param pUri
  // * new URI of node
  // * @throws TTIOException
  // * if URI of node couldn't be set
  // * @throws NullPointerException
  // * if {@code pUri} is {@code null}
  // */
  // void setURI(@Nonnull String pUri) throws AbsTTException;

  /**
   * Set value of node.
   * 
   * @param pValue
   *          new value of node
   * @throws TTIOException
   *           if value couldn't be set
   * @throws NullPointerException
   *           if {@code pUri} is {@code null}
   */
  void setValue(@Nonnull String pValue) throws AbsTTException;

  /**
   * Commit all modifications of the exclusive write transaction. Even commit
   * if there are no modification at all.
   * 
   * @throws AbsTTException
   *           if this revision couldn't be commited
   */
  void commit() throws AbsTTException;

  /**
   * Abort all modifications of the exclusive write transaction.
   * 
   * @throws TTIOException
   *           if this revision couldn't be aborted
   */
  void abort() throws TTIOException;

  /**
   * Reverting all changes to the revision defined. This command has to be
   * finalized with a commit. A revert is always bound to a {@link INodeReadTrx#moveToDocumentRoot()}.
   * 
   * @param pRev
   *          revert to the revision
   * @throws TTUsageException
   *           if {@code pRevision < 0} or {@code pRevision > maxCommitedRev}
   * @throws TTIOException
   *           if an I/O operation fails
   */
  void revertTo(@Nonnegative long pRev) throws AbsTTException;

  /**
   * Closing current WriteTransaction.
   * 
   * @throws TTIOException
   *           if write transaction couldn't be closed
   */
  @Override
  void close() throws AbsTTException;

  /**
   * Add pre commit hook.
   * 
   * @param pHook
   *          pre commit hook
   */
  void addPreCommitHook(@Nonnull IPreCommitHook pHook);

  /**
   * Add a post commit hook.
   * 
   * @param pHook
   *          post commit hook
   */
  void addPostCommitHook(@Nonnull IPostCommitHook pHook);

  /**
   * Get the {@link PathSummary} associated with the current write transaction.
   * 
   * @return {@link PathSummary} instance
   */
  PathSummary getPathSummary();

  /**
   * Get the {@link AVLTree} associated with the current write transaction.
   * 
   * @return {@link AVLTree} instance
   */
  AVLTree<TextValue, TextReferences> getAVLTree();
}
