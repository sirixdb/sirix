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

package org.sirix.api;

import java.io.IOException;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Movement;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.service.xml.shredder.XMLShredder;
import com.google.common.annotations.Beta;

/**
 * <h1>NodeWriteTrx</h1>
 *
 * <h2>Description</h2>
 *
 * <p>
 * Interface to access nodes based on the Key/ParentKey/FirstChildKey/LeftSiblingKey
 * /RightSiblingKey/ChildCount/DescendantCount encoding. This encoding keeps the children ordered
 * but has no knowledge of the global node ordering. The underlying tree is accessed in a
 * cursor-like fashion.
 * </p>
 *
 * <h2>Convention</h2>
 *
 * <p>
 * <ol>
 * <li>Only a single thread accesses the single INodeWriteTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>NodeWriteTrx.getKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after modifying the cursor:
 * <code>(NodeWriteTrx.insertX() == m &&
 *       NodeWriteTrx.getKey() == m)</code>.</li>
 * </ol>
 * </p>
 *
 * <h2>User Example</h2>
 *
 * <p>
 *
 * <pre>
 * // Without auto commit.
 * try (final NodeWriteTrx wtx = resManager.beginNodeWriteTrx()) {
 *   wtx.insertElementAsFirstChild(&quot;foo&quot;);
 *   wtx.commit();
 * }
 *
 * // With auto commit after every 10th modification.
 * try (final NodeWriteTrx wtx = resManager.beginNodeWriteTrx(10,
 * 		TimeUnit.MINUTES, 0)) {
 *   wtx.insertElementAsFirstChild(new QNm(&quot;foo&quot;));
 *   // 9 other modifications.
 *   // Auto commit.
 * }
 *
 * // With auto commit after every minute.
 * try (final NodeWriteTrx wtx = resManager.beginNodeWriteTrx(0,
 * 		TimeUnit.MINUTES, 1)) {
 *   wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 *   ...
 *   // Then rollback (transaction is also rolled back if this method wouldn't be called during the auto close.
 *   wtx.rollback();
 * }
 *
 * // With auto commit after every 10th modification and every second.
 * try (final NodeWriteTrx wtx = resManager.beginNodeWriteTrx(10,
 * 		TimeUnit.SECONDS, 1)) {
 *   wtx.insertElementAsFirstChild(new QNm(&quot;foo&quot;));
 *   ...
 *   // Implicit rollback during implicit wtx.close()-call.
 * }
 * </pre>
 *
 * </p>
 *
 * <h2>Developer Example</h2>
 *
 * <p>
 *
 * <pre>
 *   public final void someNodeWriteTrxMethod() {
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
public interface XdmNodeWriteTrx extends XdmNodeReadTrx {

  // --- Node Modifiers
  // --------------------------------------------------------

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the
   * provided transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XdmNodeReadTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullpointerException if {@code rtx} is {@code null}
   */
  XdmNodeWriteTrx copySubtreeAsFirstChild(XdmNodeReadTrx rtx) throws SirixException;

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the
   * provided transaction) and insert as left sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XdmNodeReadTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullpointerException if {@code pRtx} is {@code null}
   */
  XdmNodeWriteTrx copySubtreeAsLeftSibling(XdmNodeReadTrx rtx) throws SirixException;

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the
   * provided transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XdmNodeReadTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullpointerException if {@code pRtx} is {@code null}
   */
  XdmNodeWriteTrx copySubtreeAsRightSibling(XdmNodeReadTrx rtx) throws SirixException;

  /**
   * Replace a node with another node or subtree, depending on whether the replaced node is an
   * {@code element}- or a {@code text-}node.
   *
   * @param xml an XML representation
   * @return the transaction instance
   * @throws IOException if an I/O error occured
   * @throws XMLStreamException if {@code pXML} is not well formed
   * @throws NullpointerException if {@code pXML} is {@code null}
   * @throws SirixException if anything in Sirix fails
   */
  XdmNodeWriteTrx replaceNode(String xml)
      throws SirixException, IOException, XMLStreamException;

  /**
   * Replace a node with another node or subtree (the subtree rooted at the provided transaction),
   * depending on whether the replaced node is an {@code element}- or a {@code text-}node.
   *
   * @param pNode a node from another resource
   * @return the transaction instance
   * @throws SirixException if anything went wrong
   */
  XdmNodeWriteTrx replaceNode(XdmNodeReadTrx rtx) throws SirixException;

  /**
   * Move a subtree rooted at {@code fromKey} to the first child of the current node.
   *
   * @param fromKey root node key of the subtree to move
   * @return the transaction instance
   * @throws SirixException if move adaption fails
   * @throws IllegalArgumentException if {@code fromKey < 0}, {@code fromKey > maxNodeKey} or
   *         {@code fromKey == currentNodeKey}
   * @throws NullPointerException if {@code nodeToMove} does not exist, that is the node which is
   *         denoted by it's node key {@code fromKey}
   */
  XdmNodeWriteTrx moveSubtreeToFirstChild(@Nonnegative long fromKey) throws SirixException;

  /**
   * Move a subtree rooted at {@code fromKey} to the right sibling of the current node. In case of
   * the moved node is a text-node the value of the current node is prepended to the moved node and
   * deleted afterwards. In this case the transaction is moved to the moved node.
   *
   * @param fromKey root node key of the subtree to move
   * @return the transaction instance
   * @throws SirixException if move adaption fails
   * @throws IllegalArgumentException if {@code fromKey < 0}, {@code fromKey > maxNodeKey} or
   *         {@code fromKey == currentNodeKey}
   * @throws NullPointerException if {@code nodeToMove} does not exist, that is the node which is
   *         denoted by it's node key {@code fromKey}
   */
  XdmNodeWriteTrx moveSubtreeToRightSibling(long fromKey) throws SirixException;

  /**
   * Move a subtree rooted at {@code fromKey} to the left sibling of the current node. In case of
   * the moved node is a text-node the value of the current node is prepended to the moved node and
   * deleted afterwards. In this case the transaction is moved to the moved node.
   *
   * @param fromKey root node key of the subtree to move
   * @return the transaction instance
   * @throws SirixException if move adaption fails
   * @throws IllegalArgumentException if {@code fromKey < 0}, {@code fromKey > maxNodeKey} or
   *         {@code fromKey == currentNodeKey}
   * @throws NullPointerException if {@code nodeToMove} does not exist, that is the node which is
   *         denoted by it's node key {@code fromKey}
   */
  XdmNodeWriteTrx moveSubtreeToLeftSibling(@Nonnegative long fromKey) throws SirixException;

  /**
   * Insert new comment node as left sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertCommentAsLeftSibling(String value) throws SirixException;

  /**
   * Insert new comment node as right sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertCommentAsRightSibling(String value) throws SirixException;

  /**
   * Insert new comment node as first child of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertCommentAsFirstChild(String value) throws SirixException;

  /**
   * Insert new Processing Instruction node as left sibling of currently selected node. The cursor
   * is moved to the inserted node.
   *
   * @param content content of processing instruction
   * @param target target of processing instruction
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code content} or {@code target} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertPIAsLeftSibling(String content, @Nonnull String target)
      throws SirixException;

  /**
   * Insert new Processing Instruction node as right sibling of currently selected node. The cursor
   * is moved to the inserted node.
   *
   * @param content content of processing instruction
   * @param target target of processing instruction
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code content} or {@code target} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertPIAsRightSibling(String content, @Nonnull String target)
      throws SirixException;

  /**
   * Insert new Processing Instruction node as first child of currently selected node. The cursor is
   * moved to the inserted node.
   *
   * @param content content of processing instruction
   * @param target target of processing instruction
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code content} or {@code target} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertPIAsFirstChild(String content, @Nonnull String target)
      throws SirixException;

  /**
   * Insert new element node as first child of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param name {@link QNm} of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertElementAsFirstChild(QNm name) throws SirixException;

  /**
   * Insert new element node as left sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param pName {@link QNm} of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertElementAsLeftSibling(QNm name) throws SirixException;

  /**
   * Insert new element node as right sibling of currently selected node. The transaction is moved
   * to the inserted node.
   *
   * @param name {@link QNm} of the new node
   * @throws SirixException if element node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertElementAsRightSibling(QNm name) throws SirixException;

  /**
   * Insert new text node as first child of currently selected node. The cursor is moved to the
   * inserted node. If the result would be two adjacent {@link TextNode}s the value is appended with
   * a single whitespace character prepended at first.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertTextAsFirstChild(String value) throws SirixException;

  /**
   * Insert new text node as left sibling of currently selected node. The transaction is moved to
   * the inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertTextAsLeftSibling(String value) throws SirixException;

  /**
   * Insert new text node as right sibling of currently selected node. The transaction is moved to
   * the inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertTextAsRightSibling(String value) throws SirixException;

  /**
   * Insert attribute in currently selected node. The cursor is moved to the inserted node.
   *
   * @param name {@link QNm} reference
   * @param value value of inserted node
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} or {@code value} is null
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertAttribute(QNm name, @Nonnull String value) throws SirixException;

  /**
   * Insert attribute in currently selected node. The cursor is moved depending on the value of
   * {@code pMove}.
   *
   * @param name {@link QNm} reference
   * @param value value of inserted node
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} or {@code value} is null
   * @return the transaction instance
   */
  XdmNodeWriteTrx insertAttribute(QNm name, @Nonnull String value, @Nonnull Movement move)
      throws SirixException;

  /**
   * Insert namespace declaration in currently selected node. The cursor is moved to the inserted
   * node.
   *
   * @param name {@link QNm} reference
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} is null
   * @return the current transaction
   */
  XdmNodeWriteTrx insertNamespace(QNm name) throws SirixException;

  /**
   * Insert namespace declaration in currently selected node. The cursor is moved depending on the
   * value of {@code pMove}.
   *
   * @param pName {@link QNm} reference
   * @return the current transaction
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} or {@code move} is null
   */
  XdmNodeWriteTrx insertNamespace(QNm name, @Nonnull Movement move) throws SirixException;

  /**
   * Insert a subtree as a first child.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XMLShredder#createStringReader(String)},
   *        {@link XMLShredder#createFileReader(java.io.File)} or
   *        {@link XMLShredder#createQueueReader(java.util.Queue)
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XdmNodeWriteTrx insertSubtreeAsFirstChild(XMLEventReader reader) throws SirixException;

  /**
   * Insert a subtree as a right sibling.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XMLShredder#createStringReader(String)},
   *        {@link XMLShredder#createFileReader(java.io.File)} or
   *        {@link XMLShredder#createQueueReader(java.util.Queue)
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XdmNodeWriteTrx insertSubtreeAsRightSibling(XMLEventReader reader) throws SirixException;

  /**
   * Insert a subtree as a left sibling.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XMLShredder#createStringReader(String)},
   *        {@link XMLShredder#createFileReader(java.io.File)} or
   *        {@link XMLShredder#createQueueReader(java.util.Queue)
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XdmNodeWriteTrx insertSubtreeAsLeftSibling(XMLEventReader reader) throws SirixException;

  /**
   * Remove currently selected node. This does automatically remove descendants. If two adjacent
   * {@link TextNode}s would be the result after the remove, the value of the former right sibling
   * is appended to the left sibling {@link TextNode} and removed afterwards.
   *
   * The cursor is located at the former right sibling. If there was no right sibling, it is located
   * at the former left sibling. If there was no left sibling, it is located at the former parent.
   *
   * @return the current transaction
   * @throws SirixException if node couldn't be removed
   */
  XdmNodeWriteTrx remove() throws SirixException;

  // --- Node Setters
  // -----------------------------------------------------------

  /**
   * Set QName of node.
   *
   * @param name new qualified name of node
   * @return the current transaction
   * @throws SirixIOException if can't set Name in node
   * @throws NullPointerException if {@code pName} is {@code null}
   */
  XdmNodeWriteTrx setName(QNm name) throws SirixException;

  /**
   * Set value of node.
   *
   * @param value new value of node
   * @return the current transaction
   * @throws SirixIOException if value couldn't be set
   * @throws NullPointerException if {@code value} is {@code null}
   */
  XdmNodeWriteTrx setValue(String value) throws SirixException;

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all.
   *
   * @throws SirixException if this revision couldn't be commited
   */
  XdmNodeWriteTrx commit();

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all. The author assignes a commit message.
   *
   * @param commitMessage message of the commit
   * @throws SirixException if this revision couldn't be commited
   */
  XdmNodeWriteTrx commit(String commitMessage);

  /**
   * Rollback all modifications of the exclusive write transaction.
   *
   * @throws SirixException if the changes in this revision couldn't be rollbacked
   */
  XdmNodeWriteTrx rollback();

  /**
   * Reverting all changes to the revision defined. This command has to be finalized with a commit.
   * A revert is always bound to a {@link XdmNodeReadTrx#moveToDocumentRoot()}.
   *
   * @param revision revert to the revision
   */
  XdmNodeWriteTrx revertTo(@Nonnegative int revision);

  /**
   * Closing current WriteTransaction.
   *
   * @throws SirixIOException if write transaction couldn't be closed
   */
  @Override
  void close();

  /**
   * Add pre commit hook.
   *
   * @param hook pre commit hook
   */
  XdmNodeWriteTrx addPreCommitHook(PreCommitHook hook);

  /**
   * Add a post commit hook.
   *
   * @param hook post commit hook
   */
  XdmNodeWriteTrx addPostCommitHook(PostCommitHook hook);

  /**
   * Get the {@link PathSummaryReader} associated with the current write transaction -- might be
   * {@code null} if no path summary index is used.
   *
   * @return {@link PathSummaryReader} instance
   */
  PathSummaryReader getPathSummary();

  /**
   * Get the page transaction used within the write transaction.
   *
   * @return the {@link PageWriteTrx} instance
   */
  @Beta
  PageWriteTrx<Long, Record, UnorderedKeyValuePage> getPageTransaction();

  XdmNodeWriteTrx truncateTo(int revision);
}
