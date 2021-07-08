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

package org.sirix.api.xml;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.Movement;
import org.sirix.api.NodeTrx;
import org.sirix.api.PostCommitHook;
import org.sirix.api.PreCommitHook;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.xml.TextNode;
import org.sirix.service.xml.shredder.XmlShredder;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.stream.XMLEventReader;

/**
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
 * <ol>
 * <li>Only a single thread accesses the single INodeWriteTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>NodeWriteTrx.getNodeKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after modifying the cursor:
 * <code>(NodeWriteTrx.insertX() == m &amp;&amp;
 *       NodeWriteTrx.getNodeKey() == m)</code>.</li>
 * </ol>
 *
 * <h2>User Example</h2>
 *
 *
 * <pre>
 * // Without auto commit.
 * try (final XmlNodeTrx wtx = resManager.beginNodeWriteTrx()) {
 *   wtx.insertElementAsFirstChild(&quot;foo&quot;);
 *   wtx.commit();
 * }
 *
 * // With auto commit after every 10th modification.
 * try (final XmlNodeTrx wtx = resManager.beginNodeWriteTrx(10,
 * 		TimeUnit.MINUTES, 0)) {
 *   wtx.insertElementAsFirstChild(new QNm(&quot;foo&quot;));
 *   // 9 other modifications.
 *   // Auto commit.
 * }
 *
 * // With auto commit after every minute.
 * try (final XmlNodeTrx wtx = resManager.beginNodeWriteTrx(0,
 * 		TimeUnit.MINUTES, 1)) {
 *   wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 *   ...
 *   // Then rollback (transaction is also rolled back if this method wouldn't be called during the auto close.
 *   wtx.rollback();
 * }
 *
 * // With auto commit after every 10th modification and every second.
 * try (final XmlNodeTrx wtx = resManager.beginNodeWriteTrx(10,
 * 		TimeUnit.SECONDS, 1)) {
 *   wtx.insertElementAsFirstChild(new QNm(&quot;foo&quot;));
 *   ...
 *   // Implicit rollback during implicit wtx.close()-call.
 * }
 * </pre>
 *
 *
 * <h2>Developer Example</h2>
 *
 *
 * <pre>
 *   public void someNodeWriteTrxMethod() {
 *     // This must be called to make sure the transaction is not closed.
 *     assertNotClosed();
 *     // This must be called to track the modifications.
 *     modificationCount++;
 *     ...
 *   }
 * </pre>
 *
 */
public interface XmlNodeTrx extends XmlNodeReadOnlyTrx, NodeTrx {

  // --- Node Modifiers
  // --------------------------------------------------------

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XmlNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  XmlNodeTrx copySubtreeAsFirstChild(XmlNodeReadOnlyTrx rtx);

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as left sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XmlNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  XmlNodeTrx copySubtreeAsLeftSibling(XmlNodeReadOnlyTrx rtx);

  /**
   * Copy subtree from another {@code database/resource/revision} (the subtree rooted at the provided
   * transaction) and insert as right sibling of the current node.
   *
   * @param rtx read transaction reference which implements the {@link XmlNodeReadOnlyTrx} interface
   * @return the transaction instance
   * @throws SirixException if anything in sirix fails
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  XmlNodeTrx copySubtreeAsRightSibling(XmlNodeReadOnlyTrx rtx);

  /**
   * Replace a node with another node or subtree, depending on whether the replaced node is an
   * {@code element}- or a {@code text-}node.
   *
   * @param reader an XML reader
   * @return the transaction instance
   * @throws NullPointerException if {@code reader} is {@code null}
   * @throws SirixException if anything in Sirix fails
   */
  XmlNodeTrx replaceNode(XMLEventReader reader);

  /**
   * Replace a node with another node or subtree (the subtree rooted at the provided transaction),
   * depending on whether the replaced node is an {@code element}- or a {@code text-}node.
   *
   * @param rtx a read-only transaction, used to read from
   * @return the transaction instance
   * @throws SirixException if anything went wrong
   */
  XmlNodeTrx replaceNode(XmlNodeReadOnlyTrx rtx);

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
  XmlNodeTrx moveSubtreeToFirstChild(@Nonnegative long fromKey);

  /**
   * Move a subtree rooted at {@code fromKey} to the right sibling of the current node. In case of the
   * moved node is a text-node the value of the current node is prepended to the moved node and
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
  XmlNodeTrx moveSubtreeToRightSibling(long fromKey);

  /**
   * Move a subtree rooted at {@code fromKey} to the left sibling of the current node. In case of the
   * moved node is a text-node the value of the current node is prepended to the moved node and
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
  XmlNodeTrx moveSubtreeToLeftSibling(@Nonnegative long fromKey);

  /**
   * Insert new comment node as left sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertCommentAsLeftSibling(String value);

  /**
   * Insert new comment node as right sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertCommentAsRightSibling(String value);

  /**
   * Insert new comment node as first child of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertCommentAsFirstChild(String value);

  /**
   * Insert new Processing Instruction node as left sibling of currently selected node. The cursor is
   * moved to the inserted node.
   *
   * @param content content of processing instruction
   * @param target target of processing instruction
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code content} or {@code target} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertPIAsLeftSibling(String content, @Nonnull String target);

  /**
   * Insert new Processing Instruction node as right sibling of currently selected node. The cursor is
   * moved to the inserted node.
   *
   * @param content content of processing instruction
   * @param target target of processing instruction
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code content} or {@code target} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertPIAsRightSibling(String content, @Nonnull String target);

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
  XmlNodeTrx insertPIAsFirstChild(String content, @Nonnull String target);

  /**
   * Insert new element node as first child of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param name {@link QNm} of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertElementAsFirstChild(QNm name);

  /**
   * Insert new element node as left sibling of currently selected node. The cursor is moved to the
   * inserted node.
   *
   * @param name {@link QNm} of node to insert
   * @throws SirixException if element node couldn't be inserted as first child
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertElementAsLeftSibling(QNm name);

  /**
   * Insert new element node as right sibling of currently selected node. The transaction is moved to
   * the inserted node.
   *
   * @param name {@link QNm} of the new node
   * @throws SirixException if element node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code name} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertElementAsRightSibling(QNm name);

  /**
   * Insert new text node as first child of currently selected node. The cursor is moved to the
   * inserted node. If the result would be two adjacent {@link TextNode}s the value is appended with a
   * single whitespace character prepended at first.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as first child
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertTextAsFirstChild(String value);

  /**
   * Insert new text node as left sibling of currently selected node. The transaction is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertTextAsLeftSibling(String value);

  /**
   * Insert new text node as right sibling of currently selected node. The transaction is moved to the
   * inserted node.
   *
   * @param value value of node to insert
   * @throws SirixException if text node couldn't be inserted as right sibling
   * @throws NullPointerException if {@code value} is {@code null}
   * @return the transaction instance
   */
  XmlNodeTrx insertTextAsRightSibling(String value);

  /**
   * Insert attribute in currently selected node. The cursor is moved to the inserted node.
   *
   * @param name {@link QNm} reference
   * @param value value of inserted node
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} or {@code value} is null
   * @return the transaction instance
   */
  XmlNodeTrx insertAttribute(QNm name, @Nonnull String value);

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
  XmlNodeTrx insertAttribute(QNm name, @Nonnull String value, @Nonnull Movement move);

  /**
   * Insert namespace declaration in currently selected node. The cursor is moved to the inserted
   * node.
   *
   * @param name {@link QNm} reference
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} is null
   * @return the current transaction
   */
  XmlNodeTrx insertNamespace(QNm name);

  /**
   * Insert namespace declaration in currently selected node. The cursor is moved depending on the
   * value of {@code pMove}.
   *
   * @param name {@link QNm} reference
   * @return the current transaction
   * @throws SirixException if attribute couldn't be inserted
   * @throws NullPointerException if {@code name} or {@code move} is null
   */
  XmlNodeTrx insertNamespace(QNm name, @Nonnull Movement move);

  /**
   * Insert a subtree as a first child.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XmlShredder#createStringReader(String)},
   *        {@link XmlShredder#createFileReader(java.io.FileInputStream)} or
   *        {@link XmlShredder#createQueueReader(java.util.Queue)}
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XmlNodeTrx insertSubtreeAsFirstChild(XMLEventReader reader);

  /**
   * Insert a subtree as a right sibling.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XmlShredder#createStringReader(String)},
   *        {@link XmlShredder#createFileReader(java.io.FileInputStream)} or
   *        {@link XmlShredder#createQueueReader(java.util.Queue)}
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XmlNodeTrx insertSubtreeAsRightSibling(XMLEventReader reader);

  /**
   * Insert a subtree as a left sibling.
   *
   * @param reader {@link XMLEventReader} instance maybe derived from
   *        {@link XmlShredder#createStringReader(String)},
   *        {@link XmlShredder#createFileReader(java.io.FileInputStream)} or
   *        {@link XmlShredder#createQueueReader(java.util.Queue)}
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException if subtree is inserted as right sibling of a root-node or
   *         document-node
   * @throws NullPointerException if {@code reader} or {@code insert} is {@code null}
   */
  XmlNodeTrx insertSubtreeAsLeftSibling(XMLEventReader reader);

  /**
   * Remove currently selected node. This does automatically remove descendants. If two adjacent
   * {@link TextNode}s would be the result after the remove, the value of the former right sibling is
   * appended to the left sibling {@link TextNode} and removed afterwards.
   *
   * The cursor is located at the former right sibling. If there was no right sibling, it is located
   * at the former left sibling. If there was no left sibling, it is located at the former parent.
   *
   * @return the current transaction
   * @throws SirixException if node couldn't be removed
   */
  XmlNodeTrx remove();

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
  XmlNodeTrx setName(QNm name);

  /**
   * Set value of node.
   *
   * @param value new value of node
   * @return the current transaction
   * @throws SirixIOException if value couldn't be set
   * @throws NullPointerException if {@code value} is {@code null}
   */
  XmlNodeTrx setValue(String value);

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all.
   *
   * @throws SirixException if this revision couldn't be commited
   */
  @Override
  default XmlNodeTrx commit() {
    return commit(null);
  }

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all. The author assignes a commit message.
   *
   * @param commitMessage message of the commit
   * @throws SirixException if this revision couldn't be commited
   */
  @Override
  XmlNodeTrx commit(@Nullable String commitMessage);

  /**
   * Rollback all modifications of the exclusive write transaction.
   *
   * @throws SirixException if the changes in this revision couldn't be rollbacked
   */
  @Override
  XmlNodeTrx rollback();

  /**
   * Reverting all changes to the revision defined. This command has to be finalized with a commit. A
   * revert is always bound to a {@link XmlNodeReadOnlyTrx#moveToDocumentRoot()}.
   *
   * @param revision revert to the revision
   */
  @Override
  XmlNodeTrx revertTo(@Nonnegative int revision);

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
  @Override
  XmlNodeTrx addPreCommitHook(PreCommitHook hook);

  /**
   * Add a post commit hook.
   *
   * @param hook post commit hook
   */
  @Override
  XmlNodeTrx addPostCommitHook(PostCommitHook hook);

  // /**
  // * Get the page transaction used within the write transaction.
  // *
  // * @return the {@link PageWriteTrx} instance
  // */
  // @Beta
  // PageWriteTrx<Long, Record, UnorderedKeyValuePage> getPageTransaction();

  @Override
  XmlNodeTrx truncateTo(int revision);
}
