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

import org.sirix.access.Movement;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.PathSummary;
import org.sirix.index.value.AVLTree;
import org.sirix.node.TextNode;
import org.sirix.node.TextReferences;
import org.sirix.node.TextValue;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * <h1>IWriteTransaction</h1>
 * 
 * <h2>Description</h2>
 * 
 * <p>
 * Interface to access nodes based on the
 * Key/ParentKey/FirstChildKey/LeftSiblingKey
 * /RightSiblingKey/ChildCount/DescendantCount encoding. This encoding keeps the
 * children ordered but has no knowledge of the global node ordering. The
 * underlying tree is accessed in a cursor-like fashion.
 * </p>
 * 
 * <p>
 * Each commit at least adds <code>10kB</code> to the sirix file. It is thus
 * recommended to work with the auto commit mode only committing after a given
 * amount of node modifications or elapsed time. For very update-intensive data,
 * a value of one million modifications and ten seconds is recommended. Note
 * that this might require to increment the available heap.
 * </p>
 * 
 * <h2>Convention</h2>
 * 
 * <p>
 * <ol>
 * <li>Only a single thread accesses the single INodeWriteTransaction instance.</li>
 * <li><strong>Precondition</strong> before moving cursor:
 * <code>INodeWriteTransaction.getKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after modifying the cursor:
 * <code>(IWriteTransaction.insertX() == m &&
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
 * wtx.commit();
 * wtx.close();
 * 
 * // With auto commit after every 10th modification.
 * final INodeWriteTransaction wtx = session.beginNodeWriteTrx(10,
 * 		TimeUnit.MINUTES, 0);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * // 9 other modifications.
 * // Auto commit.
 * wtx.close();
 * 
 * // With auto commit after every minute.
 * final INodeWriteTransaction wtx = session.beginNodeWriteTrx(0,
 * 		TimeUnit.MINUTES, 1);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * ...
 * // Then abort.
 * wtx.abort();
 * wtx.close();
 * 
 * // With auto commit after every 10th modification and every second.
 * final INodeWriteTransaction wtx = session.beginNodeWriteTrx(10,
 * 		TimeUnit.SECONDS, 1);
 * wtx.insertElementAsFirstChild(new QName(&quot;foo&quot;));
 * ...
 * // Then abort.
 * wtx.abort();
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
public interface NodeWriteTrx extends NodeReadTrx {

	// --- Node Modifiers
	// --------------------------------------------------------

	/**
	 * Copy subtree from another {@code database/resource/revision} (the subtree
	 * rooted at the provided transaction) and insert as right sibling of the
	 * current node.
	 * 
	 * @param pRtx
	 *          read transaction reference which implements the
	 *          {@link NodeReadTrx} interface
	 * @return the transaction instance
	 * @throws SirixException
	 *           if anything in sirix fails
	 * @throws NullpointerException
	 *           if {@code pRtx} is {@code null}
	 */
	NodeWriteTrx copySubtreeAsFirstChild(@Nonnull NodeReadTrx pRtx)
			throws SirixException;

	/**
	 * Copy subtree from another {@code database/resource/revision} (the subtree
	 * rooted at the provided transaction) and insert as left sibling of the
	 * current node.
	 * 
	 * @param pRtx
	 *          read transaction reference which implements the
	 *          {@link NodeReadTrx} interface
	 * @return the transaction instance
	 * @throws SirixException
	 *           if anything in sirix fails
	 * @throws NullpointerException
	 *           if {@code pRtx} is {@code null}
	 */
	NodeWriteTrx copySubtreeAsLeftSibling(@Nonnull NodeReadTrx pRtx)
			throws SirixException;

	/**
	 * Copy subtree from another {@code database/resource/revision} (the subtree
	 * rooted at the provided transaction) and insert as right sibling of the
	 * current node.
	 * 
	 * @param pRtx
	 *          read transaction reference which implements the
	 *          {@link NodeReadTrx} interface
	 * @return the transaction instance
	 * @throws SirixException
	 *           if anything in sirix fails
	 * @throws NullpointerException
	 *           if {@code pRtx} is {@code null}
	 */
	NodeWriteTrx copySubtreeAsRightSibling(@Nonnull NodeReadTrx pRtx)
			throws SirixException;

	/**
	 * Replace a node with another node or subtree, depending on whether the
	 * replaced node is an {@code element}- or a {@code text-}node.
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
	 * @throws SirixException
	 *           if anything in Sirix fails
	 */
	NodeWriteTrx replaceNode(@Nonnull String pXML) throws SirixException,
			IOException, XMLStreamException;

	/**
	 * Replace a node with another node or subtree (the subtree rooted at the
	 * provided transaction), depending on whether the replaced node is an
	 * {@code element}- or a {@code text-}node.
	 * 
	 * @param pNode
	 *          a node from another resource
	 * @return the transaction instance
	 * @throws SirixException
	 *           if anything went wrong
	 */
	NodeWriteTrx replaceNode(@Nonnull NodeReadTrx pRtx) throws SirixException;

	/**
	 * Move a subtree rooted at {@code pToKey} to the first child of the current
	 * node.
	 * 
	 * @param pFromKey
	 *          root node key of the subtree to move
	 * @return the transaction instance
	 * @throws SirixException
	 *           if move adaption fails
	 * @throws IllegalArgumentException
	 *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or
	 *           {@code pFromKey == currentNodeKey}
	 * @throws NullPointerException
	 *           if {@code nodeToMove} does not exist, that is the node which is
	 *           denoted by it's node key {@code pFromKey}
	 */
	NodeWriteTrx moveSubtreeToFirstChild(@Nonnegative long pFromKey)
			throws SirixException;

	/**
	 * Move a subtree rooted at {@code pFromKey} to the right sibling of the
	 * current node. In case of the moved node is a text-node the value of the
	 * current node is prepended to the moved node and deleted afterwards. In this
	 * case the transaction is moved to the moved node.
	 * 
	 * @param pFromKey
	 *          root node key of the subtree to move
	 * @return the transaction instance
	 * @throws SirixException
	 *           if move adaption fails
	 * @throws IllegalArgumentException
	 *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or
	 *           {@code pFromKey == currentNodeKey}
	 * @throws NullPointerException
	 *           if {@code nodeToMove} does not exist, that is the node which is
	 *           denoted by it's node key {@code pFromKey}
	 */
	NodeWriteTrx moveSubtreeToRightSibling(long pFromKey) throws SirixException;

	/**
	 * Move a subtree rooted at {@code pFromKey} to the left sibling of the
	 * current node. In case of the moved node is a text-node the value of the
	 * current node is prepended to the moved node and deleted afterwards. In this
	 * case the transaction is moved to the moved node.
	 * 
	 * @param pFromKey
	 *          root node key of the subtree to move
	 * @return the transaction instance
	 * @throws SirixException
	 *           if move adaption fails
	 * @throws IllegalArgumentException
	 *           if {@code pFromKey < 0}, {@code pFromKey > maxNodeKey} or
	 *           {@code pFromKey == currentNodeKey}
	 * @throws NullPointerException
	 *           if {@code nodeToMove} does not exist, that is the node which is
	 *           denoted by it's node key {@code pFromKey}
	 */
	NodeWriteTrx moveSubtreeToLeftSibling(@Nonnegative long pFromKey)
			throws SirixException;

	/**
	 * Insert new comment node as left sibling of currently selected node. The
	 * cursor is moved to the inserted node.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertCommentAsLeftSibling(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert new comment node as right sibling of currently selected node. The
	 * cursor is moved to the inserted node.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertCommentAsRightSibling(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert new comment node as first child of currently selected node. The
	 * cursor is moved to the inserted node.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertCommentAsFirstChild(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert new Processing Instruction node as left sibling of currently
	 * selected node. The cursor is moved to the inserted node.
	 * 
	 * @param pContent
	 *          content of processing instruction
	 * @param pTarget
	 *          target of processing instruction
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertPIAsLeftSibling(@Nonnull String pContent,
			@Nonnull String pTarget) throws SirixException;

	/**
	 * Insert new Processing Instruction node as right sibling of currently
	 * selected node. The cursor is moved to the inserted node.
	 * 
	 * @param pContent
	 *          content of processing instruction
	 * @param pTarget
	 *          target of processing instruction
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertPIAsRightSibling(@Nonnull String pContent,
			@Nonnull String pTarget) throws SirixException;

	/**
	 * Insert new Processing Instruction node as first child of currently selected
	 * node. The cursor is moved to the inserted node.
	 * 
	 * @param pContent
	 *          content of processing instruction
	 * @param pTarget
	 *          target of processing instruction
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertPIAsFirstChild(@Nonnull String pContent,
			@Nonnull String pTarget) throws SirixException;

	/**
	 * Insert new element node as first child of currently selected node. The
	 * cursor is moved to the inserted node.
	 * 
	 * @param pName
	 *          {@link QName} of node to insert
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertElementAsFirstChild(@Nonnull QName pName)
			throws SirixException;

	/**
	 * Insert new element node as left sibling of currently selected node. The
	 * cursor is moved to the inserted node.
	 * 
	 * @param pName
	 *          {@link QName} of node to insert
	 * @throws SirixException
	 *           if element node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pQName} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertElementAsLeftSibling(@Nonnull QName pQName)
			throws SirixException;

	/**
	 * Insert new element node as right sibling of currently selected node. The
	 * transaction is moved to the inserted node.
	 * 
	 * @param pQName
	 *          {@link QName} of the new node
	 * @throws SirixException
	 *           if element node couldn't be inserted as right sibling
	 * @return the transaction instance
	 */
	NodeWriteTrx insertElementAsRightSibling(@Nonnull QName pQName)
			throws SirixException;

	/**
	 * Insert new text node as first child of currently selected node. The cursor
	 * is moved to the inserted node. If the result would be two adjacent
	 * {@link TextNode}s the value is appended with a single whitespace character
	 * prepended at first.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if text node couldn't be inserted as first child
	 * @throws NullPointerException
	 *           if {@code pValue} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertTextAsFirstChild(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert new text node as left sibling of currently selected node. The
	 * transaction is moved to the inserted node.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if text node couldn't be inserted as right sibling
	 * @throws NullPointerException
	 *           if {@code pValue} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertTextAsLeftSibling(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert new text node as right sibling of currently selected node. The
	 * transaction is moved to the inserted node.
	 * 
	 * @param pValue
	 *          value of node to insert
	 * @throws SirixException
	 *           if text node couldn't be inserted as right sibling
	 * @throws NullPointerException
	 *           if {@code pValue} is {@code null}
	 * @return the transaction instance
	 */
	NodeWriteTrx insertTextAsRightSibling(@Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert attribute in currently selected node. The cursor is moved to the
	 * inserted node.
	 * 
	 * @param pName
	 *          {@link QName} reference
	 * @param pValue
	 *          value of inserted node
	 * @throws SirixException
	 *           if attribute couldn't be inserted.
	 * @return the transaction instance
	 */
	NodeWriteTrx insertAttribute(@Nonnull QName pName, @Nonnull String pValue)
			throws SirixException;

	/**
	 * Insert attribute in currently selected node. The cursor is moved depending
	 * on the value of {@code pMove}.
	 * 
	 * @param pName
	 *          {@link QName} reference
	 * @param pValue
	 *          value of inserted node
	 * @throws SirixException
	 *           if attribute couldn't be inserted.
	 * @return the transaction instance
	 */
	NodeWriteTrx insertAttribute(@Nonnull QName pName, @Nonnull String pValue,
			@Nonnull Movement pMove) throws SirixException;

	/**
	 * Insert namespace declaration in currently selected node. The cursor is
	 * moved to the inserted node.
	 * 
	 * @param pName
	 *          {@link QName} reference
	 * @throws SirixException
	 *           if attribute couldn't be inserted.
	 * @return the current transaction
	 */
	NodeWriteTrx insertNamespace(@Nonnull QName pName) throws SirixException;

	/**
	 * Insert namespace declaration in currently selected node. The cursor is
	 * moved depending on the value of {@code pMove}.
	 * 
	 * @param pName
	 *          {@link QName} reference
	 * @return the current transaction
	 * @throws SirixException
	 *           if attribute couldn't be inserted.
	 */
	NodeWriteTrx insertNamespace(@Nonnull QName pQName, @Nonnull Movement pMove)
			throws SirixException;

/**
   * Insert a subtree.
   * 
   * @param pReader
   *            {@link XMLEventReader} instance maybe derived from {@link XMLShredder#createStringReader(String)}, {@link XMLShredder#createFileReader(java.io.File)} or {@link XMLShredder#createQueueReader(java.util.Queue).
   * @param pInsert
   *            insert position
   * @return the current transaction located at the root of the subtree which has been inserted
   * @throws SirixException
   *          if an I/O error occurs or another sirix internal error occurs
   * @throws IllegalStateException
   *          if subtree is inserted as right sibling of a root-node or document-node
   * @throws NullPointerException
   *          if {@code pReader} or {@code pInsert} is {@code null}
   */
	NodeWriteTrx insertSubtree(@Nonnull XMLEventReader pReader,
			@Nonnull Insert pInsert) throws SirixException;

	/**
	 * Remove currently selected node. This does automatically remove descendants.
	 * If two adjacent {@link TextNode}s would be the result after the remove, the
	 * value of the former right sibling is appended to the left sibling
	 * {@link TextNode} and removed afterwards.
	 * 
	 * The cursor is located at the former right sibling. If there was no right
	 * sibling, it is located at the former left sibling. If there was no left
	 * sibling, it is located at the former parent.
	 * 
	 * @throws SirixException
	 *           if node couldn't be removed
	 */
	void remove() throws SirixException;

	// --- Node Setters
	// -----------------------------------------------------------

	/**
	 * Set QName of node.
	 * 
	 * @param pName
	 *          new qualified name of node
	 * @throws SirixIOException
	 *           if can't set Name in node
	 * @throws NullPointerException
	 *           if {@code pName} is {@code null}
	 */
	void setQName(@Nonnull QName pName) throws SirixException;

	/**
	 * Set value of node.
	 * 
	 * @param pValue
	 *          new value of node
	 * @throws SirixIOException
	 *           if value couldn't be set
	 * @throws NullPointerException
	 *           if {@code pUri} is {@code null}
	 */
	void setValue(@Nonnull String pValue) throws SirixException;

	/**
	 * Commit all modifications of the exclusive write transaction. Even commit if
	 * there are no modification at all.
	 * 
	 * @throws SirixException
	 *           if this revision couldn't be commited
	 */
	void commit() throws SirixException;

	/**
	 * Abort all modifications of the exclusive write transaction.
	 * 
	 * @throws SirixException
	 *           if this revision couldn't be aborted
	 */
	void abort() throws SirixException;

	/**
	 * Reverting all changes to the revision defined. This command has to be
	 * finalized with a commit. A revert is always bound to a
	 * {@link NodeReadTrx#moveToDocumentRoot()}.
	 * 
	 * @param pRev
	 *          revert to the revision
	 * @throws SirixException
	 *           if anything went wrong
	 */
	void revertTo(@Nonnegative int pRev) throws SirixException;

	/**
	 * Closing current WriteTransaction.
	 * 
	 * @throws SirixIOException
	 *           if write transaction couldn't be closed
	 */
	@Override
	void close() throws SirixException;

	/**
	 * Add pre commit hook.
	 * 
	 * @param pHook
	 *          pre commit hook
	 */
	void addPreCommitHook(@Nonnull PreCommitHook pHook);

	/**
	 * Add a post commit hook.
	 * 
	 * @param pHook
	 *          post commit hook
	 */
	void addPostCommitHook(@Nonnull PostCommitHook pHook);

	/**
	 * Get the {@link PathSummary} associated with the current write transaction --
	 * might be {@code null} if no path summary index is used.
	 * 
	 * @return {@link PathSummary} instance
	 */
	PathSummary getPathSummary();

	/**
	 * Get the value index associated with the current write transaction -- might be
	 * {@code null} if no value index is used.
	 * 
	 * @return {@link AVLTree} instance
	 */
	AVLTree<TextValue, TextReferences> getValueIndex();
}
