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

package org.sirix.access.trx.node.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.HashType;
import org.sirix.access.trx.node.IndexController;
import org.sirix.access.trx.node.IndexController.ChangeType;
import org.sirix.access.trx.node.InternalResourceManager.Abort;
import org.sirix.access.trx.node.xdm.InsertPos;
import org.sirix.access.trx.node.xdm.XdmNodeReadTrxImpl;
import org.sirix.api.Axis;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.PostCommitHook;
import org.sirix.api.PreCommitHook;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.JsonBooleanNode;
import org.sirix.node.json.JsonNumberNode;
import org.sirix.node.json.JsonObjectKeyNode;
import org.sirix.node.json.JsonObjectNode;
import org.sirix.node.json.JsonStringNode;
import org.sirix.node.xdm.ElementNode;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * <h1>JSONNodeReadWriteTrxImpl</h1>
 *
 * <p>
 * Single-threaded instance of only write-transaction per resource, thus it is not thread-safe.
 * </p>
 *
 * <p>
 * If auto-commit is enabled, that is a scheduled commit(), all access to public methods is
 * synchronized, such that a commit() and another method doesn't interfere, which could produce
 * severe inconsistencies.
 * </p>
 *
 * <p>
 * All methods throw {@link NullPointerException}s in case of null values for reference parameters.
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
final class JsonNodeReadWriteTrxImpl extends AbstractForwardingJsonNodeReadOnlyTrx implements JsonNodeReadWriteTrx {

  /** Hash-function. */
  private final HashFunction mHash = Hashing.sha256();

  /** Prime for computing the hash. */
  private static final int PRIME = 77081;

  /** Maximum number of node modifications before auto commit. */
  private final int mMaxNodeCount;

  /** Modification counter. */
  private long mModificationCount;

  /** Hash kind of Structure. */
  private final HashType mHashKind;

  /** Scheduled executor service. */
  private final ScheduledExecutorService mPool =
      Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

  /** {@link XdmNodeReadTrxImpl} reference. */
  private final JsonNodeReadOnlyTrxImpl mNodeReadTrx;

  /** Determines if a bulk insert operation is done. */
  private boolean mBulkInsert;

  /** Collection holding pre-commit hooks. */
  private final List<PreCommitHook> mPreCommitHooks = new ArrayList<>();

  /** Collection holding post-commit hooks. */
  private final List<PostCommitHook> mPostCommitHooks = new ArrayList<>();

  /** {@link PathSummaryWriter} instance. */
  private PathSummaryWriter<JsonNodeReadOnlyTrx> mPathSummaryWriter;

  /**
   * Determines if a path summary should be built and kept up-to-date or not.
   */
  private final boolean mBuildPathSummary;

  /** {@link JsonNodeFactoryImpl} to be able to create nodes. */
  private JsonNodeFactoryImpl mNodeFactory;

  /** An optional lock for all methods, if an automatic commit is issued. */
  private final Optional<Semaphore> mLock;

  /** Determines if dewey IDs should be stored or not. */
  private final boolean mDeweyIDsStored;

  /** Determines if text values should be compressed or not. */
  private final boolean mCompression;

  /**
   * The {@link IndexController} used within the session this {@link XdmNodeWriteTrx} is bound to.
   */
  private final IndexController mIndexController;

  /**
   * Constructor.
   *
   * @param transactionID ID of transaction
   * @param resourceManager the {@link session} instance this transaction is bound to
   * @param pageWriteTrx {@link PageWriteTrx} to interact with the page layer
   * @param maxNodeCount maximum number of node modifications before auto commit
   * @param timeUnit unit of the number of the next param {@code pMaxTime}
   * @param maxTime maximum number of seconds before auto commit
   * @param trx the transaction to use
   * @throws SirixIOException if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  JsonNodeReadWriteTrxImpl(final @Nonnegative long transactionID, final JsonResourceManagerImpl resourceManager,
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx, final @Nonnegative int maxNodeCount,
      final TimeUnit timeUnit, final @Nonnegative int maxTime, final @Nonnull Node documentNode) {
    // Do not accept negative values.
    Preconditions.checkArgument(maxNodeCount >= 0 && maxTime >= 0,
        "Negative arguments for maxNodeCount and maxTime are not accepted.");

    mNodeReadTrx = new JsonNodeReadOnlyTrxImpl(resourceManager, transactionID, pageWriteTrx, documentNode);
    mIndexController = resourceManager.getWtxIndexController(pageWriteTrx.getRevisionNumber());
    mBuildPathSummary = resourceManager.getResourceConfig().pathSummary;

    // Only auto commit by node modifications if it is more then 0.
    mMaxNodeCount = maxNodeCount;
    mModificationCount = 0L;

    // Node factory.
    mNodeFactory = new JsonNodeFactoryImpl(pageWriteTrx);

    // Path summary.
    if (mBuildPathSummary) {
      mPathSummaryWriter = new PathSummaryWriter<>(pageWriteTrx, resourceManager, mNodeFactory, mNodeReadTrx);
    }

    if (maxTime > 0) {
      mPool.scheduleAtFixedRate(() -> commit(), maxTime, maxTime, timeUnit);
    }

    mHashKind = resourceManager.getResourceConfig().hashType;

    // Synchronize commit and other public methods if needed.
    mLock = maxTime > 0
        ? Optional.of(new Semaphore(1))
        : Optional.empty();

    mDeweyIDsStored = mNodeReadTrx.mResourceManager.getResourceConfig().areDeweyIDsStored;
    mCompression = mNodeReadTrx.mResourceManager.getResourceConfig().useTextCompression;

    // // Redo last transaction if the system crashed.
    // if (!pPageWriteTrx.isCreated()) {
    // try {
    // commit();
    // } catch (final SirixException e) {
    // throw new IllegalStateException(e);
    // }
    // }
  }

  /** Acquire a lock if necessary. */
  private void acquireLock() {
    if (mLock.isPresent()) {
      mLock.get().acquireUninterruptibly();
    }
  }

  /** Release a lock if necessary. */
  private void unLock() {
    if (mLock.isPresent()) {
      mLock.get().release();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertObjectAsFirstChild() {
    acquireLock();
    try {
      final Kind kind = getKind();
      if (kind == Kind.DOCUMENT || kind == Kind.JSON_OBJECT_KEY || kind == Kind.JSON_ARRAY) {
        checkAccessAndCommit();

        final long parentKey = mNodeReadTrx.getCurrentNode().getNodeKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = ((StructNode) mNodeReadTrx.getCurrentNode()).getFirstChildKey();

        final JsonObjectNode node = mNodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, 0);

        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertObjectAsRightSibling() {
    acquireLock();
    try {
      if (getParentKind() == Kind.JSON_ARRAY) {
        checkAccessAndCommit();

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = getCurrentNode().getNodeKey();
        final long rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();

        // Insert new text node if no adjacent text nodes are found.
        moveTo(leftSibKey);

        final JsonObjectNode node = mNodeFactory.createJsonObjectNode(parentKey, leftSibKey, rightSibKey, 0);

        // Adapt local nodes and hashes.
        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASRIGHTSIBLING, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        // Get the path node key.
        final long pathNodeKey = moveToParent().get().isObjectKey()
            ? ((JsonObjectKeyNode) getNode()).getPathNodeKey()
            : -1;
        mNodeReadTrx.setCurrentNode(node);

        // Index text value.
        mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertObjectKeyAsFirstChild(final String name) {
    checkNotNull(name);
    acquireLock();
    try {
      if (getKind() == Kind.JSON_OBJECT) {
        checkAccessAndCommit();

        final long parentKey = mNodeReadTrx.getCurrentNode().getNodeKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = ((StructNode) mNodeReadTrx.getCurrentNode()).getFirstChildKey();

        final long pathNodeKey = mBuildPathSummary
            ? mPathSummaryWriter.getPathNodeKey(new QNm(name), Kind.JSON_OBJECT_KEY)
            : 0;
        final JsonObjectKeyNode node =
            mNodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, 0, pathNodeKey, name);

        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertObjectKeyAsRightSibling(final String name) {
    checkNotNull(name);
    acquireLock();
    try {
      if (getCurrentNode().getKind() == Kind.JSON_OBJECT_KEY) {
        checkAccessAndCommit();

        final long key = getCurrentNode().getNodeKey();
        moveToParent();
        final long pathNodeKey = mBuildPathSummary
            ? mPathSummaryWriter.getPathNodeKey(new QNm(name), Kind.JSON_OBJECT_KEY)
            : 0;
        moveTo(key);

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = getCurrentNode().getNodeKey();
        final long rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();

        final JsonObjectKeyNode node =
            mNodeFactory.createJsonObjectKeyNode(parentKey, leftSibKey, rightSibKey, 0, pathNodeKey, name);

        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASRIGHTSIBLING, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertStringValueAsFirstChild(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      final Kind kind = getKind();
      if (kind == Kind.JSON_OBJECT_KEY) { // FIXME? || kind == Kind.DOCUMENT) {
        checkAccessAndCommit();

        final long pathNodeKey = getCurrentNode().getNodeKey();
        final long parentKey = getCurrentNode().getNodeKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();

        // Insert new text node if no adjacent text nodes are found.
        final byte[] textValue = getBytes(value);
        final JsonStringNode node =
            mNodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, 0, textValue, mCompression);

        // Adapt local nodes and hashes.
        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        // Index text value.
        mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode or TextNode!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx insertStringValueAsRightSibling(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      if (getCurrentNode() instanceof StructNode && getCurrentNode().getKind() != Kind.DOCUMENT) {
        checkAccessAndCommit();

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = getCurrentNode().getNodeKey();
        final long rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();

        // Insert new text node if no adjacent text nodes are found.
        moveTo(leftSibKey);
        final byte[] textValue = getBytes(value);

        final JsonStringNode node =
            mNodeFactory.createJsonStringNode(parentKey, leftSibKey, rightSibKey, 0, textValue, mCompression);

        // Adapt local nodes and hashes.
        mNodeReadTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASRIGHTSIBLING, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithAdd();

        // Get the path node key.
        final long pathNodeKey = moveToParent().get().isObjectKey()
            ? ((JsonObjectKeyNode) getNode()).getPathNodeKey()
            : -1;
        mNodeReadTrx.setCurrentNode(node);

        // Index text value.
        mIndexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
      }
    } finally {
      unLock();
    }
  }

  /**
   * Get a byte-array from a value.
   *
   * @param value the value
   * @return byte-array representation of {@code pValue}
   */
  private static byte[] getBytes(final String value) {
    return value.getBytes(Constants.DEFAULT_ENCODING);
  }

  @Override
  public JsonNodeReadWriteTrx remove() {
    checkAccessAndCommit();
    acquireLock();
    try {
      if (getCurrentNode().getKind() == Kind.DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      } else if (getCurrentNode() instanceof StructNode) {
        final StructNode node = (StructNode) mNodeReadTrx.getCurrentNode();

        // Remove subtree.
        for (final Axis axis = new PostOrderAxis(this); axis.hasNext();) {
          axis.next();

          // Remove name.
          removeName();

          // Remove text value.
          removeValue();

          // Then remove node.
          getPageTransaction().removeEntry(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        }

        // Adapt hashes and neighbour nodes as well as the name from the
        // NamePage mapping if it's not a text node.
        mNodeReadTrx.setCurrentNode(node);
        adaptHashesWithRemove();
        adaptForRemove(node, PageKind.RECORDPAGE);
        mNodeReadTrx.setCurrentNode(node);

        // Remove the name of subtree-root.
        if (node.getKind() == Kind.JSON_OBJECT_KEY) {
          removeName();
        }

        // Set current node (don't remove the moveTo(long) inside the if-clause which is needed
        // because of text merges.
        if (mNodeReadTrx.hasRightSibling() && moveTo(node.getRightSiblingKey()).hasMoved()) {
          // Do nothing.
        } else if (node.hasLeftSibling()) {
          moveTo(node.getLeftSiblingKey());
        } else {
          moveTo(node.getParentKey());
        }
      } else if (getCurrentNode().getKind() == Kind.ATTRIBUTE) {
        final ImmutableNode node = mNodeReadTrx.getCurrentNode();

        final ElementNode parent = (ElementNode) getPageTransaction().prepareEntryForModification(node.getParentKey(),
            PageKind.RECORDPAGE, -1);
        parent.removeAttribute(node.getNodeKey());
        adaptHashesWithRemove();
        getPageTransaction().removeEntry(node.getNodeKey(), PageKind.RECORDPAGE, -1);
        removeName();
        mIndexController.notifyChange(ChangeType.DELETE, getNode(), parent.getPathNodeKey());
        moveToParent();
      } else if (getCurrentNode().getKind() == Kind.NAMESPACE) {
        final ImmutableNode node = mNodeReadTrx.getCurrentNode();

        final ElementNode parent = (ElementNode) getPageTransaction().prepareEntryForModification(node.getParentKey(),
            PageKind.RECORDPAGE, -1);
        parent.removeNamespace(node.getNodeKey());
        adaptHashesWithRemove();
        getPageTransaction().removeEntry(node.getNodeKey(), PageKind.RECORDPAGE, -1);
        removeName();
        moveToParent();
      }

      return this;
    } finally {
      unLock();
    }
  }

  private void removeValue() throws SirixIOException {
    if (getCurrentNode() instanceof ValueNode) {
      final long nodeKey = getNodeKey();
      final long pathNodeKey = moveToParent().hasMoved()
          ? ((JsonObjectKeyNode) getNode()).getPathNodeKey()
          : -1;
      moveTo(nodeKey);
      mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);
    }
  }

  /**
   * Remove a name from the {@link NamePage} reference and the path summary if needed.
   *
   * @throws SirixException if Sirix fails
   */
  private void removeName() {
    if (getCurrentNode() instanceof NameNode) {
      final NameNode node = ((NameNode) getCurrentNode());
      final Kind nodeKind = node.getKind();
      final NamePage page =
          ((NamePage) getPageTransaction().getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(node.getPrefixKey(), nodeKind);
      page.removeName(node.getLocalNameKey(), nodeKind);
      page.removeName(node.getURIKey(), Kind.NAMESPACE);

      assert nodeKind != Kind.DOCUMENT;
      if (mBuildPathSummary) {
        mPathSummaryWriter.remove(node, nodeKind, page);
      }
    }
  }

  @Override
  public JsonNodeReadWriteTrx setObjectRecordName(final String name) {
    checkNotNull(name);
    acquireLock();
    try {
      if (getCurrentNode() instanceof NameNode) {
        checkAccessAndCommit();

        NameNode node = (NameNode) mNodeReadTrx.getCurrentNode();
        final long oldHash = node.hashCode();

        // Remove old keys from mapping.
        final Kind nodeKind = node.getKind();
        final int oldLocalNameKey = node.getLocalNameKey();
        final NamePage page =
            ((NamePage) getPageTransaction().getActualRevisionRootPage().getNamePageReference().getPage());
        page.removeName(oldLocalNameKey, nodeKind);

        // Create new key for mapping.
        final int localNameKey = name == null
            ? -1
            : getPageTransaction().createNameKey(name, node.getKind());

        // Set new keys for current node.
        node = (NameNode) getPageTransaction().prepareEntryForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setLocalNameKey(localNameKey);

        // Adapt path summary.
        if (mBuildPathSummary) {
          mPathSummaryWriter.adaptPathForChangedNode(node, new QNm(name), -1, -1, localNameKey, OPType.SETNAME);
        }

        // Set path node key.
        node.setPathNodeKey(mBuildPathSummary
            ? mPathSummaryWriter.getNodeKey()
            : 0);

        mNodeReadTrx.setCurrentNode(node);
        adaptHashedWithUpdate(oldHash);

        return this;
      } else {
        throw new SirixUsageException("setName is not allowed if current node is not an INameNode implementation!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx setStringValue(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      if (getCurrentNode().getKind() instanceof ValueNode) {
        checkAccessAndCommit();

        final long nodeKey = getNodeKey();
        final long pathNodeKey = moveToParent().get().getPathNodeKey();
        moveTo(nodeKey);

        // Remove old value from indexes.
        mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

        final long oldHash = mNodeReadTrx.getCurrentNode().hashCode();
        final byte[] byteVal = getBytes(value);

        final ValueNode node =
            (ValueNode) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
                PageKind.RECORDPAGE, -1);
        node.setValue(byteVal);

        mNodeReadTrx.setCurrentNode(node);
        adaptHashedWithUpdate(oldHash);

        // Index new value.
        mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "setValue(String) is not allowed if current node is not an IValNode implementation!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx setBooleanValue(final boolean value) {
    checkNotNull(value);
    acquireLock();
    try {
      if (getCurrentNode().getKind() == Kind.JSON_BOOLEAN_VALUE) {
        checkAccessAndCommit();

        final long nodeKey = getNodeKey();
        final long pathNodeKey = moveToParent().get().getPathNodeKey();
        moveTo(nodeKey);

        // Remove old value from indexes.
        mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

        final long oldHash = mNodeReadTrx.getCurrentNode().hashCode();

        final JsonBooleanNode node = (JsonBooleanNode) getPageTransaction().prepareEntryForModification(
            mNodeReadTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setValue(value);

        mNodeReadTrx.setCurrentNode(node);
        adaptHashedWithUpdate(oldHash);

        // Index new value.
        mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "setValue(String) is not allowed if current node is not an IValNode implementation!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx setNumberValue(final double value) {
    checkNotNull(value);
    acquireLock();
    try {
      if (getCurrentNode().getKind() == Kind.JSON_NUMBER_VALUE) {
        checkAccessAndCommit();

        final long nodeKey = getNodeKey();
        final long pathNodeKey = moveToParent().get().getPathNodeKey();
        moveTo(nodeKey);

        // Remove old value from indexes.
        mIndexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

        final long oldHash = mNodeReadTrx.getCurrentNode().hashCode();

        final JsonNumberNode node = (JsonNumberNode) getPageTransaction().prepareEntryForModification(
            mNodeReadTrx.getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setValue(value);

        mNodeReadTrx.setCurrentNode(node);
        adaptHashedWithUpdate(oldHash);

        // Index new value.
        mIndexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "setValue(String) is not allowed if current node is not an IValNode implementation!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx revertTo(final @Nonnegative int revision) {
    acquireLock();
    try {
      mNodeReadTrx.assertNotClosed();
      mNodeReadTrx.mResourceManager.assertAccess(revision);

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      mNodeReadTrx.mResourceManager.closeNodePageWriteTransaction(getId());
      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx =
          mNodeReadTrx.mResourceManager.createPageWriteTransaction(trxID, revision, revNumber - 1, Abort.NO, true);
      mNodeReadTrx.setPageReadTransaction(null);
      mNodeReadTrx.setPageReadTransaction(trx);
      mNodeReadTrx.mResourceManager.setNodePageWriteTransaction(getId(), trx);

      // Reset node factory.
      mNodeFactory = null;
      mNodeFactory = new JsonNodeFactoryImpl(trx);

      // New index instances.
      reInstantiateIndexes();

      // Reset modification counter.
      mModificationCount = 0L;

      // Move to document root.
      moveToDocumentRoot();

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public void close() {
    acquireLock();
    try {
      if (!isClosed()) {
        // Make sure to commit all dirty data.
        if (mModificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Release all state immediately.
        mNodeReadTrx.mResourceManager.closeWriteTransaction(getId());
        mNodeReadTrx.close();
        removeCommitFile();

        mPathSummaryWriter = null;
        mNodeFactory = null;

        // Shutdown pool.
        mPool.shutdown();
        try {
          mPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          throw new SirixThreadedException(e);
        }
      }
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx rollback() {
    acquireLock();
    try {
      mNodeReadTrx.assertNotClosed();

      // Reset modification counter.
      mModificationCount = 0L;

      // Close current page transaction.
      final long trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = getPageTransaction().getUberPage().isBootstrap()
          ? 0
          : revision - 1;

      final UberPage uberPage = getPageTransaction().rollback();

      // Remember succesfully committed uber page in resource manager.
      mNodeReadTrx.mResourceManager.setLastCommittedUberPage(uberPage);

      mNodeReadTrx.getPageTransaction().clearCaches();
      mNodeReadTrx.getPageTransaction().closeCaches();
      mNodeReadTrx.mResourceManager.closeNodePageWriteTransaction(getId());
      mNodeReadTrx.setPageReadTransaction(null);
      removeCommitFile();

      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx =
          mNodeReadTrx.mResourceManager.createPageWriteTransaction(trxID, revNumber, revNumber, Abort.YES, true);
      mNodeReadTrx.setPageReadTransaction(trx);
      mNodeReadTrx.mResourceManager.setNodePageWriteTransaction(getId(), trx);

      mNodeFactory = null;
      mNodeFactory = new JsonNodeFactoryImpl(trx);

      reInstantiateIndexes();

      return this;
    } finally {
      unLock();
    }
  }

  private void removeCommitFile() {
    try {
      final Path commitFile = mNodeReadTrx.mResourceManager.getCommitFile();
      if (java.nio.file.Files.exists(commitFile))
        java.nio.file.Files.delete(mNodeReadTrx.mResourceManager.getCommitFile());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public JsonNodeReadWriteTrx commit() {
    return commit(null);
  }

  /**
   * Create new instances.
   *
   * @param trxID transaction ID
   * @param revNumber revision number
   */
  private void reInstantiate(final @Nonnegative long trxID, final @Nonnegative int revNumber) {
    // Reset page transaction to new uber page.
    mNodeReadTrx.mResourceManager.closeNodePageWriteTransaction(getId());
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx =
        mNodeReadTrx.mResourceManager.createPageWriteTransaction(trxID, revNumber, revNumber, Abort.NO, true);
    mNodeReadTrx.setPageReadTransaction(null);
    mNodeReadTrx.setPageReadTransaction(trx);
    mNodeReadTrx.mResourceManager.setNodePageWriteTransaction(getId(), trx);

    mNodeFactory = null;
    mNodeFactory = new JsonNodeFactoryImpl(trx);

    reInstantiateIndexes();
  }

  /**
   * Create new instances for indexes.
   *
   * @param trxID transaction ID
   * @param revNumber revision number
   */
  @SuppressWarnings("unchecked")
  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (mBuildPathSummary) {
      mPathSummaryWriter = null;
      // mPathSummaryWriter =
      // new PathSummaryWriter<>((PageWriteTrx<Long, Record, UnorderedKeyValuePage>)
      // mNodeReadTrx.getPageTransaction(),
      // mNodeReadTrx.getResourceManager(), mNodeFactory, mNodeReadTrx);
    }

    // Recreate index listeners.
    mIndexController.createIndexListeners(mIndexController.getIndexes().getIndexDefs(), this);
  }

  /**
   * Modifying hashes in a postorder-traversal.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void postOrderTraversalHashes() throws SirixIOException {
    new PostOrderAxis(this, IncludeSelf.YES).forEach((unused) -> {
      addHashAndDescendantCount();
    });
  }

  /**
   * Add a hash.
   *
   * @param startNode start node
   */
  private void addParentHash(final ImmutableNode startNode) throws SirixIOException {
    switch (mHashKind) {
      case ROLLING:
        final long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
        final Node node =
            (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
                PageKind.RECORDPAGE, -1);
        node.setHash(node.getHash() + hashToAdd * PRIME);
        if (startNode instanceof StructNode) {
          ((StructNode) node).setDescendantCount(
              ((StructNode) node).getDescendantCount() + ((StructNode) startNode).getDescendantCount() + 1);
        }
        break;
      case POSTORDER:
        break;
      case NONE:
      default:
    }
  }

  /** Add a hash and the descendant count. */
  private void addHashAndDescendantCount() throws SirixIOException {
    switch (mHashKind) {
      case ROLLING:
        // Setup.
        final ImmutableNode startNode = getCurrentNode();
        final long oldDescendantCount = mNodeReadTrx.getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0
            ? 1
            : oldDescendantCount + 1;

        // Set start node.
        final long hashToAdd = mHash.hashLong(startNode.hashCode()).asLong();
        Node node = (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
            PageKind.RECORDPAGE, -1);
        node.setHash(hashToAdd);

        // Set parent node.
        if (startNode.hasParent()) {
          moveToParent();
          node = (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
          node.setHash(node.getHash() + hashToAdd * PRIME);
          setAddDescendants(startNode, node, descendantCount);
        }

        mNodeReadTrx.setCurrentNode(startNode);
        break;
      case POSTORDER:
        postorderAdd();
        break;
      case NONE:
      default:
    }
  }

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  private void checkAccessAndCommit() {
    mNodeReadTrx.assertNotClosed();
    mModificationCount++;
    intermediateCommitIfRequired();
  }

  // ////////////////////////////////////////////////////////////
  // insert operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for insert operations.
   *
   * @param newNode pointer of the new node to be inserted
   * @param insertPos determines the position where to insert
   * @param pageKind kind of subtree root page
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final Node newNode, final InsertPos insertPos, final PageKind pageKind)
      throws SirixIOException {
    assert newNode != null;
    assert insertPos != null;
    assert pageKind != null;

    if (newNode instanceof StructNode) {
      final StructNode strucNode = (StructNode) newNode;
      final StructNode parent =
          (StructNode) getPageTransaction().prepareEntryForModification(newNode.getParentKey(), pageKind, -1);
      parent.incrementChildCount();
      if (!((StructNode) newNode).hasLeftSibling()) {
        parent.setFirstChildKey(newNode.getNodeKey());
      }

      if (strucNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            (StructNode) getPageTransaction().prepareEntryForModification(strucNode.getRightSiblingKey(), pageKind, -1);
        rightSiblingNode.setLeftSiblingKey(newNode.getNodeKey());
      }
      if (strucNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            (StructNode) getPageTransaction().prepareEntryForModification(strucNode.getLeftSiblingKey(), pageKind, -1);
        leftSiblingNode.setRightSiblingKey(newNode.getNodeKey());
      }
    }
  }

  // ////////////////////////////////////////////////////////////
  // end of insert operation
  // ////////////////////////////////////////////////////////////

  // ////////////////////////////////////////////////////////////
  // remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for remove operations.
   *
   * @param oldNode pointer of the old node to be replaced
   * @throws SirixException if anything weird happens
   */
  private void adaptForRemove(final StructNode oldNode, final PageKind page) {
    assert oldNode != null;

    // Concatenate neighbor text nodes if they exist (the right sibling is
    // deleted afterwards).
    boolean concatenated = false;
    if (oldNode.hasLeftSibling() && oldNode.hasRightSibling() && moveTo(oldNode.getRightSiblingKey()).hasMoved()
        && getCurrentNode().getKind() == Kind.TEXT && moveTo(oldNode.getLeftSiblingKey()).hasMoved()
        && getCurrentNode().getKind() == Kind.TEXT) {
      final StringBuilder builder = new StringBuilder(getValue());
      moveTo(oldNode.getRightSiblingKey());
      builder.append(getValue());
      moveTo(oldNode.getLeftSiblingKey());
      // setValue(builder.toString());
      concatenated = true;
    }

    // Adapt left sibling node if there is one.
    if (oldNode.hasLeftSibling()) {
      final StructNode leftSibling =
          (StructNode) getPageTransaction().prepareEntryForModification(oldNode.getLeftSiblingKey(), page, -1);
      if (concatenated) {
        moveTo(oldNode.getRightSiblingKey());
        leftSibling.setRightSiblingKey(((StructNode) getCurrentNode()).getRightSiblingKey());
      } else {
        leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
      }
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      StructNode rightSibling;
      if (concatenated) {
        moveTo(oldNode.getRightSiblingKey());
        moveTo(mNodeReadTrx.getStructuralNode().getRightSiblingKey());
        rightSibling =
            (StructNode) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
                page, -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      } else {
        rightSibling =
            (StructNode) getPageTransaction().prepareEntryForModification(oldNode.getRightSiblingKey(), page, -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      }
    }

    // Adapt parent, if node has now left sibling it is a first child.
    StructNode parent = (StructNode) getPageTransaction().prepareEntryForModification(oldNode.getParentKey(), page, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
    }
    parent.decrementChildCount();
    if (concatenated) {
      parent.decrementDescendantCount();
      parent.decrementChildCount();
    }
    if (concatenated) {
      // Adjust descendant count.
      moveTo(parent.getNodeKey());
      while (parent.hasParent()) {
        moveToParent();
        final StructNode ancestor =
            (StructNode) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
                page, -1);
        ancestor.decrementDescendantCount();
        parent = ancestor;
      }
    }

    // Remove right sibling text node if text nodes have been
    // concatenated/merged.
    if (concatenated) {
      moveTo(oldNode.getRightSiblingKey());
      getPageTransaction().removeEntry(mNodeReadTrx.getNodeKey(), page, -1);
    }

    // Remove non structural nodes of old node.
    if (oldNode.getKind() == Kind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      // removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    getPageTransaction().removeEntry(oldNode.getNodeKey(), page, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Making an intermediate commit based on set attributes.
   *
   * @throws SirixException if commit fails
   */
  private void intermediateCommitIfRequired() {
    mNodeReadTrx.assertNotClosed();
    if ((mMaxNodeCount > 0) && (mModificationCount > mMaxNodeCount)) {
      commit();
    }
  }

  /**
   * Get the page transaction.
   *
   * @return the page transaction
   */
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> getPageTransaction() {
    @SuppressWarnings("unchecked")
    final PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx =
        (PageWriteTrx<Long, Record, UnorderedKeyValuePage>) mNodeReadTrx.getPageTransaction();
    return trx;
  }

  /**
   * Adapting the structure with a hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashesWithAdd() throws SirixIOException {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingAdd();
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with remove.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashesWithRemove() throws SirixIOException {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingRemove();
          break;
        case POSTORDER:
          postorderRemove();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with update.
   *
   * @param pOldHash pOldHash to be removed
   * @throws SirixIOException if an I/O error occurs
   */
  private void adaptHashedWithUpdate(final long pOldHash) throws SirixIOException {
    if (!mBulkInsert) {
      switch (mHashKind) {
        case ROLLING:
          rollingUpdate(pOldHash);
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Removal operation for postorder hash computation.
   *
   * @throws SirixIOException if anything weird happens
   */
  private void postorderRemove() {
    moveTo(getCurrentNode().getParentKey());
    postorderAdd();
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if anything weird happened
   */
  private void postorderAdd() {
    // start with hash to add
    final ImmutableNode startNode = getCurrentNode();
    // long for adapting the hash of the parent
    long hashCodeForParent = 0;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node =
          (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
      node.setHash(mHash.hashLong(mNodeReadTrx.getCurrentNode().hashCode()).asLong());
      moveTo(mNodeReadTrx.getCurrentNode().getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot =
          (StructNode) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
      hashCodeForParent = mNodeReadTrx.getCurrentNode().hashCode() + hashCodeForParent * PRIME;
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == Kind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          moveTo(currentElement.getAttributeKey(i));
          hashCodeForParent = mNodeReadTrx.getCurrentNode().hashCode() + hashCodeForParent * PRIME;
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          moveTo(currentElement.getNamespaceKey(i));
          hashCodeForParent = mNodeReadTrx.getCurrentNode().hashCode() + hashCodeForParent * PRIME;
        }
        moveTo(cursorToRoot.getNodeKey());
      }

      // Caring about the children of a node
      if (moveTo(mNodeReadTrx.getStructuralNode().getFirstChildKey()).hasMoved()) {
        do {
          hashCodeForParent = mNodeReadTrx.getCurrentNode().getHash() + hashCodeForParent * PRIME;
        } while (moveTo(mNodeReadTrx.getStructuralNode().getRightSiblingKey()).hasMoved());
        moveTo(mNodeReadTrx.getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
      hashCodeForParent = 0;
    } while (moveTo(cursorToRoot.getParentKey()).hasMoved());

    mNodeReadTrx.setCurrentNode(startNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final long oldHash) {
    final ImmutableNode newNode = getCurrentNode();
    final long hash = newNode.hashCode();
    final long newNodeHash = hash;
    long resultNew = hash;

    // go the path to the root
    do {
      final Node node =
          (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == newNode.getNodeKey()) {
        resultNew = node.getHash() - oldHash;
        resultNew = resultNew + newNodeHash;
      } else {
        resultNew = node.getHash() - oldHash * PRIME;
        resultNew = resultNew + newNodeHash * PRIME;
      }
      node.setHash(resultNew);
    } while (moveTo(mNodeReadTrx.getCurrentNode().getParentKey()).hasMoved());

    mNodeReadTrx.setCurrentNode(newNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final ImmutableNode startNode = getCurrentNode();
    long hashToRemove = startNode.getHash();
    long hashToAdd = 0;
    long newHash = 0;
    // go the path to the root
    do {
      final Node node =
          (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // the begin node is always null
        newHash = 0;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // the parent node is just removed
        newHash = node.getHash() - hashToRemove * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      } else {
        // the ancestors are all touched regarding the modification
        newHash = node.getHash() - hashToRemove * PRIME;
        newHash = newHash + hashToAdd * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      }
      node.setHash(newHash);
      hashToAdd = newHash;
    } while (moveTo(mNodeReadTrx.getCurrentNode().getParentKey()).hasMoved());

    mNodeReadTrx.setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startNode the node which has been removed
   */
  private void setRemoveDescendants(final ImmutableNode startNode) {
    assert startNode != null;
    if (startNode instanceof StructNode) {
      final StructNode node = ((StructNode) getCurrentNode());
      node.setDescendantCount(node.getDescendantCount() - ((StructNode) startNode).getDescendantCount() - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() throws SirixIOException {
    // start with hash to add
    final ImmutableNode startNode = mNodeReadTrx.getCurrentNode();
    final long oldDescendantCount = mNodeReadTrx.getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0
        ? 1
        : oldDescendantCount + 1;
    long hashToAdd = startNode.getHash() == 0
        ? mHash.hashLong(startNode.hashCode()).asLong()
        : startNode.getHash();
    long newHash = 0;
    long possibleOldHash = 0;
    // go the path to the root
    do {
      final Node node =
          (Node) getPageTransaction().prepareEntryForModification(mNodeReadTrx.getCurrentNode().getNodeKey(),
              PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // at the beginning, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // at the parent level, just add the node
        possibleOldHash = node.getHash();
        newHash = possibleOldHash + hashToAdd * PRIME;
        hashToAdd = newHash;
        setAddDescendants(startNode, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element
        // and add the new one
        newHash = node.getHash() - possibleOldHash * PRIME;
        newHash = newHash + hashToAdd * PRIME;
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startNode, node, descendantCount);
      }
      node.setHash(newHash);
    } while (moveTo(mNodeReadTrx.getCurrentNode().getParentKey()).hasMoved());
    mNodeReadTrx.setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after an add-operation.
   *
   * @param startNode the node which has been removed
   * @param nodeToModify node to modify
   * @param descendantCount the descendantCount to add
   */
  private static void setAddDescendants(final ImmutableNode startNode, final Node nodeToModifiy,
      final @Nonnegative long descendantCount) {
    assert startNode != null;
    assert descendantCount >= 0;
    assert nodeToModifiy != null;
    if (startNode instanceof StructNode) {
      final StructNode node = (StructNode) nodeToModifiy;
      final long oldDescendantCount = node.getDescendantCount();
      node.setDescendantCount(oldDescendantCount + descendantCount);
    }
  }

  /**
   * Check current node type (must be a structural node).
   */
  private void checkCurrentNode() {
    if (!(getCurrentNode() instanceof StructNode)) {
      throw new IllegalStateException("Current node must be a structural node!");
    }
  }

  /**
   * Get the current node.
   *
   * @return {@link Node} implementation
   */
  private ImmutableNode getCurrentNode() {
    return mNodeReadTrx.getCurrentNode();
  }

  private void removeOldNode(final StructNode node, final @Nonnegative long key) {
    assert node != null;
    assert key >= 0;
    moveTo(node.getNodeKey());
    remove();
    moveTo(key);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("readTrx", mNodeReadTrx.toString())
                      .add("hashKind", mHashKind)
                      .toString();
  }

  @Override
  protected JsonNodeReadOnlyTrx delegate() {
    return mNodeReadTrx;
  }

  @Override
  public JsonNodeReadWriteTrx addPreCommitHook(final PreCommitHook hook) {
    acquireLock();
    try {
      mPreCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx addPostCommitHook(final PostCommitHook hook) {
    acquireLock();
    try {
      mPostCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof JsonNodeReadWriteTrxImpl) {
      final JsonNodeReadWriteTrxImpl wtx = (JsonNodeReadWriteTrxImpl) obj;
      return Objects.equal(mNodeReadTrx, wtx.mNodeReadTrx);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeReadTrx);
  }

  @Override
  public PathSummaryReader getPathSummary() {
    acquireLock();
    try {
      return mPathSummaryWriter.getPathSummary();
    } finally {
      unLock();
    }
  }

  @Override
  public JsonNodeReadWriteTrx truncateTo(final int revision) {
    mNodeReadTrx.assertNotClosed();

    // TODO

    return this;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    mNodeReadTrx.assertNotClosed();

    return mNodeReadTrx.getCommitCredentials();
  }

  @Override
  public JsonNodeReadWriteTrx commit(final String commitMessage) {
    mNodeReadTrx.assertNotClosed();

    // Optionally lock while commiting and assigning new instances.
    acquireLock();
    try {
      // Execute pre-commit hooks.
      for (final PreCommitHook hook : mPreCommitHooks) {
        hook.preCommit(this);
      }

      // Reset modification counter.
      mModificationCount = 0L;

      final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWtx = getPageTransaction();
      final UberPage uberPage = commitMessage == null
          ? pageWtx.commit()
          : pageWtx.commit(commitMessage);

      // Remember succesfully committed uber page in resource manager.
      mNodeReadTrx.mResourceManager.setLastCommittedUberPage(uberPage);

      // Reinstantiate everything.
      reInstantiate(getId(), getRevisionNumber());
    } finally {
      unLock();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : mPostCommitHooks) {
      hook.postCommit(this);
    }

    return this;
  }

  @Override
  public JsonNodeReadWriteTrx insertArrayAsFirstChild() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertArrayAsRightSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertBooleanValueAsFirstChild(boolean value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertBooleanValueAsRightSibling(boolean value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertNumberValueAsFirstChild(double value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertNumberValueAsRightSibling(double value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertNullValueAsFirstChild() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadWriteTrx insertNullValueAsRightSibling() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PageWriteTrx<Long, Record, UnorderedKeyValuePage> getPageWtx() {
    // TODO Auto-generated method stub
    return null;
  }
}
