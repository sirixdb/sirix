package org.sirix.xquery.node;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Una;
import org.brackit.xquery.node.parser.NavigationalSubtreeParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Node;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Scope;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.TemporalNode;
import org.brackit.xquery.xdm.type.NodeType;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.AttributeAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NonStructuralWrapperAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FirstAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.LastAxis;
import org.sirix.axis.temporal.NextAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.axis.temporal.PreviousAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.SirixDeweyID;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.stream.SirixStream;
import org.sirix.xquery.stream.TemporalSirixStream;
import org.slf4j.LoggerFactory;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

/**
 * A node which is used to provide all XDM functionality as well as temporal functions.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class DBNode extends AbstractTemporalNode<DBNode> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(DBNode.class));

  /** Sirix {@link XdmNodeReadOnlyTrx}. */
  private final XdmNodeReadOnlyTrx mRtx;

  /** Sirix node key. */
  private final long mNodeKey;

  /** Kind of node. */
  private final org.sirix.node.Kind mKind;

  /** Collection this node is part of. */
  private final DBCollection mCollection;

  /** Determines if write-transaction is present. */
  private final boolean mIsWtx;

  /** {@link Scope} of node. */
  private SirixScope mScope;

  /** Optional dewey ID. */
  private final Optional<SirixDeweyID> mDeweyID;

  /**
   * Constructor.
   *
   * @param rtx {@link XdmNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link DBCollection} reference
   */
  public DBNode(final XdmNodeReadOnlyTrx rtx, final DBCollection collection) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mIsWtx = mRtx instanceof XdmNodeTrx;
    mNodeKey = mRtx.getNodeKey();
    mKind = mRtx.getKind();
    mDeweyID = mRtx.getNode().getDeweyID();
  }

  /**
   * Create a new {@link IReadTransaction} and move to {@link mKey}.
   *
   * @return new read transaction instance which is moved to {@link mKey}
   */
  private final void moveRtx() {
    mRtx.moveTo(mNodeKey);
  }

  /**
   * Get underlying node.
   *
   * @return underlying node
   */
  public org.sirix.node.interfaces.immutable.ImmutableNode getUnderlyingNode() {
    moveRtx();
    return mRtx.getNode();
  }

  @Override
  public boolean isSelfOf(final Node<?> other) {
    moveRtx();
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (node.getUnderlyingNode().getNodeKey() == this.getUnderlyingNode().getNodeKey()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isParentOf(final Node<?> other) {
    moveRtx();
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (node.getUnderlyingNode().getParentKey() == mRtx.getNodeKey()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isChildOf(final Node<?> other) {
    moveRtx();
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (node.getUnderlyingNode().getNodeKey() == mRtx.getParentKey()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isDescendantOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      moveRtx();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isDescendantOf(node.mDeweyID.get());
        } else {
          for (final Axis axis = new AncestorAxis(mRtx); axis.hasNext();) {
            axis.next();
            if (node.getUnderlyingNode().getNodeKey() == mRtx.getNodeKey()) {
              retVal = true;
            }
          }
        }
      }
    }
    return retVal;
  }

  /**
   * Get the transaction.
   *
   * @return transaction handle
   */
  public XdmNodeReadOnlyTrx getTrx() {
    moveRtx();
    return mRtx;
  }

  @Override
  public boolean isDescendantOrSelfOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (isSelfOf(other)) {
        retVal = true;
      }
      retVal = isDescendantOf(other);
    }
    return retVal;
  }

  @Override
  public boolean isAncestorOf(final Node<?> other) {
    moveRtx();
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (mDeweyID.isPresent()) {
        return mDeweyID.get().isAncestorOf(node.mDeweyID.get());
      } else {
        return other.isDescendantOf(this);
      }
    }
    return false;
  }

  @Override
  public boolean isAncestorOrSelfOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      if (mDeweyID.isPresent()) {
        retVal = mDeweyID.get().isAncestorOf(node.mDeweyID.get());
      } else {
        if (isSelfOf(other)) {
          retVal = true;
        }
        retVal = other.isDescendantOf(this);
      }
    }
    return retVal;
  }

  @Override
  public boolean isSiblingOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      try {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isSiblingOf(node.mDeweyID.get());
        }
        if (node.getKind() != Kind.NAMESPACE && node.getKind() != Kind.ATTRIBUTE
            && node.getParent().getUnderlyingNode().getNodeKey() == ((DBNode) other.getParent()).getUnderlyingNode()
                                                                                                .getNodeKey()) {
          retVal = true;
        }
      } catch (final DocumentException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
    return retVal;
  }

  @Override
  public boolean isPrecedingSiblingOf(final Node<?> other) {
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      moveRtx();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isPrecedingSiblingOf(node.mDeweyID.get());
        } else {
          while (mRtx.hasRightSibling()) {
            mRtx.moveToRightSibling();
            if (mRtx.getNodeKey() == node.getNodeKey()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public boolean isFollowingSiblingOf(final Node<?> other) {
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      moveRtx();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isFollowingSiblingOf(node.mDeweyID.get());
        } else {
          while (mRtx.hasLeftSibling()) {
            mRtx.moveToLeftSibling();
            if (mRtx.getNodeKey() == node.getNodeKey()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public boolean isPrecedingOf(final Node<?> other) {
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      moveRtx();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isPrecedingOf(node.mDeweyID.get());
        } else {
          for (final Axis axis = new FollowingAxis(mRtx); axis.hasNext();) {
            axis.next();
            if (mRtx.getNodeKey() == node.getNodeKey()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public boolean isFollowingOf(final Node<?> other) {
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      moveRtx();
      if (mKind != org.sirix.node.Kind.ATTRIBUTE && mKind != org.sirix.node.Kind.NAMESPACE) {
        if (mDeweyID.isPresent()) {
          return mDeweyID.get().isFollowingOf(node.mDeweyID.get());
        } else {
          for (final Axis axis = new PrecedingAxis(mRtx); axis.hasNext();) {
            axis.next();
            if (mRtx.getNodeKey() == node.getNodeKey()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public boolean isAttributeOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      try {
        if (getParent().getUnderlyingNode().getNodeKey() == node.getUnderlyingNode().getNodeKey()) {
          retVal = true;
        }
      } catch (final DocumentException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
    return retVal;
  }

  @Override
  public boolean isDocumentOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (getKind() == Kind.DOCUMENT && other instanceof DBNode) {
      final DBNode node = (DBNode) other;
      assert node.getNodeClassID() == this.getNodeClassID();
      final NodeReadTrx rtx = node.getTrx();
      if (rtx.getRevisionNumber() == mRtx.getRevisionNumber()
          && rtx.getResourceManager().getResourceConfig().getID() == mRtx.getResourceManager()
                                                                         .getResourceConfig()
                                                                         .getID()) {
        retVal = true;
      }
    }
    return retVal;
  }

  @Override
  public boolean isDocumentRoot() {
    moveRtx();
    return mRtx.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()
        ? true
        : false;
  }

  @Override
  public boolean isRoot() {
    moveRtx();
    return mRtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()
        ? true
        : false;
  }

  @Override
  public int getNodeClassID() {
    return 1732483;
  }

  @Override
  public DBCollection getCollection() {
    return mCollection;
  }

  @Override
  public Scope getScope() {
    if (mScope == null && mKind == org.sirix.node.Kind.ELEMENT) {
      mScope = new SirixScope(this);
    }
    return mScope;
  }

  @Override
  public Kind getKind() {
    moveRtx();
    switch (mRtx.getKind()) {
      case XDM_DOCUMENT:
        return Kind.DOCUMENT;
      case ELEMENT:
        return Kind.ELEMENT;
      case TEXT:
        return Kind.TEXT;
      case COMMENT:
        return Kind.COMMENT;
      case PROCESSING_INSTRUCTION:
        return Kind.PROCESSING_INSTRUCTION;
      case NAMESPACE:
        return Kind.NAMESPACE;
      case ATTRIBUTE:
        return Kind.ATTRIBUTE;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("Kind not known!");
    }
  }

  @Override
  public QNm getName() throws DocumentException {
    moveRtx();
    return mRtx.getName();
  }

  @Override
  public void setName(final QNm name) throws OperationNotSupportedException, DocumentException {
    if (mIsWtx) {
      moveRtx();
      final XdmNodeTrx wtx = (XdmNodeTrx) mRtx;
      if (wtx.isNameNode()) {
        try {
          wtx.setName(name);
        } catch (final SirixException e) {
          throw new DocumentException(e);
        }
      } else {
        throw new DocumentException("Node has no name!");
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.setName(name);
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  @Override
  public Atomic getValue() throws DocumentException {
    moveRtx();

    final String value;
    switch (mKind) {
      case XDM_DOCUMENT:
      case ELEMENT:
        value = expandString();
        break;
      case ATTRIBUTE:
        value = emptyIfNull(mRtx.getValue());
        break;
      case TEXT:
        value = mRtx.getValue();
        break;
      case COMMENT:
      case PROCESSING_INSTRUCTION:
        value = emptyIfNull(mRtx.getValue());
        break;
      // $CASES-OMITTED$
      default:
        value = "";
    }
    return new Una(value);
  }

  /**
   * Treat a node value of null as an empty string.
   *
   * @param s the node value
   * @return a zero-length string if s is null, otherwise s
   */
  private static String emptyIfNull(final String s) {
    return (s == null
        ? ""
        : s);
  }

  /**
   * Filter text nodes.
   *
   * @return concatenated String of text node values
   */
  private String expandString() {
    final StringBuilder buffer = new StringBuilder();
    final Axis axis = new DescendantAxis(mRtx);
    while (axis.hasNext()) {
      axis.next();
      if (mRtx.isText()) {
        buffer.append(mRtx.getValue());
      }
    }
    return buffer.toString();
  }

  @Override
  public void setValue(final Atomic value) throws OperationNotSupportedException, DocumentException {
    moveRtx();
    if (!mRtx.isValueNode()) {
      throw new DocumentException("Node has no value!");
    }
    if (mIsWtx) {
      final XdmNodeTrx wtx = (XdmNodeTrx) mRtx;
      try {
        wtx.setValue(value.stringValue());
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.setValue(value.stringValue());
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  @Override
  public DBNode getParent() throws DocumentException {
    moveRtx();
    return mRtx.hasParent()
        ? new DBNode(mRtx.moveToParent().get(), mCollection)
        : null;
  }

  @Override
  public DBNode getFirstChild() throws DocumentException {
    moveRtx();
    return mRtx.hasFirstChild()
        ? new DBNode(mRtx.moveToFirstChild().get(), mCollection)
        : null;
  }

  @Override
  public DBNode getLastChild() throws DocumentException {
    moveRtx();
    return mRtx.hasLastChild()
        ? new DBNode(mRtx.moveToLastChild().get(), mCollection)
        : null;
  }

  @Override
  public Stream<DBNode> getChildren() throws DocumentException {
    moveRtx();
    return new SirixStream(new ChildAxis(mRtx), mCollection);
  }

  // Returns all nodes in the subtree _including_ the subtree root.
  @Override
  public Stream<DBNode> getSubtree() throws DocumentException {
    moveRtx();
    return new SirixStream(new NonStructuralWrapperAxis(new DescendantAxis(mRtx, IncludeSelf.YES)), mCollection);
  }

  @Override
  public boolean hasChildren() throws DocumentException {
    moveRtx();
    return mRtx.getChildCount() > 0;
  }

  @Override
  public DBNode getNextSibling() throws DocumentException {
    moveRtx();
    return mRtx.hasRightSibling()
        ? new DBNode(mRtx.moveToRightSibling().get(), mCollection)
        : null;
  }

  @Override
  public DBNode getPreviousSibling() throws DocumentException {
    moveRtx();
    return mRtx.hasLeftSibling()
        ? new DBNode(mRtx.moveToLeftSibling().get(), mCollection)
        : null;
  }

  @Override
  public DBNode append(final Kind kind, final QNm name, final Atomic value) throws DocumentException {
    if (mIsWtx) {
      moveRtx();
      final XdmNodeTrx wtx = (XdmNodeTrx) mRtx;
      try {
        return append(wtx, kind, name, value);
      } catch (final SirixException e) {
        wtx.close();
        throw new DocumentException(e);
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return append(wtx, kind, name, value);
      } catch (final SirixException e) {
        throw new DocumentException(e);
      } finally {
        wtx.close();
      }
    }
  }

  private DBNode append(final XdmNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws SirixException {
    if (wtx.hasFirstChild()) {
      wtx.moveToLastChild();
      switch (kind) {
        case DOCUMENT:
          break;
        case ELEMENT:
          wtx.insertElementAsRightSibling(name);
          break;
        case ATTRIBUTE:
          wtx.insertAttribute(name, value.asStr().stringValue());
          break;
        case NAMESPACE:
          wtx.insertNamespace(name);
          break;
        case TEXT:
          wtx.insertTextAsRightSibling(value.asStr().stringValue());
          break;
        case COMMENT:
          wtx.insertCommentAsRightSibling(value.asStr().stringValue());
          break;
        case PROCESSING_INSTRUCTION:
          wtx.insertPIAsRightSibling(value.asStr().stringValue(), name.getLocalName());
          break;
        default:
          throw new AssertionError(); // May not happen.
      }
    } else {
      switch (kind) {
        case DOCUMENT:
          break;
        case ELEMENT:
          wtx.insertElementAsFirstChild(name);
          break;
        case ATTRIBUTE:
          wtx.insertAttribute(name, value.asStr().stringValue());
          break;
        case NAMESPACE:
          wtx.insertNamespace(name);
          break;
        case TEXT:
          wtx.insertTextAsFirstChild(value.asStr().stringValue());
          break;
        case COMMENT:
          wtx.insertCommentAsFirstChild(value.asStr().stringValue());
          break;
        case PROCESSING_INSTRUCTION:
          wtx.insertPIAsFirstChild(value.asStr().stringValue(), name.getLocalName());
          break;
        default:
          throw new AssertionError(); // May not happen.
      }
    }

    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode append(final Node<?> child) throws DocumentException {
    if (mIsWtx) {
      moveRtx();
      try {
        return append((XdmNodeTrx) mRtx, child);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return append(wtx, child);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode append(final XdmNodeTrx wtx, final Node<?> child) throws DocumentException {
    try {
      if (!(child.getKind() == Kind.ELEMENT))
        return append(wtx, child.getKind(), child.getName(), child.getValue());

      final SubtreeBuilder builder;

      if (wtx.hasFirstChild()) {
        wtx.moveToLastChild();

        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASRIGHTSIBLING, Collections.emptyList());
      } else {
        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASFIRSTCHILD, Collections.emptyList());
      }
      child.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode append(final SubtreeParser parser) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return append(mRtx, parser);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return append(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode append(final XdmNodeReadOnlyTrx rtx, final SubtreeParser parser) throws DocumentException {
    try {
      if (rtx.hasFirstChild()) {
        rtx.moveToLastChild();
      }

      parser.parse(
          new SubtreeBuilder(mCollection, (XdmNodeTrx) rtx, Insert.ASRIGHTSIBLING, Collections.emptyList()));

      moveRtx();
      rtx.moveToFirstChild();
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new DBNode(rtx, mCollection);
  }

  @Override
  public DBNode prepend(final Kind kind, final QNm name, final Atomic value) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return prepend((XdmNodeTrx) mRtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return prepend((XdmNodeTrx) mRtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode prepend(final XdmNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws DocumentException {
    try {
      switch (kind) {
        case DOCUMENT:
          break;
        case ELEMENT:
          wtx.insertElementAsFirstChild(name);
          break;
        case ATTRIBUTE:
          wtx.insertAttribute(name, value.asStr().stringValue());
          break;
        case NAMESPACE:
          wtx.insertNamespace(name);
          break;
        case TEXT:
          wtx.insertTextAsFirstChild(value.asStr().stringValue());
          break;
        case COMMENT:
          wtx.insertCommentAsFirstChild(value.asStr().stringValue());
          break;
        case PROCESSING_INSTRUCTION:
          wtx.insertPIAsFirstChild(value.asStr().stringValue(), name.getLocalName());
          break;
        default:
          throw new AssertionError(); // May not happen.
      }
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode prepend(final Node<?> child) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return prepend((XdmNodeTrx) mRtx, child);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return prepend(wtx, child);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode prepend(final XdmNodeTrx wtx, final Node<?> child) throws DocumentException {
    try {
      if (!(child.getKind() == Kind.ELEMENT))
        return prepend(wtx, child.getKind(), child.getName(), child.getValue());

      SubtreeBuilder builder = null;
      if (wtx.hasFirstChild()) {
        wtx.moveToFirstChild();

        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASLEFTSIBLING, Collections.emptyList());
      } else {
        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASFIRSTCHILD, Collections.emptyList());
      }
      child.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode prepend(final SubtreeParser parser) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return prepend((XdmNodeTrx) mRtx, parser);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return prepend(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode prepend(final XdmNodeTrx wtx, final SubtreeParser parser) throws DocumentException {
    try {
      parser.parse(new SubtreeBuilder(mCollection, wtx, Insert.ASFIRSTCHILD, Collections.emptyList()));
      moveRtx();
      wtx.moveToFirstChild();
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode insertBefore(final Kind kind, final QNm name, final Atomic value) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertBefore((XdmNodeTrx) mRtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode insertBefore(final XdmNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws DocumentException {
    try {
      switch (kind) {
        case DOCUMENT:
          break;
        case ELEMENT:
          wtx.insertElementAsLeftSibling(name);
          break;
        case ATTRIBUTE:
          wtx.insertAttribute(name, value.asStr().stringValue());
          break;
        case NAMESPACE:
          wtx.insertNamespace(name);
          break;
        case TEXT:
          wtx.insertTextAsLeftSibling(value.asStr().stringValue());
          break;
        case COMMENT:
          wtx.insertCommentAsLeftSibling(value.asStr().stringValue());
          break;
        case PROCESSING_INSTRUCTION:
          wtx.insertPIAsLeftSibling(value.asStr().stringValue(), name.getLocalName());
          break;
        default:
          throw new AssertionError(); // Must not happen.
      }
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode insertBefore(final Node<?> node) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertBefore((XdmNodeTrx) mRtx, node);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode insertBefore(final XdmNodeTrx wtx, final Node<?> node) throws DocumentException {
    try {
      if (!(node.getKind() == Kind.ELEMENT))
        return insertBefore(wtx, node.getKind(), node.getName(), node.getValue());
      final SubtreeBuilder builder =
          new SubtreeBuilder(mCollection, wtx, Insert.ASLEFTSIBLING, Collections.emptyList());
      node.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode insertBefore(final SubtreeParser parser) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertBefore((XdmNodeTrx) mRtx, parser);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode insertBefore(final XdmNodeTrx wtx, final SubtreeParser parser) throws DocumentException {
    try {
      final SubtreeBuilder builder =
          new SubtreeBuilder(mCollection, wtx, Insert.ASLEFTSIBLING, Collections.emptyList());
      parser.parse(builder);
      return new DBNode(wtx.moveTo(builder.getStartNodeKey()).get(), mCollection);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public DBNode insertAfter(final Kind kind, final QNm name, final Atomic value) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertAfter((XdmNodeTrx) mRtx, kind, name, value);
      } catch (final SirixException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw new DocumentException(e);
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, kind, name, value);
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  private DBNode insertAfter(final XdmNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws SirixException {
    switch (kind) {
      case DOCUMENT:
        break;
      case ELEMENT:
        wtx.insertElementAsRightSibling(name);
        break;
      case ATTRIBUTE:
        wtx.insertAttribute(name, value.asStr().stringValue());
        break;
      case NAMESPACE:
        wtx.insertNamespace(name);
        break;
      case TEXT:
        wtx.insertTextAsRightSibling(value.asStr().stringValue());
        break;
      case COMMENT:
        wtx.insertCommentAsRightSibling(value.asStr().stringValue());
        break;
      case PROCESSING_INSTRUCTION:
        wtx.insertPIAsRightSibling(value.asStr().stringValue(), name.getLocalName());
        break;
      default:
        throw new AssertionError(); // Must not happen.
    }

    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode insertAfter(final Node<?> node) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertAfter((XdmNodeTrx) mRtx, node);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode insertAfter(final XdmNodeTrx wtx, final Node<?> node) throws DocumentException {
    try {
      if (!(node.getKind() == Kind.ELEMENT))
        return insertAfter(wtx, node.getKind(), node.getName(), node.getValue());

      final SubtreeBuilder builder =
          new SubtreeBuilder(mCollection, wtx, Insert.ASRIGHTSIBLING, Collections.emptyList());
      node.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new DBNode(wtx, mCollection);
  }

  @Override
  public DBNode insertAfter(final SubtreeParser parser) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return insertAfter((XdmNodeTrx) mRtx, parser);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode insertAfter(final XdmNodeTrx wtx, final SubtreeParser parser) throws DocumentException {
    try {
      final SubtreeBuilder builder =
          new SubtreeBuilder(mCollection, wtx, Insert.ASRIGHTSIBLING, Collections.emptyList());
      parser.parse(builder);
      return new DBNode(wtx.moveTo(builder.getStartNodeKey()).get(), mCollection);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public DBNode setAttribute(final Node<?> attribute) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return setAttribute((XdmNodeTrx) mRtx, attribute);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return setAttribute(wtx, attribute);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode setAttribute(final XdmNodeTrx wtx, final Node<?> attribute) throws DocumentException {
    if (wtx.isElement()) {
      final String value = attribute.getValue().asStr().stringValue();
      final QNm name = attribute.getName();
      try {
        wtx.insertAttribute(name, value);
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
      return new DBNode(mRtx, mCollection);
    }
    throw new DocumentException("No element node selected!");
  }

  @Override
  public DBNode setAttribute(final QNm name, final Atomic value) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return setAttribute((XdmNodeTrx) mRtx, name, value);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return setAttribute(wtx, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode setAttribute(final XdmNodeTrx wtx, final QNm name, final Atomic value) throws DocumentException {
    if (wtx.isElement()) {
      try {
        wtx.insertAttribute(name, value.asStr().stringValue());
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
      return new DBNode(mRtx, mCollection);
    }
    throw new DocumentException("No element node selected!");
  }

  @Override
  public boolean deleteAttribute(final QNm name) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return deleteAttribute((XdmNodeTrx) mRtx, name);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return deleteAttribute(wtx, name);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private static boolean deleteAttribute(final XdmNodeTrx wtx, final QNm name) throws DocumentException {
    if (wtx.isElement()) {
      if (wtx.moveToAttributeByName(name).hasMoved()) {
        try {
          wtx.remove();
          return true;
        } catch (final SirixException e) {
          throw new DocumentException(e.getCause());
        }
      }
      throw new DocumentException("No attribute with name " + name + " exists!");
    }
    throw new DocumentException("No element node selected!");
  }

  @Override
  public Stream<DBNode> getAttributes() throws OperationNotSupportedException, DocumentException {
    moveRtx();
    return new SirixStream(new AttributeAxis(mRtx), mCollection);
  }

  @Override
  public DBNode getAttribute(final QNm name) throws DocumentException {
    moveRtx();
    if (mRtx.isElement() && mRtx.moveToAttributeByName(name).hasMoved()) {
      return new DBNode(mRtx, mCollection);
    }
    throw new DocumentException("No element selected!");
  }

  @Override
  public DBNode replaceWith(final Node<?> node) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return replaceWith((XdmNodeTrx) mRtx, node);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return replaceWith(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode replaceWith(final XdmNodeTrx wtx, final Node<?> node) throws DocumentException {
    if (node instanceof DBNode) {
      final DBNode other = (DBNode) node;
      try {
        final XdmNodeReadOnlyTrx rtx = other.getTrx();
        rtx.moveTo(other.getNodeKey());
        wtx.replaceNode(rtx);
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
      return new DBNode(wtx, mCollection);
    } else {
      final SubtreeBuilder builder = createBuilder(wtx);
      node.parse(builder);
      try {
        return replace(builder.getStartNodeKey(), wtx);
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    }
  }

  @Override
  public DBNode replaceWith(final SubtreeParser parser) throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return replaceWith((XdmNodeTrx) mRtx, parser);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return replaceWith(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode replaceWith(final XdmNodeTrx wtx, final SubtreeParser parser) throws DocumentException {
    final SubtreeBuilder builder = createBuilder(wtx);
    parser.parse(builder);
    try {
      return replace(builder.getStartNodeKey(), wtx);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public DBNode replaceWith(final Kind kind, final @Nullable QNm name, final @Nullable Atomic value)
      throws DocumentException {
    if (mIsWtx) {
      try {
        moveRtx();
        return replaceWith((XdmNodeTrx) mRtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XdmNodeTrx) mRtx).rollback();
        mRtx.close();
        throw e;
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(mNodeKey);
        return replaceWith(wtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private DBNode replaceWith(final XdmNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws DocumentException {
    if (wtx.hasLeftSibling()) {
      wtx.moveToLeftSibling();
    } else {
      wtx.moveToParent();
    }

    try {
      final DBNode node = insertAfter(wtx, kind, name, value);
      return replace(node.getNodeKey(), wtx);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  private DBNode replace(final long nodeKey, final XdmNodeTrx wtx) throws SirixException {
    // Move to original node.
    wtx.moveTo(nodeKey).get().moveToRightSibling();
    // Remove original node.
    wtx.remove();
    // Move to subtree root of new subtree.
    wtx.moveTo(nodeKey);

    return new DBNode(mRtx, mCollection);
  }

  private SubtreeBuilder createBuilder(final XdmNodeTrx wtx) throws DocumentException {
    SubtreeBuilder builder = null;
    try {
      if (wtx.hasLeftSibling()) {
        wtx.moveToLeftSibling();
        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASRIGHTSIBLING, Collections.emptyList());
      } else {
        wtx.moveToParent();
        builder = new SubtreeBuilder(mCollection, wtx, Insert.ASFIRSTCHILD, Collections.emptyList());
      }
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return builder;
  }

  /**
   * Get the node key.
   *
   * @return node key
   */
  public long getNodeKey() {
    moveRtx();
    return mNodeKey;
  }

  @Override
  public boolean hasAttributes() throws DocumentException {
    moveRtx();
    return mRtx.getAttributeCount() > 0;
  }

  /**
   * Get the sibling position.
   *
   * @return sibling position
   */
  public int getSiblingPosition() {
    moveRtx();
    int index = 0;
    while (mRtx.hasLeftSibling()) {
      mRtx.moveToLeftSibling();
      index++;
    }
    return index;
  }

  @Override
  public void delete() throws DocumentException {
    if (mIsWtx) {
      moveRtx();
      final XdmNodeTrx wtx = (XdmNodeTrx) mRtx;
      try {
        wtx.remove();
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    } else {
      final XdmNodeTrx wtx = getWtx();
      try {
        wtx.remove();
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  private XdmNodeTrx getWtx() {
    final XdmResourceManager resource = mRtx.getResourceManager();
    final XdmNodeTrx wtx;
    if (resource.hasRunningNodeWriteTrx() && resource.getNodeWriteTrx().isPresent()) {
      wtx = resource.getNodeWriteTrx().get();
    } else {
      wtx = resource.beginNodeWriteTrx();

      if (mRtx.getRevisionNumber() < resource.getMostRecentRevisionNumber())
        wtx.revertTo(mRtx.getRevisionNumber());
    }
    wtx.moveTo(mNodeKey);
    return wtx;
  }

  @Override
  public void parse(final SubtreeHandler handler) throws DocumentException {
    moveRtx();
    final SubtreeParser parser = new NavigationalSubtreeParser(this);
    parser.parse(handler);
  }

  @Override
  protected int cmpInternal(final AbstractTemporalNode<DBNode> otherNode) {
    moveRtx();

    // Are they the same node?
    if (this == otherNode) {
      return 0;
    }

    // Compare collection IDs.
    final int firstCollectionID = mCollection.getID();
    final int secondCollectionID = ((DBCollection) otherNode.getCollection()).getID();
    if (firstCollectionID != secondCollectionID) {
      return firstCollectionID < secondCollectionID
          ? -1
          : 1;
    }

    // Compare document IDs.
    final long firstDocumentID = getTrx().getResourceManager().getResourceConfig().getID();
    final long secondDocumentID = ((DBNode) otherNode).getTrx().getResourceManager().getResourceConfig().getID();
    if (firstDocumentID != secondDocumentID) {
      return firstDocumentID < secondDocumentID
          ? -1
          : 1;
    }

    // Temporal extension.
    final Integer revision = mRtx.getRevisionNumber();
    final Integer otherRevision = ((DBNode) otherNode).mRtx.getRevisionNumber();
    if (revision != otherRevision) {
      return revision.compareTo(otherRevision);
    }

    // Then compare node keys.
    if (mNodeKey == ((DBNode) otherNode).mNodeKey) {
      return 0;
    }

    // If dewey-IDs are present it's simply the comparison of dewey-IDs.
    if (mDeweyID.isPresent()) {
      return mDeweyID.get().compareTo(((DBNode) otherNode).mDeweyID.get());
    }

    try {
      final DBNode firstParent = this.getParent();
      if (firstParent == null) {
        // First node is the root.
        return -1;
      }

      final DBNode secondParent = (DBNode) otherNode.getParent();
      if (secondParent == null) {
        // Second node is the root.
        return +1;
      }

      // Do they have the same parent (common case)?
      if (firstParent.getNodeKey() == secondParent.getNodeKey()) {
        final int cat1 = nodeCategories(this.getKind());
        final int cat2 = nodeCategories(otherNode.getKind());
        if (cat1 == cat2) {
          final DBNode other = (DBNode) otherNode;
          if (cat1 == 1) {
            mRtx.moveToParent();
            for (int i = 0, nspCount = mRtx.getNamespaceCount(); i < nspCount; i++) {
              mRtx.moveToNamespace(i);
              if (mRtx.getNodeKey() == other.mNodeKey) {
                return +1;
              }
              if (mRtx.getNodeKey() == this.mNodeKey) {
                return -1;
              }
              mRtx.moveToParent();
            }
          }
          if (cat1 == 2) {
            mRtx.moveToParent();
            for (int i = 0, attCount = mRtx.getAttributeCount(); i < attCount; i++) {
              mRtx.moveToAttribute(i);
              if (mRtx.getNodeKey() == other.mNodeKey) {
                return +1;
              }
              if (mRtx.getNodeKey() == this.mNodeKey) {
                return -1;
              }
              mRtx.moveToParent();
            }
          }
          return this.getSiblingPosition() - ((DBNode) otherNode).getSiblingPosition();
        } else {
          return cat1 - cat2;
        }
      }

      // Find the depths of both nodes in the tree.
      int depth1 = 0;
      int depth2 = 0;
      DBNode p1 = this;
      DBNode p2 = (DBNode) otherNode;
      while (p1 != null) {
        depth1++;
        p1 = p1.getParent();
      }
      while (p2 != null) {
        depth2++;
        p2 = p2.getParent();
      }

      // Move up one branch of the tree so we have two nodes on the same level.
      p1 = this;
      while (depth1 > depth2) {
        p1 = p1.getParent();
        assert p1 != null;
        if (p1.getNodeKey() == ((DBNode) otherNode).getNodeKey()) {
          return +1;
        }
        depth1--;
      }

      p2 = ((DBNode) otherNode);
      while (depth2 > depth1) {
        p2 = p2.getParent();
        assert p2 != null;
        if (p2.getNodeKey() == this.getNodeKey()) {
          return -1;
        }
        depth2--;
      }

      // Now move up both branches in sync until we find a common parent.
      while (true) {
        final DBNode par1 = p1.getParent();
        final DBNode par2 = p2.getParent();
        if (par1 == null || par2 == null) {
          throw new NullPointerException("Node order comparison - internal error");
        }
        if (par1.getNodeKey() == par2.getNodeKey()) {
          if (p1.getKind() == Kind.ATTRIBUTE && p2.getKind() != Kind.ATTRIBUTE) {
            return -1; // attributes first
          }
          if (p1.getKind() != Kind.ATTRIBUTE && p2.getKind() == Kind.ATTRIBUTE) {
            return +1; // attributes first
          }
          return p1.getSiblingPosition() - p2.getSiblingPosition();
        }
        p1 = par1;
        p2 = par2;
      }
    } catch (final DocumentException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return 0;
  }

  /**
   * Determine node category.
   *
   * @param kind node kind
   * @return category number
   */
  private static int nodeCategories(final Kind kind) {
    switch (kind) {
      case DOCUMENT:
        return 0;
      case COMMENT:
      case PROCESSING_INSTRUCTION:
      case TEXT:
      case ELEMENT:
        return 3;
      case ATTRIBUTE:
        return 2;
      case NAMESPACE:
        return 1;
      default:
        throw new IllegalStateException("Node kind not known!");
    }
  }

  @Override
  public int hashCode() {
    moveRtx();
    return Objects.hash(mRtx.getNodeKey(), mRtx.getValue(), mRtx.getName());
  }

  @Override
  public String toString() {
    moveRtx();
    return MoreObjects.toStringHelper(this).add("rtx", mRtx).toString();
  }

  @Override
  public Stream<? extends Node<?>> performStep(final org.brackit.xquery.xdm.Axis axis, final NodeType test)
      throws DocumentException {
    return null;
  }

  @Override
  public DBNode getNext() {
    moveRtx();
    final AbstractTemporalAxis<XdmNodeReadOnlyTrx> axis = new NextAxis<>(mRtx);
    return axis.hasNext()
        ? new DBNode(axis.getTrx(), mCollection)
        : null;
  }

  @Override
  public DBNode getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<XdmNodeReadOnlyTrx> axis = new PreviousAxis<>(mRtx);
    return axis.hasNext()
        ? new DBNode(axis.getTrx(), mCollection)
        : null;
  }

  @Override
  public DBNode getFirst() {
    moveRtx();
    final AbstractTemporalAxis<XdmNodeReadOnlyTrx> axis = new FirstAxis<>(mRtx);
    return axis.hasNext()
        ? new DBNode(axis.getTrx(), mCollection)
        : null;
  }

  @Override
  public DBNode getLast() {
    moveRtx();
    final AbstractTemporalAxis<XdmNodeReadOnlyTrx> axis = new LastAxis<>(mRtx);
    return axis.hasNext()
        ? new DBNode(axis.getTrx(), mCollection)
        : null;
  }

  @Override
  public Stream<AbstractTemporalNode<DBNode>> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixStream(new PastAxis<>(mRtx, include), mCollection);
  }

  @Override
  public Stream<AbstractTemporalNode<DBNode>> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixStream(new FutureAxis<>(mRtx, include), mCollection);
  }

  @Override
  public Stream<AbstractTemporalNode<DBNode>> getAllTime() {
    moveRtx();
    return new TemporalSirixStream(new AllTimeAxis<>(mRtx), mCollection);
  }

  @Override
  public boolean isNextOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    return otherNode.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final TemporalNode<?> other) {
    moveRtx();

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    final NodeReadTrx otherTrx = otherNode.getTrx();

    return otherTrx.getResourceManager().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final TemporalNode<?> other) {
    moveRtx();

    if (!(other instanceof DBNode))
      return false;

    final DBNode otherNode = (DBNode) other;
    final NodeReadTrx otherTrx = otherNode.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  /**
   * Get the path class record (PCR).
   *
   * @return the path class record
   *
   * @throws SirixException
   */
  public long getPCR() throws SirixException {
    return mRtx.getPathNodeKey();
  }

  /**
   * Get the DeweyID associated with this node (if any).
   *
   * @return an optional DeweyID (might be absent, depending on the {@link BasicDBStore}
   *         configuration)
   */
  public Optional<SirixDeweyID> getDeweyID() {
    return mRtx.getDeweyID();
  }
}
