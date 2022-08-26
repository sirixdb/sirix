package org.sirix.xquery.node;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Una;
import org.brackit.xquery.node.parser.NavigationalSubtreeParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Kind;
import org.brackit.xquery.xdm.Scope;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.node.AbstractTemporalNode;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.node.TemporalNode;
import org.brackit.xquery.xdm.type.NodeType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.axis.*;
import org.sirix.axis.temporal.*;
import org.sirix.exception.SirixException;
import org.sirix.node.SirixDeweyID;
import org.sirix.service.InsertPosition;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.node.SirixNodeStream;
import org.sirix.xquery.stream.node.TemporalSirixNodeStream;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

/**
 * A node which is used to provide all XDM functionality as well as temporal functions.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class XmlDBNode extends AbstractTemporalNode<XmlDBNode> implements StructuredDBItem<XmlNodeReadOnlyTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(XmlDBNode.class));

  /** Sirix {@link XmlNodeReadOnlyTrx}. */
  private final XmlNodeReadOnlyTrx rtx;

  /** Sirix node key. */
  private final long nodeKey;

  /** Kind of node. */
  private final org.sirix.node.NodeKind kind;

  /** Collection this node is part of. */
  private final XmlDBCollection collection;

  /** Determines if write-transaction is present. */
  private final boolean isWtx;

  /** {@link Scope} of node. */
  private SirixScope scope;

  /**
   * Constructor.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link XmlDBCollection} reference
   */
  public XmlDBNode(final XmlNodeReadOnlyTrx rtx, final XmlDBCollection collection) {
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);
    isWtx = this.rtx instanceof XmlNodeTrx;
    nodeKey = this.rtx.getNodeKey();
    kind = this.rtx.getKind();
    deweyID = this.rtx.getNode().getDeweyID();
  }

  /** Optional dewey ID. */
  private final SirixDeweyID deweyID;

  /**
   * Create a new {@link NodeReadOnlyTrx} and move to node key.
   */
  private void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  /**
   * Get underlying node.
   *
   * @return underlying node
   */
  private org.sirix.node.interfaces.immutable.ImmutableNode getImmutableNode() {
    moveRtx();
    return rtx.getNode();
  }

  @Override
  public boolean isSelfOf(final Node<?> other) {
    moveRtx();
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      return node.getImmutableNode().getNodeKey() == this.getImmutableNode().getNodeKey();
    }
    return false;
  }

  @Override
  public boolean isParentOf(final Node<?> other) {
    moveRtx();
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      return node.getImmutableNode().getParentKey() == rtx.getNodeKey();
    }
    return false;
  }

  @Override
  public boolean isChildOf(final Node<?> other) {
    moveRtx();
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        return node.getImmutableNode().getNodeKey() == rtx.getParentKey();
      }
    }
    return false;
  }

  @Override
  public boolean isDescendantOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      moveRtx();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        if (deweyID != null) {
          return deweyID.isDescendantOf(node.deweyID);
        } else {
          for (final Axis axis = new AncestorAxis(rtx); axis.hasNext();) {
            axis.nextLong();
            if (node.getImmutableNode().getNodeKey() == rtx.getNodeKey()) {
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
  @Override
  public XmlNodeReadOnlyTrx getTrx() {
    moveRtx();
    return rtx;
  }

  @Override
  public boolean isDescendantOrSelfOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      if (isSelfOf(other)) {
        retVal = true;
      } else {
        retVal = isDescendantOf(other);
      }
    }
    return retVal;
  }

  @Override
  public boolean isAncestorOf(final Node<?> other) {
    moveRtx();
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      if (deweyID != null) {
        return deweyID.isAncestorOf(node.deweyID);
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
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      if (deweyID != null) {
        retVal = deweyID.isAncestorOf(node.deweyID);
      } else {
        if (isSelfOf(other)) {
          retVal = true;
        } else {
          retVal = other.isDescendantOf(this);
        }
      }
    }
    return retVal;
  }

  @Override
  public boolean isSiblingOf(final Node<?> other) {
    moveRtx();
    boolean retVal = false;
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      try {
        if (deweyID != null) {
          return deweyID.isSiblingOf(node.deweyID);
        }
        //noinspection ConstantConditions
        if (node.getKind() != Kind.NAMESPACE && node.getKind() != Kind.ATTRIBUTE
            && node.getParent().getImmutableNode().getNodeKey() == ((XmlDBNode) other.getParent()).getImmutableNode()
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
    if (other instanceof XmlDBNode node) {
      moveRtx();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        if (deweyID != null) {
          return deweyID.isPrecedingSiblingOf(node.deweyID);
        } else {
          while (rtx.hasRightSibling()) {
            rtx.moveToRightSibling();
            if (rtx.getNodeKey() == node.getNodeKey()) {
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
    if (other instanceof XmlDBNode node) {
      moveRtx();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        if (deweyID != null) {
          return deweyID.isFollowingSiblingOf(node.deweyID);
        } else {
          while (rtx.hasLeftSibling()) {
            rtx.moveToLeftSibling();
            if (rtx.getNodeKey() == node.getNodeKey()) {
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
    if (other instanceof XmlDBNode node) {
      moveRtx();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        if (deweyID != null) {
          return deweyID.isPrecedingOf(node.deweyID);
        } else {
          for (final Axis axis = new FollowingAxis(rtx); axis.hasNext();) {
            axis.nextLong();
            if (rtx.getNodeKey() == node.getNodeKey()) {
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
    if (other instanceof XmlDBNode node) {
      moveRtx();
      if (kind != org.sirix.node.NodeKind.ATTRIBUTE && kind != org.sirix.node.NodeKind.NAMESPACE) {
        if (deweyID != null) {
          return deweyID.isFollowingOf(node.deweyID);
        } else {
          for (final Axis axis = new PrecedingAxis(rtx); axis.hasNext();) {
            axis.nextLong();
            if (rtx.getNodeKey() == node.getNodeKey()) {
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
    if (other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      try {
        //noinspection ConstantConditions
        if (getParent().getImmutableNode().getNodeKey() == node.getImmutableNode().getNodeKey()) {
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
    if (getKind() == Kind.DOCUMENT && other instanceof XmlDBNode node) {
      assert node.getNodeClassID() == this.getNodeClassID();
      final NodeReadOnlyTrx rtx = node.getTrx();
      if (rtx.getRevisionNumber() == this.rtx.getRevisionNumber()
          && rtx.getResourceSession().getResourceConfig().getID() == this.rtx.getResourceSession()
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
    return rtx.getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isRoot() {
    moveRtx();
    return rtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty();
  }

  @Override
  public int getNodeClassID() {
    return 1732483;
  }

  @Override
  public XmlDBCollection getCollection() {
    return collection;
  }

  @Override
  public Scope getScope() {
    if (scope == null && kind == org.sirix.node.NodeKind.ELEMENT) {
      scope = new SirixScope(this);
    }
    return scope;
  }

  @Override
  public Kind getKind() {
    moveRtx();
    // $CASES-OMITTED$
    return switch (rtx.getKind()) {
      case XML_DOCUMENT -> Kind.DOCUMENT;
      case ELEMENT -> Kind.ELEMENT;
      case TEXT -> Kind.TEXT;
      case COMMENT -> Kind.COMMENT;
      case PROCESSING_INSTRUCTION -> Kind.PROCESSING_INSTRUCTION;
      case NAMESPACE -> Kind.NAMESPACE;
      case ATTRIBUTE -> Kind.ATTRIBUTE;
      default -> throw new IllegalStateException("Kind not known!");
    };
  }

  @Override
  public QNm getName() {
    moveRtx();
    return rtx.getName();
  }

  @Override
  public void setName(final QNm name) throws DocumentException {
    if (isWtx) {
      moveRtx();
      final XmlNodeTrx wtx = (XmlNodeTrx) rtx;
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
      final XmlNodeTrx wtx = getWtx();
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
  public Atomic getValue() {
    moveRtx();

    // $CASES-OMITTED$
    final String value = switch (kind) {
      case XML_DOCUMENT, ELEMENT -> expandString();
      case ATTRIBUTE, COMMENT, PROCESSING_INSTRUCTION -> emptyIfNull(rtx.getValue());
      case TEXT -> rtx.getValue();
      default -> "";
    };
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
    final Axis axis = new DescendantAxis(rtx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (rtx.isText()) {
        buffer.append(rtx.getValue());
      }
    }
    return buffer.toString();
  }

  @Override
  public void setValue(final Atomic value) throws DocumentException {
    moveRtx();
    if (!rtx.isValueNode()) {
      throw new DocumentException("Node has no value!");
    }
    if (isWtx) {
      final XmlNodeTrx wtx = (XmlNodeTrx) rtx;
      try {
        wtx.setValue(value.stringValue());
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
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
  public XmlDBNode getParent() {
    moveRtx();
    if (rtx.hasParent()) {
      rtx.moveToParent();
      return new XmlDBNode(rtx, collection);
    }
    return null;
  }

  @Override
  public XmlDBNode getFirstChild() {
    moveRtx();
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      return new XmlDBNode(rtx, collection);
    }
    return null;
  }

  @Override
  public XmlDBNode getLastChild() {
    moveRtx();
    if (rtx.hasLastChild()) {
      rtx.moveToLastChild();
      return new XmlDBNode(rtx, collection);
    }
    return null;
  }

  @Override
  public Stream<XmlDBNode> getChildren() {
    moveRtx();
    return new SirixNodeStream(new ChildAxis(rtx), collection);
  }

  // Returns all nodes in the subtree _including_ the subtree root.
  @Override
  public Stream<XmlDBNode> getSubtree() {
    moveRtx();
    return new SirixNodeStream(new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)), collection);
  }

  @Override
  public boolean hasChildren() {
    moveRtx();
    return rtx.getChildCount() > 0;
  }

  @Override
  public XmlDBNode getNextSibling() {
    moveRtx();
    if (rtx.hasRightSibling()) {
      rtx.moveToRightSibling();
      return new XmlDBNode(rtx, collection);
    }
    return null;
  }

  @Override
  public XmlDBNode getPreviousSibling() {
    moveRtx();
    if (rtx.hasLeftSibling()) {
      rtx.moveToLeftSibling();
      return new XmlDBNode(rtx, collection);
    }
    return null;
  }

  @Override
  public XmlDBNode append(final Kind kind, final QNm name, final Atomic value) {
    if (isWtx) {
      moveRtx();
      final XmlNodeTrx wtx = (XmlNodeTrx) rtx;
      try {
        return append(wtx, kind, name, value);
      } catch (final SirixException e) {
        wtx.close();
        throw new DocumentException(e);
      }
    } else {
      try (final XmlNodeTrx wtx = getWtx()) {
        return append(wtx, kind, name, value);
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
    }
  }

  private XmlDBNode append(final XmlNodeTrx wtx, final Kind kind, final QNm name, final Atomic value) {
    if (wtx.hasFirstChild()) {
      wtx.moveToLastChild();
      insertNodeAsRightSibling(wtx, kind, name, value);
    } else {
      insertNodeAsFirstChild(wtx, kind, name, value);
    }

    return new XmlDBNode(wtx, collection);
  }

  private void insertNodeAsRightSibling(XmlNodeTrx wtx, Kind kind, QNm name, Atomic value) {
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
  }

  private void insertNodeAsFirstChild(XmlNodeTrx wtx, Kind kind, QNm name, Atomic value) {
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

  @Override
  public XmlDBNode append(final Node<?> child) {
    if (isWtx) {
      moveRtx();
      try {
        return append((XmlNodeTrx) rtx, child);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return append(wtx, child);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode append(final XmlNodeTrx wtx, final Node<?> child) {
    try {
      if (!(child.getKind() == Kind.ELEMENT))
        return append(wtx, child.getKind(), child.getName(), child.getValue());

      final SubtreeBuilder builder;

      if (wtx.hasFirstChild()) {
        wtx.moveToLastChild();

        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_RIGHT_SIBLING, Collections.emptyList());
      } else {
        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());
      }
      child.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode append(final SubtreeParser parser) {
    if (isWtx) {
      try {
        moveRtx();
        return append(rtx, parser);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return append(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode append(final XmlNodeReadOnlyTrx rtx, final SubtreeParser parser) {
    try {
      if (rtx.hasFirstChild()) {
        rtx.moveToLastChild();
      }

      parser.parse(
          new SubtreeBuilder(collection, (XmlNodeTrx) rtx, InsertPosition.AS_RIGHT_SIBLING, Collections.emptyList()));

      moveRtx();
      rtx.moveToFirstChild();
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new XmlDBNode(rtx, collection);
  }

  @Override
  public XmlDBNode prepend(final Kind kind, final QNm name, final Atomic value) {
    if (isWtx) {
      try {
        moveRtx();
        return prepend((XmlNodeTrx) rtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return prepend((XmlNodeTrx) rtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode prepend(final XmlNodeTrx wtx, final Kind kind, final QNm name, final Atomic value) {
    try {
      insertNodeAsFirstChild(wtx, kind, name, value);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode prepend(final Node<?> child) {
    if (isWtx) {
      try {
        moveRtx();
        return prepend((XmlNodeTrx) rtx, child);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return prepend(wtx, child);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode prepend(final XmlNodeTrx wtx, final Node<?> child) {
    try {
      if (!(child.getKind() == Kind.ELEMENT))
        return prepend(wtx, child.getKind(), child.getName(), child.getValue());

      SubtreeBuilder builder;
      if (wtx.hasFirstChild()) {
        wtx.moveToFirstChild();

        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_LEFT_SIBLING, Collections.emptyList());
      } else {
        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());
      }
      child.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode prepend(final SubtreeParser parser) {
    if (isWtx) {
      try {
        moveRtx();
        return prepend((XmlNodeTrx) rtx, parser);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return prepend(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode prepend(final XmlNodeTrx wtx, final SubtreeParser parser) {
    try {
      parser.parse(new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList()));
      moveRtx();
      wtx.moveToFirstChild();
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode insertBefore(final Kind kind, final QNm name, final Atomic value) {
    if (isWtx) {
      try {
        moveRtx();
        return insertBefore((XmlNodeTrx) rtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode insertBefore(final XmlNodeTrx wtx, final Kind kind, final QNm name, final Atomic value) {
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

    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode insertBefore(final Node<?> node) {
    if (isWtx) {
      try {
        moveRtx();
        return insertBefore((XmlNodeTrx) rtx, node);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode insertBefore(final XmlNodeTrx wtx, final Node<?> node) {
    try {
      if (!(node.getKind() == Kind.ELEMENT))
        return insertBefore(wtx, node.getKind(), node.getName(), node.getValue());
      final SubtreeBuilder builder =
          new SubtreeBuilder(collection, wtx, InsertPosition.AS_LEFT_SIBLING, Collections.emptyList());
      node.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }

    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode insertBefore(final SubtreeParser parser) {
    if (isWtx) {
      try {
        moveRtx();
        return insertBefore((XmlNodeTrx) rtx, parser);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertBefore(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode insertBefore(final XmlNodeTrx wtx, final SubtreeParser parser) {
    try {
      final SubtreeBuilder builder =
          new SubtreeBuilder(collection, wtx, InsertPosition.AS_LEFT_SIBLING, Collections.emptyList());
      parser.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
      return new XmlDBNode(wtx, collection);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public XmlDBNode insertAfter(final Kind kind, final QNm name, final Atomic value) {
    if (isWtx) {
      try {
        moveRtx();
        return insertAfter((XmlNodeTrx) rtx, kind, name, value);
      } catch (final SirixException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw new DocumentException(e);
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, kind, name, value);
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  private XmlDBNode insertAfter(final XmlNodeTrx wtx, final Kind kind, final QNm name, final Atomic value)
      throws SirixException {
    insertNodeAsRightSibling(wtx, kind, name, value);

    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode insertAfter(final Node<?> node) {
    if (isWtx) {
      try {
        moveRtx();
        return insertAfter((XmlNodeTrx) rtx, node);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode insertAfter(final XmlNodeTrx wtx, final Node<?> node) {
    try {
      if (!(node.getKind() == Kind.ELEMENT))
        return insertAfter(wtx, node.getKind(), node.getName(), node.getValue());

      final SubtreeBuilder builder =
          new SubtreeBuilder(collection, wtx, InsertPosition.AS_RIGHT_SIBLING, Collections.emptyList());
      node.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
    return new XmlDBNode(wtx, collection);
  }

  @Override
  public XmlDBNode insertAfter(final SubtreeParser parser) {
    if (isWtx) {
      try {
        moveRtx();
        return insertAfter((XmlNodeTrx) rtx, parser);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        return insertAfter(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode insertAfter(final XmlNodeTrx wtx, final SubtreeParser parser) {
    try {
      final SubtreeBuilder builder =
          new SubtreeBuilder(collection, wtx, InsertPosition.AS_RIGHT_SIBLING, Collections.emptyList());
      parser.parse(builder);
      wtx.moveTo(builder.getStartNodeKey());
      return new XmlDBNode(wtx, collection);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public XmlDBNode setAttribute(final Node<?> attribute) {
    if (isWtx) {
      try {
        moveRtx();
        return setAttribute((XmlNodeTrx) rtx, attribute);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return setAttribute(wtx, attribute);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode setAttribute(final XmlNodeTrx wtx, final Node<?> attribute) {
    if (wtx.isElement()) {
      final String value = attribute.getValue().asStr().stringValue();
      final QNm name = attribute.getName();
      try {
        wtx.insertAttribute(name, value);
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
      return new XmlDBNode(rtx, collection);
    }
    throw new DocumentException("No element node selected!");
  }

  @Override
  public XmlDBNode setAttribute(final QNm name, final Atomic value) {
    if (isWtx) {
      try {
        moveRtx();
        return setAttribute((XmlNodeTrx) rtx, name, value);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return setAttribute(wtx, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode setAttribute(final XmlNodeTrx wtx, final QNm name, final Atomic value) {
    if (wtx.isElement()) {
      try {
        wtx.insertAttribute(name, value.asStr().stringValue());
      } catch (final SirixException e) {
        throw new DocumentException(e);
      }
      return new XmlDBNode(rtx, collection);
    }
    throw new DocumentException("No element node selected!");
  }

  @Override
  public boolean deleteAttribute(final QNm name) {
    if (isWtx) {
      try {
        moveRtx();
        return deleteAttribute((XmlNodeTrx) rtx, name);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return deleteAttribute(wtx, name);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private static boolean deleteAttribute(final XmlNodeTrx wtx, final QNm name) {
    if (wtx.isElement()) {
      if (wtx.moveToAttributeByName(name)) {
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
  public Stream<XmlDBNode> getAttributes() throws DocumentException {
    moveRtx();
    return new SirixNodeStream(new AttributeAxis(rtx), collection);
  }

  @Override
  public XmlDBNode getAttribute(final QNm name) {
    moveRtx();
    if (rtx.isElement() && rtx.moveToAttributeByName(name)) {
      return new XmlDBNode(rtx, collection);
    }
    throw new DocumentException("No element selected!");
  }

  @Override
  public XmlDBNode replaceWith(final Node<?> node) {
    if (isWtx) {
      try {
        moveRtx();
        return replaceWith((XmlNodeTrx) rtx, node);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return replaceWith(wtx, node);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode replaceWith(final XmlNodeTrx wtx, final Node<?> node) {
    if (node instanceof XmlDBNode other) {
      try {
        final XmlNodeReadOnlyTrx rtx = other.getTrx();
        rtx.moveTo(other.getNodeKey());
        wtx.replaceNode(rtx);
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
      return new XmlDBNode(wtx, collection);
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
  public XmlDBNode replaceWith(final SubtreeParser parser) {
    if (isWtx) {
      try {
        moveRtx();
        return replaceWith((XmlNodeTrx) rtx, parser);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return replaceWith(wtx, parser);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode replaceWith(final XmlNodeTrx wtx, final SubtreeParser parser) {
    final SubtreeBuilder builder = createBuilder(wtx);
    parser.parse(builder);
    try {
      return replace(builder.getStartNodeKey(), wtx);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  @Override
  public XmlDBNode replaceWith(final Kind kind, final @Nullable QNm name, final @Nullable Atomic value) {
    if (isWtx) {
      try {
        moveRtx();
        return replaceWith((XmlNodeTrx) rtx, kind, name, value);
      } catch (final DocumentException e) {
        ((XmlNodeTrx) rtx).rollback();
        rtx.close();
        throw e;
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.moveTo(nodeKey);
        return replaceWith(wtx, kind, name, value);
      } catch (final DocumentException e) {
        wtx.rollback();
        wtx.close();
        throw e;
      }
    }
  }

  private XmlDBNode replaceWith(final XmlNodeTrx wtx, final Kind kind, final QNm name, final Atomic value) {
    if (wtx.hasLeftSibling()) {
      wtx.moveToLeftSibling();
    } else {
      wtx.moveToParent();
    }

    try {
      final XmlDBNode node = insertAfter(wtx, kind, name, value);
      return replace(node.getNodeKey(), wtx);
    } catch (final SirixException e) {
      throw new DocumentException(e);
    }
  }

  private XmlDBNode replace(final long nodeKey, final XmlNodeTrx wtx) throws SirixException {
    // Move to original node.
    wtx.moveTo(nodeKey);
    wtx.moveToRightSibling();
    // Remove original node.
    wtx.remove();
    // Move to subtree root of new subtree.
    wtx.moveTo(nodeKey);

    return new XmlDBNode(rtx, collection);
  }

  private SubtreeBuilder createBuilder(final XmlNodeTrx wtx) {
    SubtreeBuilder builder;
    try {
      if (wtx.hasLeftSibling()) {
        wtx.moveToLeftSibling();
        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_RIGHT_SIBLING, Collections.emptyList());
      } else {
        wtx.moveToParent();
        builder = new SubtreeBuilder(collection, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());
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
    return nodeKey;
  }

  @Override
  public boolean hasAttributes() {
    moveRtx();
    return rtx.getAttributeCount() > 0;
  }

  /**
   * Get the sibling position.
   *
   * @return sibling position
   */
  public int getSiblingPosition() {
    moveRtx();
    int index = 0;
    while (rtx.hasLeftSibling()) {
      rtx.moveToLeftSibling();
      index++;
    }
    return index;
  }

  @Override
  public void delete() {
    if (isWtx) {
      moveRtx();
      final XmlNodeTrx wtx = (XmlNodeTrx) rtx;
      try {
        wtx.remove();
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    } else {
      final XmlNodeTrx wtx = getWtx();
      try {
        wtx.remove();
      } catch (final SirixException e) {
        wtx.rollback();
        wtx.close();
        throw new DocumentException(e);
      }
    }
  }

  private XmlNodeTrx getWtx() {
    final XmlResourceSession resource = rtx.getResourceSession();
    final XmlNodeTrx wtx;
    if (resource.hasRunningNodeWriteTrx() && resource.getNodeTrx().isPresent()) {
      wtx = resource.getNodeTrx().get();
    } else {
      wtx = resource.beginNodeTrx();

      if (rtx.getRevisionNumber() < resource.getMostRecentRevisionNumber())
        wtx.revertTo(rtx.getRevisionNumber());
    }
    wtx.moveTo(nodeKey);
    return wtx;
  }

  @Override
  public void parse(final SubtreeHandler handler) {
    moveRtx();
    final SubtreeParser parser = new NavigationalSubtreeParser(this);
    parser.parse(handler);
  }

  @Override
  protected int cmpInternal(final AbstractTemporalNode<XmlDBNode> otherNode) {
    moveRtx();

    // Are they the same node?
    if (this == otherNode) {
      return 0;
    }

    // Compare collection IDs.
    final int firstCollectionID = collection.getID();
    final int secondCollectionID = ((XmlDBCollection) otherNode.getCollection()).getID();
    if (firstCollectionID != secondCollectionID) {
      return firstCollectionID < secondCollectionID
          ? -1
          : 1;
    }

    // Compare document IDs.
    final long firstDocumentID = getTrx().getResourceSession().getResourceConfig().getID();
    final long secondDocumentID = ((XmlDBNode) otherNode).getTrx().getResourceSession().getResourceConfig().getID();
    if (firstDocumentID != secondDocumentID) {
      return firstDocumentID < secondDocumentID
          ? -1
          : 1;
    }

    // Temporal extension.
    final Integer revision = rtx.getRevisionNumber();
    final int otherRevision = ((XmlDBNode) otherNode).rtx.getRevisionNumber();
    if (revision != otherRevision) {
      return revision.compareTo(otherRevision);
    }

    // Then compare node keys.
    if (nodeKey == ((XmlDBNode) otherNode).nodeKey) {
      return 0;
    }

    // If dewey-IDs are present it's simply the comparison of dewey-IDs.
    if (deweyID != null && ((XmlDBNode) otherNode).deweyID != null) {
      return deweyID.compareTo(((XmlDBNode) otherNode).deweyID);
    }

    try {
      final XmlDBNode firstParent = this.getParent();
      if (firstParent == null) {
        // First node is the root.
        return -1;
      }

      final XmlDBNode secondParent = (XmlDBNode) otherNode.getParent();
      if (secondParent == null) {
        // Second node is the root.
        return +1;
      }

      // Do they have the same parent (common case)?
      if (firstParent.getNodeKey() == secondParent.getNodeKey()) {
        final int cat1 = nodeCategories(this.getKind());
        final int cat2 = nodeCategories(otherNode.getKind());
        if (cat1 == cat2) {
          final XmlDBNode other = (XmlDBNode) otherNode;
          if (cat1 == 1) {
            rtx.moveToParent();
            for (int i = 0, nspCount = rtx.getNamespaceCount(); i < nspCount; i++) {
              rtx.moveToNamespace(i);
              if (rtx.getNodeKey() == other.nodeKey) {
                return +1;
              }
              if (rtx.getNodeKey() == this.nodeKey) {
                return -1;
              }
              rtx.moveToParent();
            }
          }
          if (cat1 == 2) {
            rtx.moveToParent();
            for (int i = 0, attCount = rtx.getAttributeCount(); i < attCount; i++) {
              rtx.moveToAttribute(i);
              if (rtx.getNodeKey() == other.nodeKey) {
                return +1;
              }
              if (rtx.getNodeKey() == this.nodeKey) {
                return -1;
              }
              rtx.moveToParent();
            }
          }
          return this.getSiblingPosition() - ((XmlDBNode) otherNode).getSiblingPosition();
        } else {
          return cat1 - cat2;
        }
      }

      // Find the depths of both nodes in the tree.
      int depth1 = 0;
      int depth2 = 0;
      XmlDBNode p1 = this;
      XmlDBNode p2 = (XmlDBNode) otherNode;
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
        if (p1.getNodeKey() == ((XmlDBNode) otherNode).getNodeKey()) {
          return +1;
        }
        depth1--;
      }

      p2 = ((XmlDBNode) otherNode);
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
        final XmlDBNode par1 = p1.getParent();
        final XmlDBNode par2 = p2.getParent();
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
    return switch (kind) {
      case DOCUMENT -> 0;
      case COMMENT, PROCESSING_INSTRUCTION, TEXT, ELEMENT -> 3;
      case ATTRIBUTE -> 2;
      case NAMESPACE -> 1;
    };
  }

  @Override
  public int hashCode() {
    moveRtx();
    return Objects.hash(rtx.getNodeKey(), rtx.getValue(), rtx.getName());
  }

  @Override
  public String toString() {
    moveRtx();
    return MoreObjects.toStringHelper(this).add("rtx", rtx).toString();
  }

  @Override
  public Stream<? extends Node<?>> performStep(final org.brackit.xquery.xdm.Axis axis, final NodeType test) {
    return null;
  }

  @Override
  public XmlDBNode getNext() {
    moveRtx();

    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new NextAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  private XmlDBNode moveTemporalAxis(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis) {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new XmlDBNode(rtx, collection);
    }

    return null;
  }

  @Override
  public XmlDBNode getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
        new PreviousAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public XmlDBNode getFirst() {
    moveRtx();
    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new FirstAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public XmlDBNode getLast() {
    moveRtx();
    final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis = new LastAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public Stream<AbstractTemporalNode<XmlDBNode>> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixNodeStream(new PastAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<AbstractTemporalNode<XmlDBNode>> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixNodeStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<AbstractTemporalNode<XmlDBNode>> getAllTime() {
    moveRtx();
    return new TemporalSirixNodeStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
  }

  @Override
  public boolean isNextOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final TemporalNode<?> other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    return otherNode.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final TemporalNode<?> other) {
    moveRtx();

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    return otherTrx.getResourceSession().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final TemporalNode<?> other) {
    moveRtx();

    if (!(other instanceof XmlDBNode otherNode))
      return false;

    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  /**
   * Get the path class record (PCR).
   *
   * @return the path class record
   * @throws SirixException if Sirix fails to get the path class record
   */
  public long getPCR() {
    return rtx.getPathNodeKey();
  }

  /**
   * Get the DeweyID associated with this node (if any).
   *
   * @return an optional DeweyID (might be absent, depending on the {@link BasicXmlDBStore}
   *         configuration)
   */
  public SirixDeweyID getDeweyID() {
    return rtx.getDeweyID();
  }
}
