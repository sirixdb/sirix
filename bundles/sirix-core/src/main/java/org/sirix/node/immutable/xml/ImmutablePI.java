package org.sirix.node.immutable.xml;

import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.PINode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable processing instruction node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutablePI implements ImmutableValueNode, ImmutableNameNode, ImmutableStructNode, ImmutableXmlNode {
  /** Mutable {@link PINode}. */
  private final PINode node;

  /**
   * Private constructor.
   *
   * @param node {@link PINode} to wrap
   */
  private ImmutablePI(final PINode node) {
    this.node = requireNonNull(node);
  }

  /**
   * Get an immutable processing instruction node instance.
   *
   * @param node the mutable {@link PINode} to wrap
   * @return immutable processing instruction node instance
   */
  public static ImmutablePI of(final PINode node) {
    return new ImmutablePI(node);
  }

  @Override
  public int getTypeKey() {
    return node.getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return node.isSameItem(other);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public long getHash() {
    return node.getHash();
  }

  @Override
  public long getParentKey() {
    return node.getParentKey();
  }

  @Override
  public boolean hasParent() {
    return node.hasParent();
  }

  @Override
  public long getNodeKey() {
    return node.getNodeKey();
  }

  @Override
  public NodeKind getKind() {
    return node.getKind();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return node.getPreviousRevisionNumber();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return node.getLastModifiedRevisionNumber();
  }

  @Override
  public boolean hasFirstChild() {
    return node.hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public boolean hasLeftSibling() {
    return node.hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return node.hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return node.getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return node.getDescendantCount();
  }

  @Override
  public long getFirstChildKey() {
    return node.getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLeftSiblingKey() {
    return node.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return node.getRightSiblingKey();
  }

  @Override
  public int getLocalNameKey() {
    return node.getLocalNameKey();
  }

  @Override
  public int getPrefixKey() {
    return node.getPrefixKey();
  }

  @Override
  public int getURIKey() {
    return node.getURIKey();
  }

  @Override
  public long getPathNodeKey() {
    return node.getPathNodeKey();
  }

  @Override
  public byte[] getRawValue() {
    return node.getRawValue();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return node.getDeweyID();
  }

  @Override
  public boolean equals(Object obj) {
    return node.equals(obj);
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  @Override
  public String toString() {
    return node.toString();
  }

  @Override
  public QNm getName() {
    return node.getName();
  }

  @Override
  public String getValue() {
    return node.getValue();
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return node.getDeweyIDAsBytes();
  }
}
