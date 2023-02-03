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
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.NamespaceNode;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable namespace node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableNamespace implements ImmutableNameNode, ImmutableXmlNode {

  /** Mutable {@link NamespaceNode}. */
  private final NamespaceNode node;

  /**
   * Private constructor.
   *
   * @param node {@link NamespaceNode} to wrap
   */
  private ImmutableNamespace(final NamespaceNode node) {
    this.node = checkNotNull(node);
  }

  /**
   * Get an immutable namespace node instance.
   *
   * @param node the mutable {@link NamespaceNode} to wrap
   * @return immutable namespace node instance
   */
  public static ImmutableNamespace of(final NamespaceNode node) {
    return new ImmutableNamespace(node);
  }

  @Override
  public int getTypeKey() {
    return node.getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node pOther) {
    return node.isSameItem(pOther);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor pVisitor) {
    return pVisitor.visit(this);
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
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return node.getDeweyIDAsBytes();
  }
}
