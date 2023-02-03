package org.sirix.node.immutable.xml;

import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.AttributeNode;
import org.sirix.settings.Constants;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable attribute node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableAttributeNode implements ImmutableValueNode, ImmutableNameNode, ImmutableXmlNode {

  /** Mutable {@link AttributeNode}. */
  private final AttributeNode node;

  /**
   * Private constructor.
   *
   * @param node mutable {@link AttributeNode}
   */
  private ImmutableAttributeNode(final AttributeNode node) {
    this.node = checkNotNull(node);
  }

  /**
   * Get an immutable attribute node.
   *
   * @param node the {@link AttributeNode} which should be immutable
   * @return an immutable instance
   */
  public static ImmutableAttributeNode of(final AttributeNode node) {
    return new ImmutableAttributeNode(node);
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
    return new String(((ValueNode) node).getRawValue(), Constants.DEFAULT_ENCODING);
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
