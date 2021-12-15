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

package org.sirix.node.json;

import java.math.BigInteger;
import javax.annotation.Nonnegative;

import com.google.common.hash.HashFunction;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.HashCode;

/**
 *
 * <p>
 * Node representing an XML element.
 * </p>
 *
 * <strong>This class is not part of the public API and might change.</strong>
 */
public final class ObjectKeyNode extends AbstractStructForwardingNode implements ImmutableJsonNode, ImmutableNameNode {

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate structNodeDel;

  private int nameKey;

  private final String name;

  private long pathNodeKey;

  private BigInteger hash;

  /**
   * Constructor
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param name the key name
   */
  public ObjectKeyNode(final StructNodeDelegate structDel, final int nameKey, final String name,
      final long pathNodeKey) {
    assert structDel != null;
    assert name != null;
    structNodeDel = structDel;
    this.nameKey = nameKey;
    this.name = name;
    this.pathNodeKey = pathNodeKey;
  }

  /**
   * Constructor
   *
   * @param hashCode the hash code
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameKey the key of the name
   * @param name the String name
   * @param pathNodeKey the path node key
   */
  public ObjectKeyNode(final BigInteger hashCode, final StructNodeDelegate structDel, final int nameKey, final String name,
      final long pathNodeKey) {
    hash = hashCode;
    assert structDel != null;
    structNodeDel = structDel;
    this.nameKey = nameKey;
    this.name = name;
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_KEY;
  }

  @Override
  public BigInteger computeHash() {
    final HashFunction hashFunction = structNodeDel.getNodeDelegate().getHashFunction();
    assert name != null;
    final HashCode hashCode = hashFunction.hashString(name, Constants.DEFAULT_ENCODING);

    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(new BigInteger(1, hashCode.asBytes()));

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hash;
  }

  public int getNameKey() {
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    this.nameKey = nameKey;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableObjectKeyNode.of(this));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("name", name)
                      .add("nameKey", nameKey)
                      .add("structDelegate", structNodeDel)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, nameKey, delegate());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ObjectKeyNode))
      return false;

    final ObjectKeyNode other = (ObjectKeyNode) obj;
    return Objects.equal(name, other.name) && nameKey == other.nameKey
        && Objects.equal(delegate(), other.delegate());
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  public ObjectKeyNode setPathNodeKey(final @Nonnegative long pathNodeKey) {
    this.pathNodeKey = pathNodeKey;
    return this;
  }

  @Override public int getLocalNameKey() {
    return nameKey;
  }

  @Override public int getPrefixKey() {
    throw new UnsupportedOperationException();
  }

  @Override public int getURIKey() {
    throw new UnsupportedOperationException();
  }

  public long getPathNodeKey() {
    return pathNodeKey;
  }

  public QNm getName() {
    return new QNm(name);
  }
}
