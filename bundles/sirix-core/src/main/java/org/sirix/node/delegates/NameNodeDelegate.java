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
package org.sirix.node.delegates;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;

import java.math.BigInteger;

/**
 * Delegate method for all nodes containing naming-data. That means that different fixed defined
 * names are represented by the nodes delegating the calls of the interface {@link NameNode} to this
 * class. Mainly, keys are stored referencing later on to the string stored in dedicated pages.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public class NameNodeDelegate extends AbstractForwardingNode implements NameNode {

  /**
   * Node delegate, containing basic node information.
   */
  private final NodeDelegate nodeDelegate;

  /**
   * Key of the prefix.
   */
  private int prefixKey;

  /**
   * Key of the local name
   */
  private int localNameKey;

  /**
   * URI of the related namespace.
   */
  private int uriKey;

  /**
   * Path node key.
   */
  private long pathNodeKey;

  /**
   * Constructor.
   *
   * @param delegate     page delegator
   * @param uriKey       uriKey to be stored
   * @param prefixKey    prefixKey to be stored
   * @param localNameKey localNameKey to be stored
   * @param pathNodeKey  path node key associated with node
   */
  public NameNodeDelegate(final NodeDelegate delegate, final int uriKey, final int prefixKey, final int localNameKey,
      final @NonNegative long pathNodeKey) {
    assert delegate != null : "delegate must not be null!";
    this.nodeDelegate = delegate;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    assert pathNodeKey >= 0 : "pathNodeKey may not be < 0!";
    this.pathNodeKey = pathNodeKey;
  }

  /**
   * Copy constructor.
   *
   * @param nameDel old name node delegate
   */
  public NameNodeDelegate(final NameNodeDelegate nameDel) {
    nodeDelegate = nameDel.nodeDelegate;
    prefixKey = nameDel.prefixKey;
    localNameKey = nameDel.localNameKey;
    uriKey = nameDel.uriKey;
    pathNodeKey = nameDel.pathNodeKey;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.UNKNOWN;
  }

  @Override
  public BigInteger computeHash() {
    final Funnel<NameNode> nodeFunnel = (NameNode node, PrimitiveSink into) -> into.putInt(node.getURIKey())
                                                                                   .putInt(node.getPrefixKey())
                                                                                   .putInt(node.getLocalNameKey())
                                                                                   .putLong(node.getPathNodeKey());

    return Node.to128BitsAtMaximumBigInteger(new BigInteger(1,
                                                            nodeDelegate.getHashFunction()
                                                                        .hashObject(this, nodeFunnel)
                                                                        .asBytes()));
  }

  @Override
  public BigInteger getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final BigInteger hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getLocalNameKey() {
    return localNameKey;
  }

  @Override
  public int getURIKey() {
    return uriKey;
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    this.localNameKey = localNameKey;
  }

  @Override
  public void setURIKey(final int uriKey) {
    this.uriKey = uriKey;
  }

  /**
   * Setting the class path record.
   *
   * @param pathNodeKey the path class record to set
   */
  @Override
  public void setPathNodeKey(@NonNegative final long pathNodeKey) {
    assert pathNodeKey >= 0 : "pathNodeKey may not be < 0!";
    this.pathNodeKey = pathNodeKey;
  }

  /**
   * Get the path class record.
   *
   * @return path class record the node belongs to
   */
  @Override
  public long getPathNodeKey() {
    return pathNodeKey;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uriKey, prefixKey, localNameKey);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof final NameNodeDelegate other))
      return false;

    return Objects.equal(uriKey, other.uriKey) && Objects.equal(prefixKey, other.prefixKey) && Objects.equal(
        localNameKey,
        other.localNameKey);
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", nodeDelegate)
                      .add("uriKey", uriKey)
                      .add("prefixKey", prefixKey)
                      .add("localNameKey", localNameKey)
                      .add("pathNodeKey", pathNodeKey)
                      .toString();
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public int getPrefixKey() {
    return prefixKey;
  }

  @Override
  public void setPrefixKey(int prefixKey) {
    this.prefixKey = prefixKey;
  }

  // Override in concrete node-classes, as the type is not known over here.
  @Override
  public QNm getName() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDelegate.getDeweyIDAsBytes();
  }
}
