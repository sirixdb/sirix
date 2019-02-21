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

import javax.annotation.Nonnegative;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xdm.AbstractStructForwardingNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>ElementNode</h1>
 *
 * <p>
 * Node representing an XML element.
 * </p>
 *
 * <strong>This class is not part of the public API and might change.</strong>
 */
public final class ObjectKeyNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate mStructNodeDel;

  private int mNameKey;

  private final String mName;

  private long mPathNodeKey;

  /**
   * Constructor
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param name the key name
   */
  public ObjectKeyNode(final StructNodeDelegate structDel, final int nameKey, final String name,
      final long pathNodeKey) {
    assert structDel != null;
    mStructNodeDel = structDel;
    mNameKey = nameKey;
    mName = name;
    mPathNodeKey = pathNodeKey;
  }

  public int getNameKey() {
    return mNameKey;
  }

  public void setLocalNameKey(final int nameKey) {
    mNameKey = nameKey;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableObjectKeyNode.of(this));
  }

  @Override
  public Kind getKind() {
    return Kind.OBJECT_KEY;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("name", mName)
                      .add("nameKey", mNameKey)
                      .add("structDelegate", mStructNodeDel)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mNameKey, delegate());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ObjectKeyNode))
      return false;

    final ObjectKeyNode other = (ObjectKeyNode) obj;
    return Objects.equal(mName, other.mName) && mNameKey == other.mNameKey
        && Objects.equal(delegate(), other.delegate());
  }

  @Override
  protected NodeDelegate delegate() {
    return mStructNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }

  public ObjectKeyNode setPathNodeKey(final @Nonnegative long pathNodeKey) {
    mPathNodeKey = pathNodeKey;
    return this;
  }

  public long getPathNodeKey() {
    return mPathNodeKey;
  }

  public String getName() {
    return mName;
  }
}
