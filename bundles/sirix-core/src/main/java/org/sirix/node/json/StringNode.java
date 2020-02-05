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

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.immutable.json.ImmutableStringNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Constants;

/**
 * <h1>JSONStringNode</h1>
 *
 * <p>
 * Node representing a JSON string.
 * </p>
 */
public final class StringNode extends AbstractStringNode {

  /**
   * Constructor.
   *
   * @param valueNodeDelegate delegate for {@link ValueNode} implementation
   * @param structNodeDelegate delegate for {@link StructNode} implementation
   */
  public StringNode(final BigInteger hashCode, final ValueNodeDelegate valueNodeDelegate, final StructNodeDelegate structNodeDelegate) {
    super(valueNodeDelegate, structNodeDelegate);
    setHash(hashCode);
  }

  /**
   * Constructor.
   *
   * @param valueNodeDelegate delegate for {@link ValueNode} implementation
   * @param structNodeDelegate delegate for {@link StructNode} implementation
   */
  public StringNode(final ValueNodeDelegate valueNodeDelegate, final StructNodeDelegate structNodeDelegate) {
    super(valueNodeDelegate, structNodeDelegate);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.STRING_VALUE;
  }

  @Override
  public VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableStringNode.of(this));
  }

}
