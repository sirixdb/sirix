/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node.json;

import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Constants;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.node.xml.AbstractStructForwardingNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;

public abstract class AbstractStringNode extends AbstractStructForwardingNode implements ValueNode, ImmutableJsonNode {

  private final ValueNodeDelegate valueNodeDelegate;

  private final StructNodeDelegate structNodeDelegate;

  private long hashCode;

  public AbstractStringNode(ValueNodeDelegate valueNodeDelegate, StructNodeDelegate structNodeDelegate) {
    this.valueNodeDelegate = valueNodeDelegate;
    this.structNodeDelegate = structNodeDelegate;
  }

  @Override
  public long computeHash(final BytesOut<?> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getLeftSiblingKey()).writeLong(structNodeDelegate.getRightSiblingKey());

    bytes.writeUtf8(new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING));

    return bytes.hashDirect(nodeDelegate.getHashFunction());
  }

  @Override
  public void setHash(final long hash) {
    hashCode = hash;
  }

  @Override
  public long getHash() {
    if (hashCode == 0L) {
      hashCode = computeHash(Bytes.threadLocalHashBuffer());
    }
    return hashCode;
  }

  @Override
  public byte[] getRawValue() {
    return valueNodeDelegate.getRawValue();
  }

  @Override
  public void setRawValue(final byte[] value) {
    hashCode = 0L;
    valueNodeDelegate.setRawValue(value);
  }

  @Override
  public String getValue() {
    return new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public StructNodeDelegate getStructNodeDelegate() {
    return structNodeDelegate;
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return valueNodeDelegate;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  @Override
  protected @NonNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }
}
