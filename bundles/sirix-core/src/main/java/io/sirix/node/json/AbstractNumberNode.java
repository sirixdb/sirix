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

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.xml.AbstractStructForwardingNode;
import io.sirix.settings.Fixed;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class AbstractNumberNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  private final StructNodeDelegate structNodeDelegate;
  private final MemorySegment memorySegment;
  private boolean useMemorySegment;
  private Number number;

  private long hashCode;

  public AbstractNumberNode(StructNodeDelegate structNodeDel, Number number) {
    this.structNodeDelegate = structNodeDel;
    this.number = number;
    this.memorySegment = null;
    this.useMemorySegment = false;
  }

  public AbstractNumberNode(StructNodeDelegate structNodeDelegate, final MemorySegment segment) {
    this.structNodeDelegate = structNodeDelegate;
    this.memorySegment = segment;
    this.useMemorySegment = true;
  }

  @Override
  public long computeHash(final BytesOut<?> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getChildCount())
         .writeLong(structNodeDelegate.getDescendantCount())
         .writeLong(structNodeDelegate.getLeftSiblingKey())
         .writeLong(structNodeDelegate.getRightSiblingKey())
         .writeLong(structNodeDelegate.getFirstChildKey());

    if (structNodeDelegate.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(structNodeDelegate.getLastChildKey());
    }

    switch (number) {
      case Float floatVal -> bytes.writeFloat(floatVal);
      case Double doubleVal -> bytes.writeDouble(doubleVal);
      case BigDecimal bigDecimalVal -> bytes.writeBigDecimal(bigDecimalVal);
      case Integer intVal -> bytes.writeInt(intVal);
      case Long longVal -> bytes.writeLong(longVal);
      case BigInteger bigIntegerVal -> bytes.writeBigInteger(bigIntegerVal);
      default -> throw new IllegalStateException("Unexpected value: " + number);
    }

    return bytes.hashDirect(nodeDelegate.getHashFunction());
  }

  @Override
  public void setHash(final long hash) {
    hashCode = hash;
  }

  @Override
  public long getHash() {
    if (hashCode == 0L) {
      hashCode = computeHash(Bytes.elasticOffHeapByteBuffer());
    }
    return hashCode;
  }

  public void setValue(final Number number) {
    useMemorySegment = false;
    hashCode = 0L;
    this.number = number;
  }

  public Number getValue() {
    if (useMemorySegment) {
      useMemorySegment = false; // Switch to using the read value directly next time.

      var source = new MemorySegmentBytesIn(memorySegment);
      final var valueType = source.readByte();

      switch (valueType) {
        case 0 -> number = source.readDouble();
        case 1 -> number = source.readFloat();
        case 2 -> number = source.readInt();
        case 3 -> number = source.readLong();
        case 4 -> number = deserializeBigInteger(source);
        case 5 -> {
          final BigInteger bigInt = deserializeBigInteger(source);
          final int scale = source.readInt();
          number = new BigDecimal(bigInt, scale);
        }
        default -> throw new AssertionError("Type not known.");
      }
    }
    return number;
  }

  private BigInteger deserializeBigInteger(final BytesIn<?> source) {
    final byte[] bytes = new byte[(int) source.readStopBit()];
    source.read(bytes);
    return new BigInteger(bytes);
  }

  @Override
  public StructNodeDelegate getStructNodeDelegate() {
    return structNodeDelegate;
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
