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

package io.sirix.node;

import io.sirix.node.interfaces.StructNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

/**
 * Shared cached state and heap encoding for XML and JSON document roots. Both root kinds have the
 * same field layout; only their kind byte differs.
 */
public abstract class AbstractDocumentRootNode extends AbstractFlyweightNode implements StructNode {
  protected @Nullable LongHashFunction hashFunction;
  protected @Nullable SirixDeweyID sirixDeweyID;
  protected byte @Nullable [] deweyIDBytes;
  /** @see #getOwnerPage() */
  protected @Nullable KeyValueLeafPage ownerPage;
  /** @see #isWriteSingleton() */
  private boolean writeSingleton;

  @Override
  public final boolean isWriteSingleton() {
    return writeSingleton;
  }

  @Override
  public final void setWriteSingleton(final boolean writeSingleton) {
    this.writeSingleton = writeSingleton;
  }

  @Override
  public final @Nullable KeyValueLeafPage getOwnerPage() {
    return ownerPage;
  }

  @Override
  public final void setOwnerPage(final KeyValueLeafPage ownerPage) {
    this.ownerPage = ownerPage;
  }

  @Override
  public final SirixDeweyID getDeweyID() {
    if (sirixDeweyID == null && deweyIDBytes != null) {
      sirixDeweyID = new SirixDeweyID(deweyIDBytes);
    }
    if (sirixDeweyID == null) {
      sirixDeweyID = SirixDeweyID.newRootID();
    }
    return sirixDeweyID;
  }

  @Override
  public final byte[] getDeweyIDAsBytes() {
    if (deweyIDBytes == null && sirixDeweyID != null) {
      deweyIDBytes = sirixDeweyID.toBytes();
    }
    if (deweyIDBytes == null) {
      deweyIDBytes = SirixDeweyID.newRootID().toBytes();
    }
    return deweyIDBytes;
  }

  @Override
  public final void setDeweyID(@Nullable SirixDeweyID id) {
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = getNodeKey();
      final int slot = getSlotIndex();
      unbind();
      this.sirixDeweyID = id;
      this.deweyIDBytes = null;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  @Override
  public final void setDeweyIDBytes(final byte @Nullable [] bytes) {
    this.deweyIDBytes = bytes;
    this.sirixDeweyID = null;
  }

  /**
   * Set DeweyID fields directly after creation, bypassing write-through. The DeweyID is already in
   * the page trailer -- this just sets the Java cache fields.
   */
  public final void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  /**
   * Get the hash function.
   *
   * @return the hash function
   */
  public final @Nullable LongHashFunction getHashFunction() {
    return hashFunction;
  }

  /**
   * Encode the shared document-root layout without allocating per-record scratch storage. The caller
   * supplies the XML or JSON document kind; both use this seven-field layout.
   */
  protected static int writeDocumentRootRecord(final MemorySegment target, final long offset, final int[] heapOffsets,
      final long nodeKey, final long firstChildKey, final long lastChildKey, final long childCount,
      final long descendantCount, final long hash, final NodeKind kind) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, kind.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += NodeFieldLayout.XML_DOCUMENT_ROOT_FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: firstChildKey (delta-varint from nodeKey)
    heapOffsets[NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 1: lastChildKey (delta-varint from nodeKey)
    heapOffsets[NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 2: childCount (signed long varint)
    heapOffsets[NodeFieldLayout.XDOCROOT_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 3: descendantCount (signed long varint)
    heapOffsets[NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Field 4: previousRevision (signed varint) -- always 0 for document root
    heapOffsets[NodeFieldLayout.XDOCROOT_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 5: lastModifiedRevision (signed varint) -- always 0 for document root
    heapOffsets[NodeFieldLayout.XDOCROOT_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 6: hash (fixed 8 bytes)
    heapOffsets[NodeFieldLayout.XDOCROOT_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Write offset table
    for (int i = 0; i < NodeFieldLayout.XML_DOCUMENT_ROOT_FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }
}
