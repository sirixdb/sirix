/*
 * Copyright (c) 2018, Sirix
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
package io.sirix.page;

import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.utils.GapList;
import io.sirix.exception.SirixIOException;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Defines the serialization/deserialization type.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public enum SerializationType {

  /**
   * The actual data.
   */
  DATA {
    @Override
    public void serializeBitmapReferencesPage(BytesOut<?> out, List<PageReference> pageReferences, BitSet bitmap) {
      assert out != null;
      assert pageReferences != null;

      try {
        serializeBitSet(out, bitmap);

        for (int index = 0; index < pageReferences.size(); index++) {
          final PageReference pageReference = pageReferences.get(index);
          writePageFragments(out, pageReference);
          writeHash(out, pageReference);
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public void serializeReferencesPage4(BytesOut<?> out, List<PageReference> pageReferences, ShortList offsets) {
      try {
        out.writeByte((byte) pageReferences.size());
        for (int index = 0; index < pageReferences.size(); index++) {
          final PageReference pageReference = pageReferences.get(index);
          writePageFragments(out, pageReference);
          writeHash(out, pageReference);
        }
        for (int index = 0; index < offsets.size(); index++) {
          out.writeShort(offsets.getShort(index));
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public DeserializedBitmapReferencesPageTuple deserializeBitmapReferencesPage(int referenceCount, BytesIn<?> in) {
      assert in != null;

      try {
        final BitSet bitmap = deserializeBitSet(in);
        final int length = bitmap.cardinality();
        final GapList<PageReference> references = new GapList<>(length);

        for (int offset = 0; offset < length; offset++) {
          final PageReference reference = new PageReference();
          readPageFragments(in, reference);
          readHash(in, reference);
          references.add(offset, reference);
        }

        return new DeserializedBitmapReferencesPageTuple(references, bitmap);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public DeserializedReferencesPage4Tuple deserializeReferencesPage4(BytesIn<?> in) {
      try {
        final byte size = in.readByte();
        final List<PageReference> pageReferences = new ArrayList<>(4);
        final ShortList offsets = new ShortArrayList(4);
        for (int i = 0; i < size; i++) {
          final var reference = new PageReference();
          readPageFragments(in, reference);
          readHash(in, reference);
          pageReferences.add(reference);
        }
        for (int i = 0; i < size; i++) {
          offsets.add(in.readShort());
        }
        return new DeserializedReferencesPage4Tuple(pageReferences, offsets);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public void serializeFullReferencesPage(BytesOut<?> out, PageReference[] pageReferences) {
      try {
        serializeReferencePresence(out, pageReferences);

        for (int index = 0; index < pageReferences.length; index++) {
          final PageReference pageReference = pageReferences[index];
          if (pageReference != null) {
            // writePageFragments already writes pageReference.getKey() as its trailing long, so the
            // former explicit out.writeLong(getKey()) here duplicated the key (8 wasted bytes/ref).
            // Mirrors serializeBitmapReferencesPage / serializeReferencesPage4, which never wrote it.
            writePageFragments(out, pageReference);
            writeHash(out, pageReference);
          }
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public PageReference[] deserializeFullReferencesPage(BytesIn<?> in) {
      try {
        final PageReference[] references = new PageReference[Constants.INP_REFERENCE_COUNT];
        final BitSet bitSet = deserializeBitSet(in);

        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          final var pageReference = new PageReference();
          // readPageFragments reads the trailing key long and setKey()s it (mirrors the serialize
          // side); the former explicit setKey(readLong()) here consumed a duplicate 8-byte key.
          readPageFragments(in, pageReference);
          readHash(in, pageReference);
          references[i] = pageReference;
        }

        return references;
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  };

  private static void writeHash(BytesOut<?> out, PageReference pageReference) throws IOException {
    // One presence flag byte instead of the old [i32 len] prefix for an always-8-byte value —
    // 3 bytes saved per reference (IndirectPages carry up to 1024 of them).
    if (!pageReference.hasHash()) {
      out.writeByte((byte) 0);
    } else {
      out.writeByte((byte) 1);
      // BytesOut primitives are little-endian, while the established checksum wire is the
      // canonical big-endian representation produced by HashAlgorithm.longToBytes().
      out.writeLong(Long.reverseBytes(pageReference.getHashAsLong()));
    }
  }

  private static void readHash(BytesIn<?> in, PageReference reference) throws IOException {
    if (in.readByte() != 0) {
      reference.setHash(Long.reverseBytes(in.readLong()));
    }
  }

  private static void readPageFragments(BytesIn<?> in, PageReference reference) throws IOException {
    final int keysSize = in.readByte() & 0xff;
    if (keysSize > 0) {
      for (int i = 0; i < keysSize; i++) {
        final var revision = in.readInt();
        final var key = in.readLong();
        // Note: Database and resource IDs will be set by Reader.fixupPageReferenceIds()
        // after the page is fully deserialized. This matches PostgreSQL pattern.
        reference.addPageFragment(new PageFragmentKeyImpl(revision, key, 0, 0));
      }
    }
    final long key = in.readLong();
    reference.setKey(key);
  }

  private static void writePageFragments(BytesOut<?> out, PageReference pageReference) throws IOException {
    final var keys = pageReference.getPageFragments();
    if (keys.size() > 255) {
      // The count is one byte on the wire — a silent (byte) wrap would mis-frame everything after.
      throw new IllegalStateException("Too many page fragments to serialize: " + keys.size() + " (max 255)");
    }
    out.writeByte((byte) keys.size());
    for (int index = 0; index < keys.size(); index++) {
      final PageFragmentKey key = keys.get(index);
      out.writeInt(key.revision());
      out.writeLong(key.key());
    }
    out.writeLong(pageReference.getKey());
  }

  public static void serializeBitSet(BytesOut<?> out, final BitSet bitmap) {
    final int byteLength = Math.toIntExact((bitmap.length() + 7L) >>> 3);
    if (byteLength > Short.MAX_VALUE) {
      throw new IllegalStateException("Bitmap wire length exceeds signed-short limit: " + byteLength);
    }
    out.writeShort((short) byteLength);

    int nextSetBit = bitmap.nextSetBit(0);
    for (int byteIndex = 0; byteIndex < byteLength; byteIndex++) {
      final int byteStartBit = byteIndex << 3;
      final int byteEndBit = byteStartBit + Byte.SIZE;
      int byteValue = 0;
      while (nextSetBit >= byteStartBit && nextSetBit < byteEndBit) {
        byteValue |= 1 << (nextSetBit - byteStartBit);
        nextSetBit = bitmap.nextSetBit(nextSetBit + 1);
      }
      out.writeByte((byte) byteValue);
    }
  }

  /** Serialize a full delegate's presence bitmap directly from indexed references. */
  private static void serializeReferencePresence(final BytesOut<?> out, final PageReference[] references) {
    int lastPresent = references.length - 1;
    while (lastPresent >= 0 && references[lastPresent] == null) {
      lastPresent--;
    }
    final int byteLength = lastPresent < 0
        ? 0
        : (lastPresent >>> 3) + 1;
    out.writeShort((short) byteLength);
    for (int byteIndex = 0; byteIndex < byteLength; byteIndex++) {
      final int referenceBase = byteIndex << 3;
      final int referenceEnd = Math.min(referenceBase + Byte.SIZE, references.length);
      int byteValue = 0;
      for (int referenceIndex = referenceBase; referenceIndex < referenceEnd; referenceIndex++) {
        if (references[referenceIndex] != null) {
          byteValue |= 1 << (referenceIndex - referenceBase);
        }
      }
      out.writeByte((byte) byteValue);
    }
  }

  public static BitSet deserializeBitSet(BytesIn<?> in) {
    final int len = in.readShort();
    final var bytes = new byte[len];
    in.read(bytes);
    return BitSet.valueOf(bytes);
  }

  /**
   * Serialize all page references.
   *
   * @param out the output
   * @param pageReferences the page references
   * @param bitmap the bitmap
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeBitmapReferencesPage(BytesOut<?> out, List<PageReference> pageReferences,
      BitSet bitmap);

  /**
   * Serialize all page references.
   *
   * @param out the output
   * @param pageReferences the page references
   * @param offsets the offset indexes
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeReferencesPage4(BytesOut<?> out, List<PageReference> pageReferences, ShortList offsets);

  /**
   * Deserialize all page references.
   *
   * @param referenceCount the number of references
   * @param in the input
   * @return the in-memory instances
   */
  public abstract DeserializedBitmapReferencesPageTuple deserializeBitmapReferencesPage(int referenceCount,
      BytesIn<?> in);

  /**
   * Deserialize all page references.
   *
   * @param in the input
   * @return the in-memory instances
   */
  public abstract DeserializedReferencesPage4Tuple deserializeReferencesPage4(BytesIn<?> in);

  /**
   * Serialize all page references.
   *
   * @param out the output
   * @param pageReferences the page references
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeFullReferencesPage(BytesOut<?> out, PageReference[] pageReferences);

  /**
   * Deserialize all page references.
   *
   * @param in the input
   * @return the in-memory instances
   */
  public abstract PageReference[] deserializeFullReferencesPage(BytesIn<?> in);
}
