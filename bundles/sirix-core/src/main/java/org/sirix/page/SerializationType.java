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
package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.magicwerk.brownies.collections.GapList;
import org.sirix.exception.SirixIOException;
import org.sirix.page.interfaces.PageFragmentKey;
import org.sirix.settings.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
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
    public void serializeBitmapReferencesPage(Bytes<ByteBuffer> out, List<PageReference> pageReferences,
        BitSet bitmap) {
      assert out != null;
      assert pageReferences != null;

      try {
        serializeBitSet(out, bitmap);

        for (final PageReference pageReference : pageReferences) {
          writePageFragments(out, pageReference);
          writeHash(out, pageReference);
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public void serializeReferencesPage4(Bytes<ByteBuffer> out, List<PageReference> pageReferences,
        List<Short> offsets) {
      try {
        out.writeByte((byte) pageReferences.size());
        for (final PageReference pageReference : pageReferences) {
          writePageFragments(out, pageReference);
          writeHash(out, pageReference);
        }
        for (final short offset : offsets) {
          out.writeShort(offset);
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public DeserializedBitmapReferencesPageTuple deserializeBitmapReferencesPage(@NonNegative int referenceCount,
        Bytes<?> in) {
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
    public DeserializedReferencesPage4Tuple deserializeReferencesPage4(Bytes<?> in) {
      try {
        final byte size = in.readByte();
        final List<PageReference> pageReferences = new ArrayList<>(4);
        final List<Short> offsets = new ArrayList<>(4);
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
    public void serializeFullReferencesPage(Bytes<ByteBuffer> out, PageReference[] pageReferences) {
      try {
        final BitSet bitSet = new BitSet(Constants.INP_REFERENCE_COUNT);
        for (int i = 0, size = pageReferences.length; i < size; i++) {
          if (pageReferences[i] != null) {
            bitSet.set(i, true);
          }
        }
        serializeBitSet(out, bitSet);

        for (final PageReference pageReference : pageReferences) {
          if (pageReference != null) {
            out.writeLong(pageReference.getKey());
            writePageFragments(out, pageReference);
            writeHash(out, pageReference);
          }
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public PageReference[] deserializeFullReferencesPage(Bytes<?> in) {
      try {
        final PageReference[] references = new PageReference[Constants.INP_REFERENCE_COUNT];
        final BitSet bitSet = deserializeBitSet(in);

        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          final var pageReference = new PageReference();
          pageReference.setKey(in.readLong());
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

  private static void writeHash(Bytes<ByteBuffer> out, PageReference pageReference) throws IOException {
    if (pageReference.getHash() == null) {
      out.writeInt(-1);
    } else {
      final byte[] hash = pageReference.getHash();
      out.writeInt(hash.length);
      out.write(pageReference.getHash());
    }
  }

  private static void readHash(Bytes<?> in, PageReference reference) throws IOException {
    final int hashLength = in.readInt();
    if (hashLength != -1) {
      final byte[] hash = new byte[hashLength];
      in.read(hash);

      reference.setHash(hash);
    }
  }

  private static void readPageFragments(Bytes<?> in, PageReference reference) throws IOException {
    final int keysSize = in.readByte() & 0xff;
    if (keysSize > 0) {
      for (int i = 0; i < keysSize; i++) {
        final var revision = in.readInt();
        final var key = in.readLong();
        reference.addPageFragment(new PageFragmentKeyImpl(revision, key));
      }
    }
    final long key = in.readLong();
    reference.setKey(key);
  }

  private static void writePageFragments(Bytes<ByteBuffer> out, PageReference pageReference) throws IOException {
    final var keys = pageReference.getPageFragments();
    out.writeByte((byte) keys.size());
    for (final PageFragmentKey key : keys) {
      out.writeInt(key.revision());
      out.writeLong(key.key());
    }
    out.writeLong(pageReference.getKey());
  }

  public static void serializeBitSet(Bytes<ByteBuffer> out, @NonNull final BitSet bitmap) {
    final var bytes = bitmap.toByteArray();
    final int len = bytes.length;
    out.writeShort((short) len);
    out.write(bytes);
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
   * @param out            the output
   * @param pageReferences the page references
   * @param bitmap         the bitmap
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeBitmapReferencesPage(Bytes<ByteBuffer> out, List<PageReference> pageReferences,
      BitSet bitmap);

  /**
   * Serialize all page references.
   *
   * @param out            the output
   * @param pageReferences the page references
   * @param offsets        the offset indexes
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeReferencesPage4(Bytes<ByteBuffer> out, List<PageReference> pageReferences,
      List<Short> offsets);

  /**
   * Deserialize all page references.
   *
   * @param referenceCount the number of references
   * @param in             the input
   * @return the in-memory instances
   */
  public abstract DeserializedBitmapReferencesPageTuple deserializeBitmapReferencesPage(@NonNegative int referenceCount,
      Bytes<?> in);

  /**
   * Deserialize all page references.
   *
   * @param in the input
   * @return the in-memory instances
   */
  public abstract DeserializedReferencesPage4Tuple deserializeReferencesPage4(Bytes<?> in);

  /**
   * Serialize all page references.
   *
   * @param out            the output
   * @param pageReferences the page references
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serializeFullReferencesPage(Bytes<ByteBuffer> out, PageReference[] pageReferences);

  /**
   * Deserialize all page references.
   *
   * @param in the input
   * @return the in-memory instances
   */
  public abstract PageReference[] deserializeFullReferencesPage(Bytes<?> in);
}
