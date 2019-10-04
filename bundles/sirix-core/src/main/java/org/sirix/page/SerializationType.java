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

import org.magicwerk.brownies.collections.GapList;
import org.sirix.exception.SirixIOException;

import javax.annotation.Nonnegative;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

/**
 * Defines the serialization/deserialization type.
 *
 * @author Johannes Lichtenberger <lichtenberger.johannes@gmail.com>
 *
 */
public enum SerializationType {
  /** The transaction intent log. */
  TRANSACTION_INTENT_LOG {
    @Override
    public void serialize(DataOutput out, List<PageReference> pageReferences, BitSet bitmap) {
      assert out != null;
      assert pageReferences != null;

      try {
        serializeBitSet(out, bitmap);

        for (final PageReference pageReference : pageReferences) {
          out.writeInt(pageReference.getLogKey());
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public DeserializedTuple deserialize(@Nonnegative int referenceCount, DataInput in) {
      assert in != null;

      try {
        final BitSet bitmap = deserializeBitSet(in, referenceCount);

        final int length = bitmap.cardinality();

        final List<PageReference> references = new GapList<>(length);

        for (int offset = 0; offset < length; offset++) {
          final int key = in.readInt();
          final PageReference reference = new PageReference();
          reference.setLogKey(key);
          references.add(offset, reference);
        }

        return new DeserializedTuple(references, bitmap);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  },

  /** The actual data. */
  DATA {
    @Override
    public void serialize(DataOutput out, List<PageReference> pageReferences, BitSet bitmap) {
      assert out != null;
      assert pageReferences != null;

      try {
        serializeBitSet(out, bitmap);

        for (final PageReference pageReference : pageReferences) {
          out.writeLong(pageReference.getKey());

          if (pageReference.getHash() == null) {
            out.writeInt(-1);
          } else {
            final byte[] hash = pageReference.getHash();
            out.writeInt(hash.length);
            out.write(pageReference.getHash());
          }
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }

    @Override
    public DeserializedTuple deserialize(@Nonnegative int referenceCount, DataInput in) {
      assert in != null;

      try {
        final BitSet bitmap = deserializeBitSet(in, referenceCount);
        final int length = bitmap.cardinality();
        final GapList<PageReference> references = new GapList<>(length);

        for (int offset = 0; offset < length; offset++) {
          final long key = in.readLong();
          final PageReference reference = new PageReference();
          reference.setKey(key);

          final int hashLength = in.readInt();
          if (hashLength != -1) {
            final byte[] hash = new byte[hashLength];
            in.readFully(hash);

            reference.setHash(hash);
          }

          references.add(offset, reference);
        }

        return new DeserializedTuple(references, bitmap);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  };

  private static void serializeBitSet(DataOutput out, final BitSet bitmap) throws IOException {
    final int len = bitmap.length();
    out.writeShort(len);
    for (int i = 0; i < len; i++) {
      out.writeBoolean(bitmap.get(i));
    }
  }

  private static BitSet deserializeBitSet(DataInput in, @Nonnegative int referenceCount)
      throws IOException {
    final int len = in.readShort();
    final BitSet ret = new BitSet(referenceCount);

    for (int i = 0; i < len; i++) {
      ret.set(i, in.readBoolean());
    }

    return ret;
  }

  /**
   * Serialize all page references.
   *
   * @param out the output
   * @param pageReferences the page references
   * @param bitmap the bitmap
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serialize(DataOutput out, List<PageReference> pageReferences, BitSet bitmap);

  /**
   * Deserialize all page references.
   *
   * @param referenceCount the number of references
   * @param in the input
   * @return the in-memory instances
   */
  public abstract DeserializedTuple deserialize(@Nonnegative int referenceCount, DataInput in);
}
