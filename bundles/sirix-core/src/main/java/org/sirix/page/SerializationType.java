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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.sirix.exception.SirixIOException;
import org.sirix.settings.Constants;

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
    public void serialize(DataOutput out, PageReference[] pageReferences) {
      assert out != null;
      assert pageReferences != null;

      for (final PageReference reference : pageReferences) {
        try {
          out.writeBoolean(reference.getLogKey() != Constants.NULL_ID_INT);
          if (reference.getLogKey() != Constants.NULL_ID_INT)
            out.writeInt(reference.getLogKey());
        } catch (final IOException e) {
          throw new SirixIOException(e);
        }
      }
    }

    @Override
    public PageReference[] deserialize(int referenceCount, DataInput in) {
      assert referenceCount >= 0;
      assert in != null;

      try {
        final PageReference[] references = new PageReference[referenceCount];

        for (int offset = 0, length = references.length; offset < length; offset++) {
          references[offset] = new PageReference();

          final boolean hasKey = in.readBoolean();
          if (hasKey) {
            final int key = in.readInt();
            references[offset].setLogKey(key);
          }
        }

        return references;
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  },

  /** The actual data. */
  DATA {
    @Override
    public void serialize(DataOutput out, PageReference[] pageReferences) {
      assert out != null;
      assert pageReferences != null;

      for (final PageReference reference : pageReferences) {
        try {
          out.writeBoolean(reference.getKey() != Constants.NULL_ID_LONG);
          if (reference.getKey() != Constants.NULL_ID_LONG)
            out.writeLong(reference.getKey());
        } catch (final IOException e) {
          throw new SirixIOException(e);
        }
      }
    }

    @Override
    public PageReference[] deserialize(int referenceCount, DataInput in) {
      assert referenceCount >= 0;
      assert in != null;

      try {
        final PageReference[] references = new PageReference[referenceCount];

        for (int offset = 0, length = references.length; offset < length; offset++) {
          references[offset] = new PageReference();

          final boolean hasKey = in.readBoolean();
          if (hasKey) {
            final long key = in.readLong();
            references[offset].setKey(key);
          }
        }

        return references;
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  };

  /**
   * Serialize all page references.
   *
   * @param out the output
   * @param pageReferences the page references
   * @throws SirixIOException if an I/O error occurs.
   */
  public abstract void serialize(DataOutput out, PageReference[] pageReferences);

  /**
   * Deserialize all page references.
   *
   * @param referenceCount the number of references
   * @param in the input
   * @return the in-memory instances
   */
  public abstract PageReference[] deserialize(int referenceCount, DataInput in);
}
