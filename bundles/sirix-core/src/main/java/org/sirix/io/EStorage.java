/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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
package org.sirix.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.TTIOException;
import org.sirix.io.berkeley.BerkeleyFactory;
import org.sirix.io.berkeley.BerkeleyKey;
import org.sirix.io.file.FileFactory;
import org.sirix.io.file.FileKey;

/**
 * Utility methods for the storage. Those methods included common deletion
 * procedures as well as common checks. Furthermore, specific serialization are
 * summarized upon this enum.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public enum EStorage {
  File(1, FileKey.class) {
    @Override
    public IKey deserialize(@Nonnull final ITTSource pSource) {
      return new FileKey(pSource.readLong(), pSource.readLong());
    }

    @Override
    public void serialize(@Nonnull final ITTSink pSink, @Nonnull final IKey pKey) {
      serializeKey(pKey, getIdentifier(), pSink);
    }

    @Override
    public IStorage getInstance(@Nonnull final ResourceConfiguration pResourceConf) throws TTIOException {
      return new FileFactory(pResourceConf.mPath);
    }
  },

  BerkeleyDB(2, BerkeleyKey.class) {
    @Override
    public IKey deserialize(@Nonnull final ITTSource pSource) {
      return new BerkeleyKey(pSource.readLong());
    }

    @Override
    public void serialize(@Nonnull final ITTSink pSink, @Nonnull final IKey pKey) {
      serializeKey(pKey, getIdentifier(), checkNotNull(pSink));
    }

    @Override
    public IStorage getInstance(@Nonnull final ResourceConfiguration pResourceConf) throws TTIOException {
      return new BerkeleyFactory(pResourceConf.mPath);
    }
  };

  /**
   * Get an instance of the storage backend.
   * 
   * @param pResourceConf
   *          {@link ResourceConfiguration} reference
   * @return instance of a storage backend specified within the {@link ResourceConfiguration}
   * @throws TTIOException
   *           if an IO-error occured
   */
  public abstract IStorage getInstance(@Nonnull final ResourceConfiguration pResourceConf)
    throws TTIOException;

  /** Getting identifier mapping. */
  private static final Map<Integer, EStorage> INSTANCEFORID = new HashMap<>();
  private static final Map<Class<? extends IKey>, EStorage> INSTANCEFORCLASS = new HashMap<>();
  static {
    for (EStorage storage : values()) {
      INSTANCEFORID.put(storage.mIdent, storage);
      INSTANCEFORCLASS.put(storage.mClass, storage);
    }
  }

  /** Identifier for the storage. */
  private final int mIdent;

  /** Class for Key. */
  private final Class<? extends IKey> mClass;

  /**
   * Constructor.
   * 
   * @param pIdent
   *          identifier to be set.
   */
  EStorage(final int pIdent, final Class<? extends IKey> pClass) {
    mIdent = pIdent;
    mClass = pClass;
  }

  /**
   * Get the identifier.
   * 
   * @return the identifier of the storage
   */
  public int getIdentifier() {
    return mIdent;
  }

  /**
   * Deserialization of a key
   * 
   * @param pSource
   *          where the key should be serialized from.
   * @return the {@link IKey} retrieved from the storage.
   */
  public abstract IKey deserialize(@Nonnull final ITTSource pSource);

  /**
   * Serialization of a key
   * 
   * @param pSink
   *          where the key should be serialized to
   * @param pKey
   *          which should be serialized.
   */
  public abstract void serialize(@Nonnull final ITTSink pSink, @Nonnull final IKey pKey);

  /**
   * Deleting a storage recursive. Used for deleting a database.
   * 
   * @param pFile
   *          which should be deleted included descendants
   * @return {@code true} if delete is valid, {@code false} otherwise
   * @throws NullPointerException
   *           if {@code pFile} is {@code null}
   */
  public static boolean recursiveDelete(@Nonnull final File pFile) {
    if (pFile.isDirectory()) {
      for (final File child : pFile.listFiles()) {
        if (!recursiveDelete(child)) {
          return false;
        }
      }
    }
    return pFile.delete();
  }

  /**
   * Factory method to retrieve suitable {@link IStorage} instances based upon
   * the suitable {@link ResourceConfiguration}.
   * 
   * @param pResourceConf
   *          determining the storage
   * @return an implementation of the {@link IStorage} interface
   * @throws TTIOException
   *           if an IO-error occurs
   * @throws NullPointerException
   *           if {@code pResourceConf} is {@code null}
   */
  public static final IStorage getStorage(@Nonnull final ResourceConfiguration pResourceConf)
    throws TTIOException {
    return pResourceConf.mType.getInstance(pResourceConf);
  }

  /**
   * Getting an instance of this enum for the identifier.
   * 
   * @param pId
   *          the identifier of the enum
   * @return a concrete enum
   */
  public static final EStorage getInstance(final int pId) {
    return INSTANCEFORID.get(pId);
  }

  /**
   * Getting an instance of this enum for the identifier.
   * 
   * @param pKey
   *          the identifier of the enum
   * @return a concrete enum
   */
  public static final EStorage getInstance(@Nonnull final Class<? extends IKey> pKey) {
    return INSTANCEFORCLASS.get(pKey);
  }

  /**
   * Serializing the keys.
   * 
   * @param pKey
   *          to serialize
   * @param pId
   *          to serialize
   * @param pSink
   *          to be serialized to
   */
  private static void serializeKey(@Nonnull final IKey pKey, final int pId, @Nonnull final ITTSink pSink) {
    pSink.writeInt(pId);
    for (final long val : pKey.getKeys()) {
      pSink.writeLong(val);
    }
  }
}
