/*
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
package org.sirix.io;

import java.io.RandomAccessFile;

import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.filechannel.FileChannelStorage;
import org.sirix.io.file.FileStorage;
import org.sirix.io.memorymapped.MMStorage;
import org.sirix.io.ram.RAMStorage;

/**
 * Specific backend types are specified in this enum.
 *
 * @author Johannes Lichtenberger
 *
 */
public enum StorageType {
  /** In memory backend. */
  IN_MEMORY {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      return new RAMStorage(resourceConf);
    }
  },

  /** {@link RandomAccessFile} backend. */
  FILE {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      return new FileStorage(resourceConf);
    }
  },

  /** FileChannel backend. */
  FILECHANNEL {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      return new FileChannelStorage(resourceConf);
    }
  },

  /** Memory mapped backend. */
  MEMORY_MAPPED {
    @Override
    public IOStorage getInstance(final ResourceConfiguration resourceConf) {
      return new MMStorage(resourceConf);
    }
  };

  /**
   * Get an instance of the storage backend.
   *
   * @param resourceConf {@link ResourceConfiguration} reference
   * @return instance of a storage backend specified within the {@link ResourceConfiguration}
   * @throws SirixIOException if an IO-error occured
   */
  public abstract IOStorage getInstance(final ResourceConfiguration resourceConf);

  /**
   * Factory method to retrieve suitable {@link IOStorage} instances based upon the suitable
   * {@link ResourceConfiguration}.
   *
   * @param resourceConf determining the storage
   * @return an implementation of the {@link IOStorage} interface
   * @throws SirixIOException if an IO-exception occurs
   * @throws NullPointerException if {@code resourceConf} is {@code null}
   */
  public static final IOStorage getStorage(final ResourceConfiguration resourceConf) {
    return resourceConf.storageType.getInstance(resourceConf);
  }
}
