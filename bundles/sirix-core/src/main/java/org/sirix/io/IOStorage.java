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

import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandler;

/**
 * Interface to generate access to the storage. The storage is flexible as long as {@link Reader}
 * and {@link Writer}-implementations are provided. Utility methods for common interaction with the
 * storage are provided via the {@code Storage}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public interface IOStorage {

  /**
   * Getting a writer.
   * 
   * @return an {@link Writer} instance
   * @throws SirixIOException if the initialization fails
   */
  Writer createWriter();

  /**
   * Getting a reader.
   * 
   * @return an {@link Reader} instance
   * @throws SirixIOException if the initialization fails
   */
  Reader createReader();

  /**
   * Closing this storage.
   * 
   * @throws SirixIOException if an I/O error occurs
   */
  void close();

  /**
   * Check if storage exists.
   * 
   * @return true if storage holds data, false otherwise
   * @throws SirixIOException if storage is not accessible
   */
  boolean exists();

  /**
   * Get the byte handler pipeline.
   * 
   * @return byte handler pipeline
   */
  ByteHandler getByteHandler();
}
