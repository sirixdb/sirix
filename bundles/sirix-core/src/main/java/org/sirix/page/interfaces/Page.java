/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.page.interfaces;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Page interface all pages have to implement.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public interface Page {

  /**
   * Serialize a page
   *
   * @param out {@link DataOutput} to serialize to
   * @param type serialization type (currently transaction intent log or normal commit)
   */
  void serialize(Bytes<ByteBuffer> out, SerializationType type);

  /**
   * Get all page references.
   *
   * @return all page references
   */
  List<PageReference> getReferences();

  /**
   * Commit page.
   *
   * @param pageWriteTrx {@link PageTrx} implementation
   * @throws SirixIOException if an I/O exception occured
   */
  void commit(@NonNull PageTrx pageWriteTrx);

  /**
   * Get the {@link PageReference} at the specified offset
   *
   * @param offset the offset
   * @return the {@link PageReference} at the specified offset
   */
  PageReference getOrCreateReference(@NonNegative int offset);

  /**
   * Set the reference at the specified offset
   * @param offset the offset
   * @param pageReference the page reference
   * @return {@code true}, if the page is already full, {@code false} otherwise
   */
  boolean setOrCreateReference(int offset, PageReference pageReference);
}
