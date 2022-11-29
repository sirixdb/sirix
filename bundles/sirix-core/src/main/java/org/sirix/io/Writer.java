/**
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

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.PageReference;

import java.nio.ByteBuffer;

/**
 * Interface to provide the abstract layer related to write access of the Sirix-backend.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public interface Writer extends Reader {

  short UBER_PAGE_BYTE_ALIGN = 512;

  short REVISION_ROOT_PAGE_BYTE_ALIGN = 256; // Must be a power of two.

  byte PAGE_FRAGMENT_BYTE_ALIGN = 8; // Must be a power of two.

  int FLUSH_SIZE = 64_000;

  /**
   * Writing a page related to the reference.
   *
   * @param pageReference that points to a page
   * @param bufferedBytes  the bytes to write
   * @throws SirixIOException exception to be thrown if something bad happens
   * @return this writer instance
   */
  Writer write(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference, Bytes<ByteBuffer> bufferedBytes);

  /**
   * Write beacon for the first reference.
   *
   * @param pageReference that points to the beacon
   * @param bufferedBytes  the bytes to write
   * @throws SirixIOException if an I/O error occured
   * @return this writer instance
   */
  Writer writeUberPageReference(PageReadOnlyTrx pageReadOnlyTrx, PageReference pageReference, Bytes<ByteBuffer> bufferedBytes);

  /**
   * Truncate to a specific revision.
   *
   * @param revision the revision to truncate to.
   * @return this writer instance
   */
  Writer truncateTo(PageReadOnlyTrx pageReadOnlyTrx, int revision);

  /**
   * Truncate, that is remove all file content.
   */
  Writer truncate();
}
