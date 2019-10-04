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

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nullable;

/**
 * Interface for reading the stored pages in every backend.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public interface Reader extends AutoCloseable {

  /**
   * Getting the first reference of the {@code Uberpage}.
   *
   * @return a {@link PageReference} with link to the first reference
   * @throws SirixIOException if something bad happens
   */
  PageReference readUberPageReference() throws SirixIOException;

  /**
   * Getting a reference for the given pointer.
   *
   * @param key the reference for the page to be determined
   * @param pageReadTrx {@link PageReadOnlyTrx} reference
   * @return a {@link PageDelegate} as the base for a page
   * @throws SirixIOException if something bad happens during read
   */
  Page read(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx) throws SirixIOException;

  /**
   * Closing the storage.
   *
   * @throws SirixIOException if something bad happens while access
   */
  @Override
  void close() throws SirixIOException;

  /**
   * Read the revision root page.
   *
   * @param revision the revision to read
   * @param pageReadTrx the page reading transaction
   * @return the revision root page
   */
  RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx);
}
