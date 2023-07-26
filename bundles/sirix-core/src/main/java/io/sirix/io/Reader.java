/*
 * Copyright (c) 2023, Sirix Contributors
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

package io.sirix.io;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.exception.SirixIOException;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Interface for reading the stored pages in every backend.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public interface Reader extends AutoCloseable {

  /** Hash function to use for hashing pages. */
  HashFunction hashFunction = Hashing.sha256();

  /** Executor Service used for the async read. */
  ExecutorService POOL = Executors.newVirtualThreadPerTaskExecutor();

  /**
   * Getting the first reference of the {@code Uberpage}.
   *
   * @return a {@link PageReference} with link to the first reference
   * @throws SirixIOException if something bad happens
   */
  PageReference readUberPageReference();

  /**
   * Getting a reference for the given pointer.
   *
   * @param key the reference for the page to be determined
   * @param pageReadTrx {@link PageReadOnlyTrx} reference
   * @return a {@link BitmapReferencesPage} as the base for a page
   * @throws SirixIOException if something bad happens during read
   */
  default CompletableFuture<? extends Page> readAsync(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx) {
    return CompletableFuture.supplyAsync(() -> read(key, pageReadTrx), POOL);
  }

  /**
   * Getting a reference for the given pointer.
   *
   * @param key the reference for the page to be determined
   * @param pageReadTrx {@link PageReadOnlyTrx} reference
   * @return a {@link BitmapReferencesPage} as the base for a page
   * @throws SirixIOException if something bad happens during read
   */
  Page read(PageReference key, @Nullable PageReadOnlyTrx pageReadTrx);

  /**
   * Closing the storage.
   *
   * @throws SirixIOException if something bad happens while access
   */
  @Override
  void close();

  /**
   * Read the revision root page.
   *
   * @param revision the revision to read
   * @param pageReadTrx the page reading transaction
   * @return the revision root page
   */
  RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx);

  Instant readRevisionRootPageCommitTimestamp(int revision);

  RevisionFileData getRevisionFileData(int revision);
}
