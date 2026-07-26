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

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

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

  /**
   * Executor Service used for the async read.
   */
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
   * @param resourceConfiguration the resource configuration
   * @return a {@link BitmapReferencesPage} as the base for a page
   * @throws SirixIOException if something bad happens during read
   */
  default CompletableFuture<? extends Page> readAsync(PageReference key,
      ResourceConfiguration resourceConfiguration) {
    return CompletableFuture.supplyAsync(() -> read(key, resourceConfiguration), POOL);
  }

  /**
   * Getting a reference for the given pointer.
   *
   * @param key the reference for the page to be determined
   * @param resourceConfiguration the resource configuration
   * @return a {@link BitmapReferencesPage} as the base for a page
   * @throws SirixIOException if something bad happens during read
   */
  Page read(PageReference key, ResourceConfiguration resourceConfiguration);

  /**
   * Batched positional page read for offset-keyed references. Implementations backed by a
   * seekable file should override this with COALESCED reads: runs of near-adjacent offsets
   * become one large sequential read instead of two preads (length header + body) per page —
   * the projection column fetch reads ~2 segments per leaf per query, so the per-page
   * syscall pair dominates warm-cache fills. The default preserves exact per-page semantics
   * by delegating to {@link #read(PageReference, ResourceConfiguration)}.
   *
   * <p>Contract: {@code result[i]} is the page for {@code references[i]} (input order); a
   * reference with no disk key yields {@code null}. Offsets need not be sorted — the
   * override coalesces only what is profitably adjacent.
   *
   * @param references the offset-keyed references to read
   * @param resourceConfiguration the resource configuration
   * @return one page per reference, input-aligned
   * @throws SirixIOException if something bad happens during read
   */
  default Page[] read(final PageReference[] references,
      final ResourceConfiguration resourceConfiguration) {
    final Page[] pages = new Page[references.length];
    for (int i = 0; i < references.length; i++) {
      if (references[i] != null && references[i].getKey() != Constants.NULL_ID_LONG) {
        pages[i] = read(references[i], resourceConfiguration);
      }
    }
    return pages;
  }

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
   * @param resourceConfiguration the resource configuration
   * @return the revision root page
   */
  RevisionRootPage readRevisionRootPage(int revision, ResourceConfiguration resourceConfiguration);

  Instant readRevisionRootPageCommitTimestamp(int revision);

  RevisionFileData getRevisionFileData(int revision);

  /**
   * Read a contiguous range of revision records. Implementations should override this with a
   * single bulk read — the revision-index load on storage open calls it with the FULL history,
   * and the default per-revision loop costs one syscall plus one buffer per revision, which
   * made request-scoped opens linear in revision count.
   *
   * @param fromRevision first revision (inclusive)
   * @param count        number of consecutive revisions to read
   * @return one {@link RevisionFileData} per revision, in order
   */
  default RevisionFileData[] getRevisionFileData(final int fromRevision, final int count) {
    final RevisionFileData[] result = new RevisionFileData[Math.max(count, 0)];
    for (int i = 0; i < count; i++) {
      result[i] = getRevisionFileData(fromRevision + i);
    }
    return result;
  }
}
