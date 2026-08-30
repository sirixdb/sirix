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
import io.sirix.exception.SirixIOException;
import io.sirix.page.PageReference;
import io.sirix.page.RegionsOnlyPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

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
  default CompletableFuture<? extends Page> readAsync(PageReference key, ResourceConfiguration resourceConfiguration) {
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
   * Read a record page without expanding the records the caller has not asked for.
   *
   * <p>
   * A point lookup opens a whole page to answer for one slot and decodes the other thousand records
   * for nothing. A chunk-framed body lets the reader stop after the page's metadata and expand only
   * the chunk a slot lives in; see {@code LazyChunkedBody}. This is a request, not a requirement — a
   * backend that cannot answer lazily, or a page whose body is not chunk-framed, returns a fully
   * decoded page and the caller cannot tell the difference except by how long the call took.
   *
   * <p>
   * Only ever worth asking for when the load is driven by a single record key. A scan reads every
   * slot, so it would pay the per-chunk framing for no saving; the default below is what it gets.
   *
   * @param key the reference for the page to be determined
   * @param resourceConfiguration the resource configuration
   * @return the page, whose records may be expanded on demand
   * @throws SirixIOException if something bad happens during read
   */
  default Page readRecordPageLazily(PageReference key, ResourceConfiguration resourceConfiguration) {
    return read(key, resourceConfiguration);
  }

  /**
   * Batched positional page read for offset-keyed references. Implementations backed by a seekable
   * file should override this with COALESCED reads: runs of near-adjacent offsets become one large
   * sequential read instead of two preads (length header + body) per page — the projection column
   * fetch reads ~2 segments per leaf per query, so the per-page syscall pair dominates warm-cache
   * fills. The default preserves exact per-page semantics by delegating to
   * {@link #read(PageReference, ResourceConfiguration)}.
   *
   * <p>
   * Contract: {@code result[i]} is the page for {@code references[i]} (input order); a reference with
   * no disk key yields {@code null}. Offsets need not be sorted — the override coalesces only what is
   * profitably adjacent.
   *
   * @param references the offset-keyed references to read
   * @param resourceConfiguration the resource configuration
   * @return one page per reference, input-aligned
   * @throws SirixIOException if something bad happens during read
   */
  default Page[] read(final PageReference[] references, final ResourceConfiguration resourceConfiguration) {
    final Page[] pages = new Page[references.length];
    for (int i = 0; i < references.length; i++) {
      if (references[i] != null && references[i].getKey() != Constants.NULL_ID_LONG) {
        pages[i] = read(references[i], resourceConfiguration);
      }
    }
    return pages;
  }

  /**
   * Best-effort batched warm-up of upcoming page reads, for scan paths that know their page schedule
   * ahead of consumption. An implementation may fetch the referenced pages' raw bytes in one
   * submission and satisfy the next {@link #read(PageReference, ResourceConfiguration)} of each
   * reference from that staging area — collapsing a queue-depth-1 read-per-page loop (two device
   * round trips per page: length header, then body) into two round trips per batch. Purely an I/O
   * hint: it must not change what any subsequent read returns, and fragments beyond the referenced
   * offsets still read normally.
   *
   * <p>
   * The default is a no-op — a buffered backend already enjoys kernel readahead, and a backend
   * without a batching primitive loses nothing.
   *
   * <p>
   * Failure contract: an ordinary I/O failure must be reported as {@link SirixIOException}, which
   * callers treat as "declined" and ignore. Throw anything else ONLY when the backend has left itself
   * in a state where subsequent reads could return wrong bytes (an un-drainable completion queue,
   * say) — that escapes to the caller and fails the query, because silently reading on would be worse
   * than stopping.
   *
   * <p>
   * The array is CALLER-OWNED scratch: implementations must consume it before returning and must not
   * retain it (callers reuse and clear the same array across windows). Anything an implementation
   * needs after the call — offsets, staged bytes — must be copied out.
   *
   * @param references offset-keyed references expected to be read soon; entries may be {@code null}
   *        or lack a disk key and are then ignored; not retained past the call
   * @param count number of leading entries of {@code references} to consider
   */
  default void prefetch(final PageReference[] references, final int count) {}

  /**
   * How many pages this backend profitably prefetches per {@link #prefetch} batch, or {@code 0} when
   * it does not implement prefetching. Callers must check this ONCE per scan and skip all prefetch
   * work — reference resolution included — on {@code 0}: the batch size is a backend property (ring
   * depth, coalescing width), and a backend without the primitive must not tax the scan loop for it.
   */
  default int preferredPrefetchBatch() {
    return 0;
  }

  /**
   * Read a record page's PAX regions <em>without</em> materializing its record heap.
   *
   * <p>
   * The columnar answer to a columnar question: a page's values are written column-oriented and the
   * full read path transposes them into a row heap, only for the scan to derive columns back out of
   * it. This entry point stops after the columns. It decompresses nothing but the region kinds named
   * in {@code regionKindMask}, allocates their payloads from the bounded native frame allocator, and
   * never enters the record-page cache.
   *
   * <p>
   * The default returns {@code null} — a backend without the fast path is not an error, it just sends
   * the caller back to {@link #read(PageReference, ResourceConfiguration)}.
   *
   * @param key the reference of the record page to read
   * @param resourceConfiguration the resource configuration
   * @param regionKindMask bitmask of region kinds to read; see
   *        {@link io.sirix.page.pax.RegionTable#maskOf(byte)}
   * @param regionDeferMask subset of {@code regionKindMask} whose decompression waits until the
   *        caller actually reads it — lets a cheap region veto an expensive one
   * @return the decoded regions, or {@code null} when unsupported / not a record page; the caller
   *         owns and must close a non-null result
   */
  default @Nullable RegionsOnlyPage readRegionsOnly(PageReference key, ResourceConfiguration resourceConfiguration,
      int regionKindMask, int regionDeferMask) {
    return null;
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
   * Read a contiguous range of revision records. Implementations should override this with a single
   * bulk read — the revision-index load on storage open calls it with the FULL history, and the
   * default per-revision loop costs one syscall plus one buffer per revision, which made
   * request-scoped opens linear in revision count.
   *
   * @param fromRevision first revision (inclusive)
   * @param count number of consecutive revisions to read
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
