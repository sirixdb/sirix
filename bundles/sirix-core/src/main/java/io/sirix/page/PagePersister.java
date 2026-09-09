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

package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.interfaces.Page;

import org.jspecify.annotations.Nullable;
import java.io.IOException;

/**
 * Persists pages on secondary storage.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PagePersister {

  /**
   * Deserialize page.
   *
   * @param source source to read from
   * @param resourceConfiguration the resource configuration
   * @return {@link Page} instance
   * @throws IOException if an exception during deserialization of a page occurs
   */
  public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type) throws IOException {
    return deserializePage(resourceConfiguration, source, type, null);
  }

  /**
   * Deserialize page with optional DecompressionResult for zero-copy support.
   * 
   * <p>
   * When decompressionResult is provided, KeyValueLeafPages can take ownership of the decompression
   * buffer and use it directly as slotMemory, eliminating per-slot copy operations.
   *
   * @param resourceConfiguration the resource configuration
   * @param source source to read from
   * @param type the serialization type
   * @param decompressionResult optional decompression result for zero-copy (may be null)
   * @return {@link Page} instance
   * @throws IOException if an exception during deserialization of a page occurs
   */
  public Page deserializePage(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) throws IOException {
    return PageKind.getKind(source.readByte())
                   .deserializePage(resourceConfiguration, source, type, decompressionResult);
  }

  /**
   * Deserialize a page, leaving the records a caller has not asked for unexpanded where the page kind
   * supports it. See {@link PageKind#deserializePageLazily}; every other kind decodes whole.
   *
   * @param resourceConfiguration the resource configuration
   * @param source source to read from
   * @param type the serialization type
   * @param decompressionResult optional decompression result for zero-copy (may be null)
   * @return {@link Page} instance
   * @throws IOException if an exception during deserialization of a page occurs
   */
  public Page deserializePageLazily(final ResourceConfiguration resourceConfiguration, final BytesIn<?> source,
      final SerializationType type, final ByteHandler.DecompressionResult decompressionResult) throws IOException {
    return PageKind.getKind(source.readByte())
                   .deserializePageLazily(resourceConfiguration, source, type, decompressionResult);
  }

  /**
   * Deserialize only the PAX regions of a record page — the columns, without the record heap.
   *
   * @param resourceConfiguration the resource configuration
   * @param source source to read from
   * @param regionKindMask bitmask of region kinds to read
   * @param regionDeferMask subset left compressed until first use
   * @return the decoded regions, or {@code null} when the page kind carries none
   */
  /** See {@link PageKind#probeRegionTableOffset}. Returns {@code -1} for non-record pages. */
  public long probeRegionTableOffset(final BytesIn<?> source, final long[] out, final long @Nullable [] bitmapOut) {
    final PageKind kind = PageKind.getKind(source.readByte());
    if (kind != PageKind.KEYVALUELEAFPAGE) {
      return -1L;
    }
    return kind.probeRegionTableOffset(source, out, bitmapOut);
  }

  /** See {@link PageKind#deserializeRegionTableAt}. */
  public RegionsOnlyPage deserializeRegionTableAt(final ResourceConfiguration resourceConfiguration,
      final BytesIn<?> source, final long pageKey, final int revision, final int populatedCount,
      final long fsstSymbolTableId, final int regionKindMask, final int regionDeferMask,
      final long @Nullable [] slotBitmap, final boolean hasCompleteColumnCoverage) {
    return PageKind.KEYVALUELEAFPAGE.deserializeRegionTableAt(resourceConfiguration, source, pageKey, revision,
        populatedCount, fsstSymbolTableId, regionKindMask, regionDeferMask, slotBitmap, hasCompleteColumnCoverage);
  }

  public RegionsOnlyPage deserializeRegionsOnlyPage(final ResourceConfiguration resourceConfiguration,
      final BytesIn<?> source, final int regionKindMask, final int regionDeferMask) {
    return PageKind.getKind(source.readByte())
                   .deserializeRegionsOnlyPage(resourceConfiguration, source, regionKindMask, regionDeferMask);
  }

  /**
   * Serialize page.
   *
   * @param sink output sink
   * @param page the {@link Page} to serialize
   * @throws IOException if an exception during serialization of a page occurs
   */
  public void serializePage(final ResourceConfiguration resourceConfiguration, final BytesOut<?> sink, final Page page,
      final SerializationType type) throws IOException {
    PageKind.getKind(page.getClass()).serializePage(resourceConfiguration, sink, page, type);
  }
}
