/*
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

package io.sirix.page.delegates;

import io.sirix.api.StorageEngineWriter;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Class to provide basic reference handling functionality.
 */
public final class FullReferencesPage implements Page {

  /**
   * Page references.
   */
  private final PageReference[] references;

  /**
   * Constructor to read from durable storage.
   *
   * @param in input stream to read from
   * @param type the serialization type
   */
  public FullReferencesPage(final BytesIn<?> in, final SerializationType type) {
    references = type.deserializeFullReferencesPage(in);
  }

  /**
   * Constructor to copy data from a {@link BitmapReferencesPage}.
   *
   * @param pageToClone committed page
   */
  public FullReferencesPage(final BitmapReferencesPage pageToClone) {
    references = new PageReference[Constants.INP_REFERENCE_COUNT];
    final BitSet bitSet = pageToClone.getBitmap();
    final List<PageReference> compactReferences = pageToClone.getReferences();
    int compactRank = 0;

    // BitmapReferencesPage stores references densely: bitmap offsets identify the destination
    // slots, while the source list is indexed by the rank of each set bit. Using the sparse
    // offset as a list index both misplaced references and failed for any offset >= list size.
    for (int offset = bitSet.nextSetBit(0); offset >= 0; offset = bitSet.nextSetBit(offset + 1)) {
      references[offset] = new PageReference(compactReferences.get(compactRank++));
    }
  }

  /**
   * Copy constructor.
   *
   * @param pageToClone committed page
   */
  public FullReferencesPage(final FullReferencesPage pageToClone) {
    references = new PageReference[Constants.INP_REFERENCE_COUNT];

    for (int index = 0, size = pageToClone.references.length; index < size; index++) {
      final PageReference pageReferenceToClone = pageToClone.referenceAt(index);
      // Route through the PageReference copy constructor (copies hashInBytes + fragments, nulls a
      // resolvable swizzle) — a manual copy dropped the hash, disabling checksum verification.
      references[index] = pageReferenceToClone == null
          ? null
          : new PageReference(pageReferenceToClone);
    }
  }

  @Override
  public List<PageReference> getReferences() {
    return Arrays.asList(references);
  }

  @Override
  public int getReferencesCount() {
    return references.length;
  }

  /** Allocation-free indexed access without exposing the mutable structural backing array. */
  public @Nullable PageReference referenceAt(final int index) {
    return references[Objects.checkIndex(index, references.length)];
  }

  /** Serialize through the trusted enum codec without letting the mutable backing array escape. */
  public void serializeReferences(final BytesOut<?> sink, final SerializationType type) {
    type.serializeFullReferencesPage(sink, references);
  }

  /**
   * Get page reference of given offset.
   *
   * @param offset offset of page reference
   * @return {@link PageReference} at given offset
   */
  @Override
  public PageReference getOrCreateReference(final int offset) {
    final var pageReference = references[offset];
    if (pageReference != null) {
      return pageReference;
    }
    final var newPageReference = new PageReference();
    references[offset] = newPageReference;
    return newPageReference;
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    references[offset] = pageReference;
    return false;
  }

  /**
   * Recursively call commit on all referenced pages.
   *
   * @param storageEngineWriter the storage engine writer
   */
  @Override
  public void commit(final StorageEngineWriter storageEngineWriter) {
    for (final PageReference reference : references) {
      if (reference != null && (reference.getLogKey() != Constants.NULL_ID_INT)) {
        storageEngineWriter.commit(reference);
      }
    }
  }


  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this);
    for (final PageReference ref : references) {
      helper.add("reference", ref);
    }
    return helper.toString();
  }

  @Override
  public void close() {
    Arrays.fill(references, null);
  }
}
