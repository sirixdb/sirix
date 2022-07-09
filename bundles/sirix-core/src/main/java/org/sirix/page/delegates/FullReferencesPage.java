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

package org.sirix.page.delegates;

import com.google.common.base.MoreObjects;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageTrx;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

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
   * @param in   input stream to read from
   * @param type the serialization type
   */
  public FullReferencesPage(final Bytes<ByteBuffer> in, final SerializationType type) {
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

    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      final var pageReferenceToClone = pageToClone.getReferences().get(i);
      final var newPageReference = new PageReference(pageReferenceToClone);
      references[i] = newPageReference;
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
      final var pageReference = new PageReference();
      final var pageReferenceToClone = pageToClone.getReferences().get(index);

      if (pageReferenceToClone != null) {
        pageReference.setKey(pageReferenceToClone.getKey());
        pageReference.setLogKey(pageReferenceToClone.getLogKey());
        pageReference.setPageFragments(new ArrayList<>(pageReferenceToClone.getPageFragments()));
        pageReference.setPersistentLogKey(pageReferenceToClone.getPersistentLogKey());
      }

      references[index] = pageReference;
    }
  }

  @Override
  public List<PageReference> getReferences() {
    return Arrays.asList(references);
  }

  /**
   * Get page reference of given offset.
   *
   * @param offset offset of page reference
   * @return {@link PageReference} at given offset
   */
  @Override
  public PageReference getOrCreateReference(final @NonNegative int offset) {
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
   * @param pageWriteTrx the page write transaction
   */
  @Override
  public void commit(@NonNull final PageTrx pageWriteTrx) {
    for (final PageReference reference : references) {
      if (reference != null && (reference.getLogKey() != Constants.NULL_ID_INT
          || reference.getPersistentLogKey() != Constants.NULL_ID_LONG)) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  /**
   * Serialize page references into output.
   *
   * @param out  output stream
   * @param type the type to serialize (transaction intent log or the data file
   *             itself).
   */
  @Override
  public void serialize(final Bytes<ByteBuffer> out, final SerializationType type) {
    assert out != null;
    assert type != null;
    type.serializeFullReferencesPage(out, references);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final PageReference ref : references) {
      helper.add("reference", ref);
    }
    return helper.toString();
  }
}
