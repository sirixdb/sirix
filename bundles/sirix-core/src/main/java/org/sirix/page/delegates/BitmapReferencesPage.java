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

package org.sirix.page.delegates;

import com.google.common.base.MoreObjects;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.magicwerk.brownies.collections.GapList;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.page.DeserializedBitmapReferencesPageTuple;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Class to provide basic reference handling functionality.
 */
public final class BitmapReferencesPage implements Page {

  public static final int THRESHOLD = Constants.INP_REFERENCE_COUNT - 16;

  /**
   * Page references.
   */
  private final List<PageReference> references;

  /**
   * The bitmap to use, which indexes are null/not null in the references array.
   */
  private final BitSet bitmap;

  /**
   * Constructor to initialize instance.
   *
   * @param numberOfEntriesAtMax number of entries at maximum
   * @param pageToCopy           the page to copy references from
   */
  public BitmapReferencesPage(final int numberOfEntriesAtMax, final ReferencesPage4 pageToCopy) {
    checkArgument(numberOfEntriesAtMax >= 0);

    references = new GapList<>(8);
    bitmap = new BitSet(numberOfEntriesAtMax);

    final var offsets = pageToCopy.getOffsets();
    final var pageReferences = pageToCopy.getReferences();

    for (int i = 0, size = offsets.size(); i < size; i++) {
      setOrCreateReference(offsets.get(i), pageReferences.get(i));
    }
  }

  /**
   * Constructor to initialize instance.
   *
   * @param referenceCount number of references of page
   */
  public BitmapReferencesPage(final @NonNegative int referenceCount) {
    checkArgument(referenceCount >= 0);

    final int initialSize;

    if (referenceCount == Constants.UBPINP_REFERENCE_COUNT || referenceCount == Constants.INP_REFERENCE_COUNT
        || referenceCount == Constants.PATHINP_REFERENCE_COUNT || referenceCount == Constants.NDP_NODE_COUNT) {
      /*
       * Currently backing array has an initial size of 8. Thus for the last layer of indirect pages
       * it has to resize the first time after 8 record pages, that is 512 * 8 records.
       */
      initialSize = referenceCount >> 6;
    } else {
      // All pages which have less references are fully set (UberPage, RevisionRootPage...).
      initialSize = referenceCount;
    }

    references = new GapList<>(initialSize);
    bitmap = new BitSet(referenceCount);
  }

  /**
   * Constructor to initialize instance.
   *
   * @param referenceCount number of references of page
   * @param in             input stream to read from
   * @param type           the serialization type
   */
  public BitmapReferencesPage(final @NonNegative int referenceCount, final Bytes<ByteBuffer> in,
      final SerializationType type) {
    final DeserializedBitmapReferencesPageTuple tuple = type.deserializeBitmapReferencesPage(referenceCount, in);
    references = tuple.getReferences();
    bitmap = tuple.getBitmap();
  }

  /**
   * Constructor to initialize instance.
   *
   * @param pageToClone commited page
   */
  public BitmapReferencesPage(final Page pageToClone, final BitSet bitSet) {
    bitmap = (BitSet) bitSet.clone();

    final int length = pageToClone.getReferences().size();

    references = new GapList<>(length);

    for (int offset = 0; offset < length; offset++) {
      final PageReference pageReference = new PageReference();
      final var pageReferenceToClone = pageToClone.getReferences().get(offset);
      pageReference.setKey(pageReferenceToClone.getKey());
      pageReference.setLogKey(pageReferenceToClone.getLogKey());
      pageReference.setPageFragments(new ArrayList<>(pageReferenceToClone.getPageFragments()));
      pageReference.setPersistentLogKey(pageReferenceToClone.getPersistentLogKey());
      references.add(offset, pageReference);
    }
  }

  @Override
  public List<PageReference> getReferences() {
    return references;
  }

  public BitSet getBitmap() {
    return (BitSet) bitmap.clone();
  }

  /**
   * Get page reference of given offset.
   *
   * @param offset offset of page reference
   * @return {@link PageReference} at given offset
   */
  @Override
  public PageReference getOrCreateReference(final @NonNegative int offset) {
    if (bitmap.get(offset)) {
      final int index = index(offset);
      return references.get(index);
    } else {
      return createNewReference(offset);
    }
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    final int index = index(offset);
    if (!bitmap.get(offset)) {
      references.add(index, pageReference);
    } else {
      references.set(index, pageReference);
    }
    bitmap.set(index, true);

    return bitmap.cardinality() == THRESHOLD;
  }

  private PageReference createNewReference(final int offset) {
    final int index = index(offset);
    final PageReference pageReference = new PageReference();
    references.add(index, pageReference);
    bitmap.set(offset, true);

    if (bitmap.cardinality() == THRESHOLD) {
      return null;
    }

    return pageReference;
  }

  private int index(final int offset) {
    BitSet offsetBitmap = new BitSet(bitmap.size());

    offsetBitmap.set(offset);

    // Flip from 0 to offset.
    offsetBitmap.flip(0, offset + 1);

    offsetBitmap.and(bitmap);

    final var cardinality =  offsetBitmap.cardinality();
    offsetBitmap = null;
    return cardinality;
  }

  @Override
  public void serialize(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> out, final SerializationType type) {
    assert out != null;
    assert type != null;

    type.serializeBitmapReferencesPage(out, references, bitmap);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final PageReference ref : references) {
      helper.add("reference", ref);
    }
    helper.add("bitmap", dumpBitmap(bitmap));
    return helper.toString();
  }

  private static String dumpBitmap(final BitSet bitmap) {
    final StringBuilder s = new StringBuilder();

    for (int i = 0; i < bitmap.length(); i++) {
      s.append(bitmap.get(i) ? 1 : 0);
    }

    return s.toString();
  }

  @Override
  public Page clearPage() {
    references.clear();
    bitmap.clear();
    return this;
  }
}
