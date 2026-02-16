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

package io.sirix.page.delegates;

import com.google.common.base.MoreObjects;
import io.sirix.node.BytesIn;
import org.checkerframework.checker.index.qual.NonNegative;
import org.magicwerk.brownies.collections.GapList;
import io.sirix.page.DeserializedBitmapReferencesPageTuple;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

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
   * Cached long[] representation of the bitmap for efficient popcount operations.
   * Invalidated (set to null) whenever the bitmap is modified.
   */
  private long[] cachedWords;

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
      setOrCreateReference(offsets.getShort(i), pageReferences.get(i));
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

    if (referenceCount == Constants.NDP_NODE_COUNT) {
      /*
       * Currently backing array has an initial size of 8. Thus, for the last layer of indirect pages
       * it has to resize the first time after 8 record pages, that is 512 * 8 records.
       */
      initialSize = referenceCount >> 6;
    } else {
      // All pages which have fewer references are fully set (UberPage, RevisionRootPage...).
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
  public BitmapReferencesPage(final @NonNegative int referenceCount, final BytesIn<?> in,
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
      pageReference.setPage(pageReferenceToClone.getPage());
      pageReference.setLogKey(pageReferenceToClone.getLogKey());
      pageReference.setActiveTilGeneration(pageReferenceToClone.getActiveTilGeneration());
      pageReference.setDatabaseId(pageReferenceToClone.getDatabaseId());
      pageReference.setResourceId(pageReferenceToClone.getResourceId());
      pageReference.setPageFragments(new ArrayList<>(pageReferenceToClone.getPageFragments()));
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
      bitmap.set(offset, true);  // Fixed: was incorrectly using 'index' instead of 'offset'
      cachedWords = null;        // Invalidate cache after bitmap modification
    } else {
      references.set(index, pageReference);
    }

    return bitmap.cardinality() == THRESHOLD;
  }

  private PageReference createNewReference(final int offset) {
    final int index = index(offset);
    final PageReference pageReference = new PageReference();
    references.add(index, pageReference);
    bitmap.set(offset, true);
    cachedWords = null;  // Invalidate cache after bitmap modification

    if (bitmap.cardinality() == THRESHOLD) {
      return null;
    }

    return pageReference;
  }

  /**
   * Compute the dense index for a given sparse offset.
   * 
   * <p>This method counts how many bits are set in the bitmap from position 0 to offset-1,
   * which gives the index into the dense references list.</p>
   * 
   * <p>Uses POPCNT-based counting for O(offset/64) complexity instead of O(offset).
   * Each {@link Long#bitCount} call compiles to a single POPCNT instruction (~1 cycle).</p>
   *
   * @param offset the sparse offset (0-1023)
   * @return the dense index into the references list
   */
  private int index(final int offset) {
    final long[] words = getWords();
    int count = 0;
    final int wordIndex = offset >>> 6;      // offset / 64
    final int bitInWord = offset & 63;       // offset % 64
    
    // Count all set bits in complete words before the target word
    for (int i = 0; i < wordIndex && i < words.length; i++) {
      count += Long.bitCount(words[i]);
    }
    
    // Count set bits in the target word up to (not including) offset
    if (wordIndex < words.length) {
      // Mask: bits 0 to bitInWord-1 are 1, rest are 0
      final long mask = (1L << bitInWord) - 1;
      count += Long.bitCount(words[wordIndex] & mask);
    }
    
    return count;
  }

  /**
   * Get the cached long[] representation of the bitmap.
   * Lazily creates the cache on first access.
   *
   * @return the long[] array backing the bitmap
   */
  private long[] getWords() {
    if (cachedWords == null) {
      cachedWords = bitmap.toLongArray();
    }
    return cachedWords;
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
  public void close() {
    references.clear();
    bitmap.clear();
    cachedWords = null;
  }
}
