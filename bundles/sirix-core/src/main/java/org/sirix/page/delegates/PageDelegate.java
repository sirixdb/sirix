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

package org.sirix.page.delegates;

import static com.google.common.base.Preconditions.checkArgument;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import javax.annotation.Nonnegative;
import org.magicwerk.brownies.collections.GapList;
import org.sirix.api.PageWriteTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.DeserializedTuple;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * <h1>PageDelegate</h1>
 *
 * <p>
 * Class to provide basic reference handling functionality.
 * </p>
 */
public final class PageDelegate implements Page {

  /** Page references. */
  private final List<PageReference> mReferences;

  /** The bitmap to use, which indexes are null/not null in the references array. */
  private final BitSet mBitmap;

  /** The bitmap to use to check for an index offset. */
  private final BitSet mOffsetBitmap;

  /**
   * Constructor to initialize instance.
   *
   * @param referenceCount number of references of page
   */
  public PageDelegate(final @Nonnegative int referenceCount) {
    checkArgument(referenceCount >= 0);

    final int initialSize;

    if (referenceCount == Constants.INP_REFERENCE_COUNT
        || referenceCount == Constants.PATHINP_REFERENCE_COUNT
        || referenceCount == Constants.NDP_NODE_COUNT)
      /*
       * Currently backing array has an initial size of 8. Thus for the last layer of indirect pages
       * it has to resize the first time after 8 record pages, that is 512 * 8 records.
       */
      initialSize = referenceCount >> 6;
    else
      // All pages which have less references are fully set (UberPage, RevisionRootPage...).
      initialSize = referenceCount;

    mReferences = new GapList<>(initialSize);
    mBitmap = new BitSet(referenceCount);
    mOffsetBitmap = new BitSet(referenceCount);
  }

  /**
   * Constructor to initialize instance.
   *
   * @param referenceCount number of references of page
   * @param in input stream to read from
   * @param type the serialization type
   * @throws IOException if the delegate couldn't be deserialized
   */
  public PageDelegate(final @Nonnegative int referenceCount, final DataInput in,
      final SerializationType type) {
    final DeserializedTuple tuple = type.deserialize(referenceCount, in);
    mReferences = tuple.getReferences();
    mBitmap = tuple.getBitmap();
    mOffsetBitmap = new BitSet(referenceCount);
  }

  /**
   * Constructor to initialize instance.
   *
   * @param commitedPage commited page
   */
  public PageDelegate(final Page commitedPage, final BitSet bitSet) {
    mBitmap = (BitSet) bitSet.clone();
    mOffsetBitmap = new BitSet(mBitmap.size());

    final int length = commitedPage.getReferences().size();

    mReferences = new GapList<>(length);

    for (int offset = 0; offset < length; offset++) {
      final PageReference reference = new PageReference();
      reference.setKey(commitedPage.getReferences().get(offset).getKey());
      mReferences.add(offset, reference);
    }
  }

  @Override
  public List<PageReference> getReferences() {
    return mReferences;
  }

  public BitSet getBitmap() {
    return (BitSet) mBitmap.clone();
  }

  /**
   * Get page reference of given offset.
   *
   * @param offset offset of page reference
   * @return {@link PageReference} at given offset
   */
  @Override
  public final PageReference getReference(final @Nonnegative int offset) {
    if (mBitmap.get(offset)) {
      final int index = index(offset);
      return mReferences.get(index);
    } else {
      return createNewReference(offset);
    }
  }

  private PageReference createNewReference(final int offset) {
    final int index = index(offset);

    final PageReference reference = new PageReference();
    mReferences.add(index, reference);

    mBitmap.set(offset, true);

    return reference;
  }

  private int index(final int offset) {
    mOffsetBitmap.set(offset);

    // Flip 0 to offset.
    mOffsetBitmap.flip(0, offset + 1);

    mOffsetBitmap.and(mBitmap);

    final int cardinality = mOffsetBitmap.cardinality();

    mOffsetBitmap.clear();

    return cardinality;
  }

  /**
   * Recursively call commit on all referenced pages.
   *
   * @param pageWriteTransaction the page write transaction
   */
  @Override
  public final <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      final PageWriteTrx<K, V, S> pageWriteTrx) {
    for (final PageReference reference : mReferences) {
      if (reference.getLogKey() != Constants.NULL_ID_INT
          || reference.getPersistentLogKey() != Constants.NULL_ID_LONG) {
        pageWriteTrx.commit(reference);
      }
    }
  }

  /**
   * Serialize page references into output.
   *
   * @param out output stream
   * @param serializationType the type to serialize (transaction intent log or the data file
   *        itself).
   */
  @Override
  public void serialize(final DataOutput out, final SerializationType type) {
    assert out != null;
    assert type != null;

    type.serialize(out, mReferences, mBitmap);
  }

  @Override
  public String toString() {
    final MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
    for (final PageReference ref : mReferences) {
      helper.add("reference", ref);
    }
    helper.add("bitmap", dumpBitmap(mBitmap));
    return helper.toString();
  }

  private String dumpBitmap(final BitSet bitmap) {
    final StringBuilder s = new StringBuilder();

    for (int i = 0; i < bitmap.length(); i++) {
      s.append(
          bitmap.get(i) == true
              ? 1
              : 0);
    }

    return s.toString();
  }
}
