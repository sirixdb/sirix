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

package org.sirix.page;

import com.google.common.base.MoreObjects;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.name.Names;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <h1>NamePage</h1>
 *
 * <p>
 * Name page holds all names and their keys for a revision.
 * </p>
 */
public final class NamePage extends AbstractForwardingPage {

  /** Offset of reference to attributes index-tree. */
  public static final int ATTRIBUTES_REFERENCE_OFFSET = 0;

  /** Offset of reference to elements index-tree. */
  public static final int ELEMENTS_REFERENCE_OFFSET = 1;

  /** Offset of reference to namespace index-tree. */
  public static final int NAMESPACE_REFERENCE_OFFSET = 2;

  /** Offset of reference to processing instruction index-tree. */
  public static final int PROCESSING_INSTRUCTION_REFERENCE_OFFSET = 3;

  /** Offset of reference to processing instruction index-tree. */
  public static final int JSON_OBJECT_KEY_REFERENCE_OFFSET = 0;

  /** Attribute names. */
  private Names mAttributes;

  /** Element names. */
  private Names mElements;

  /** Namespace URIs. */
  private Names mNamespaces;

  /** Processing instruction names. */
  private Names mPIs;

  /** JSON Object key names. */
  private Names mJSONObjectKeys;

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /** The number of arrays stored. */
  private int mNumberOfArrays;

  /** Maximum node keys. */
  private final Map<Integer, Long> mMaxNodeKeys;

  /** Current maximum levels of indirect pages in the tree. */
  private final Map<Integer, Integer> mCurrentMaxLevelsOfIndirectPages;

  /**
   * Create name page.
   */
  public NamePage() {
    mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR);
    mMaxNodeKeys = new HashMap<>();
    mAttributes = Names.getInstance(ATTRIBUTES_REFERENCE_OFFSET);
    mElements = Names.getInstance(ELEMENTS_REFERENCE_OFFSET);
    mNamespaces = Names.getInstance(NAMESPACE_REFERENCE_OFFSET);
    mPIs = Names.getInstance(PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
    mJSONObjectKeys = Names.getInstance(JSON_OBJECT_KEY_REFERENCE_OFFSET);
    mCurrentMaxLevelsOfIndirectPages = new HashMap<>();
    mNumberOfArrays = 0;
  }

  /**
   * Read name page.
   *
   * @param in input bytes to read from
   */
  protected NamePage(final DataInput in, final SerializationType type) throws IOException {
    mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR, in, type);
    final int size = in.readInt();
    mMaxNodeKeys = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      mMaxNodeKeys.put(i, in.readLong());
    }

    mNumberOfArrays = in.readInt();
    final int currentMaxLevelOfIndirectPages = in.readInt();
    mCurrentMaxLevelsOfIndirectPages = new HashMap<>(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      mCurrentMaxLevelsOfIndirectPages.put(i, in.readByte() & 0xFF);
    }
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key
   */
  public byte[] getRawName(final int key, final NodeKind nodeKind, final PageReadOnlyTrx pageRtx) {
    final byte[] rawName;
    switch (nodeKind) {
      case ELEMENT:
        if (mElements == null) {
          mElements = Names.clone(pageRtx, ELEMENTS_REFERENCE_OFFSET, mMaxNodeKeys.get(ELEMENTS_REFERENCE_OFFSET));
        }
        rawName = mElements.getRawName(key);
        break;
      case NAMESPACE:
        if (mNamespaces == null) {
          mNamespaces = Names.clone(pageRtx, NAMESPACE_REFERENCE_OFFSET, mMaxNodeKeys.get(NAMESPACE_REFERENCE_OFFSET));
        }
        rawName = mNamespaces.getRawName(key);
        break;
      case ATTRIBUTE:
        if (mAttributes == null) {
          mAttributes =
              Names.clone(pageRtx, ATTRIBUTES_REFERENCE_OFFSET, mMaxNodeKeys.get(ATTRIBUTES_REFERENCE_OFFSET));
        }
        rawName = mAttributes.getRawName(key);
        break;
      case PROCESSING_INSTRUCTION:
        if (mPIs == null) {
          mPIs = Names.clone(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET,
              mMaxNodeKeys.get(PROCESSING_INSTRUCTION_REFERENCE_OFFSET));
        }
        rawName = mPIs.getRawName(key);
        break;
      case OBJECT_KEY:
        if (mJSONObjectKeys == null) {
          mJSONObjectKeys = Names.clone(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET,
              mMaxNodeKeys.get(JSON_OBJECT_KEY_REFERENCE_OFFSET));
        }
        rawName = mJSONObjectKeys.getRawName(key);
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("No other node types supported!");
    }
    return rawName;
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key, or {@code null} if not present
   */
  public String getName(final int key, @Nonnull final NodeKind nodeKind, final PageReadOnlyTrx pageRtx) {
    final String name;
    switch (nodeKind) {
      case ELEMENT:
        if (mElements == null) {
          mElements = Names.clone(pageRtx, ELEMENTS_REFERENCE_OFFSET, mMaxNodeKeys.get(ELEMENTS_REFERENCE_OFFSET));
        }
        name = mElements.getName(key);
        break;
      case NAMESPACE:
        if (mNamespaces == null) {
          mNamespaces = Names.clone(pageRtx, NAMESPACE_REFERENCE_OFFSET, mMaxNodeKeys.get(NAMESPACE_REFERENCE_OFFSET));
        }
        name = mNamespaces.getName(key);
        break;
      case ATTRIBUTE:
        if (mAttributes == null) {
          mAttributes =
              Names.clone(pageRtx, ATTRIBUTES_REFERENCE_OFFSET, mMaxNodeKeys.get(ATTRIBUTES_REFERENCE_OFFSET));
        }
        name = mAttributes.getName(key);
        break;
      case PROCESSING_INSTRUCTION:
        if (mPIs == null) {
          mPIs = Names.clone(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET,
              mMaxNodeKeys.get(PROCESSING_INSTRUCTION_REFERENCE_OFFSET));
        }
        name = mPIs.getName(key);
        break;
      case OBJECT_KEY:
        if (mJSONObjectKeys == null) {
          mJSONObjectKeys = Names.clone(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET,
              mMaxNodeKeys.get(JSON_OBJECT_KEY_REFERENCE_OFFSET));
        }
        name = mJSONObjectKeys.getName(key);
        break;
      case ARRAY:
        name = "$$array$$";
        break;
      case OBJECT:
        name = "$$object$$";
        // $CASES-OMITTED$
      default:
        throw new IllegalStateException("No other node types supported!");
    }
    return name;
  }

  /**
   * Get number of nodes with the given name key.
   *
   * @param key name key identifying name
   * @return number of nodes with the given name key
   */
  public int getCount(final int key, @Nonnull final NodeKind nodeKind, final PageReadOnlyTrx pageRtx) {
    int count;
    switch (nodeKind) {
      case ELEMENT:
        if (mElements == null) {
          mElements = Names.clone(pageRtx, ELEMENTS_REFERENCE_OFFSET, mMaxNodeKeys.get(ELEMENTS_REFERENCE_OFFSET));
        }
        count = mElements.getCount(key);
        break;
      case NAMESPACE:
        if (mNamespaces == null) {
          mNamespaces = Names.clone(pageRtx, NAMESPACE_REFERENCE_OFFSET, mMaxNodeKeys.get(NAMESPACE_REFERENCE_OFFSET));
        }
        count = mNamespaces.getCount(key);
        break;
      case ATTRIBUTE:
        if (mAttributes == null) {
          mAttributes =
              Names.clone(pageRtx, ATTRIBUTES_REFERENCE_OFFSET, mMaxNodeKeys.get(ATTRIBUTES_REFERENCE_OFFSET));
        }
        count = mAttributes.getCount(key);
        break;
      case PROCESSING_INSTRUCTION:
        if (mPIs == null) {
          mPIs = Names.clone(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET,
              mMaxNodeKeys.get(PROCESSING_INSTRUCTION_REFERENCE_OFFSET));
        }
        count = mPIs.getCount(key);
        break;
      case OBJECT_KEY:
        if (mJSONObjectKeys == null) {
          mJSONObjectKeys = Names.clone(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET,
              mMaxNodeKeys.get(JSON_OBJECT_KEY_REFERENCE_OFFSET));
        }
        count = mJSONObjectKeys.getCount(key);
        break;
      case ARRAY:
        count = mNumberOfArrays;
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("No other node types supported!");
    }
    return count;
  }

  /**
   * Create name key given a name.
   *
   * @param name name to create key for
   * @param nodeKind kind of node
   * @return the created key
   */
  public int setName(final String name, final NodeKind nodeKind,
      final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx) {
    switch (nodeKind) {
      case ELEMENT:
        if (mElements == null) {
          mElements = Names.clone(pageTrx, ELEMENTS_REFERENCE_OFFSET, mMaxNodeKeys.get(ELEMENTS_REFERENCE_OFFSET));
        }
        return mElements.setName(name, pageTrx);
      case NAMESPACE:
        if (mNamespaces == null) {
          mNamespaces = Names.clone(pageTrx, NAMESPACE_REFERENCE_OFFSET, mMaxNodeKeys.get(NAMESPACE_REFERENCE_OFFSET));
        }
        return mNamespaces.setName(name, pageTrx);
      case ATTRIBUTE:
        if (mAttributes == null) {
          mAttributes =
              Names.clone(pageTrx, ATTRIBUTES_REFERENCE_OFFSET, mMaxNodeKeys.get(ATTRIBUTES_REFERENCE_OFFSET));
        }
        return mAttributes.setName(name, pageTrx);
      case PROCESSING_INSTRUCTION:
        if (mPIs == null) {
          mPIs = Names.clone(pageTrx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET,
              mMaxNodeKeys.get(PROCESSING_INSTRUCTION_REFERENCE_OFFSET));
        }
        return mPIs.setName(name, pageTrx);
      case OBJECT_KEY:
        if (mJSONObjectKeys == null) {
          mJSONObjectKeys = Names.clone(pageTrx, JSON_OBJECT_KEY_REFERENCE_OFFSET,
              mMaxNodeKeys.get(JSON_OBJECT_KEY_REFERENCE_OFFSET));
        }
        return mJSONObjectKeys.setName(name, pageTrx);
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("No other node types supported!");
    }
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    super.serialize(out, type);
    final int size = mMaxNodeKeys.size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      final long keys = mMaxNodeKeys.get(i);
      out.writeLong(keys);
    }
    out.writeInt(mNumberOfArrays);
    final int currentMaxLevelOfIndirectPages = mMaxNodeKeys.size();
    out.writeInt(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      out.writeByte(mCurrentMaxLevelsOfIndirectPages.get(i));
    }
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return mCurrentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return mCurrentMaxLevelsOfIndirectPages.merge(index, 1, (previousValue, value) -> previousValue + value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("elements", mElements)
                      .add("attributes", mAttributes)
                      .add("URIs", mNamespaces)
                      .add("PIs", mPIs)
                      .toString();
  }

  /**
   * Remove an attribute-name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final NodeKind nodeKind,
      final PageTrx<Long, Record, UnorderedKeyValuePage> pageTrx) {
    switch (nodeKind) {
      case ELEMENT:
        if (mElements == null) {
          mElements = Names.clone(pageTrx, ELEMENTS_REFERENCE_OFFSET, mMaxNodeKeys.get(ELEMENTS_REFERENCE_OFFSET));
        }
        mElements.removeName(key, pageTrx);
        break;
      case NAMESPACE:
        if (mNamespaces == null) {
          mNamespaces = Names.clone(pageTrx, NAMESPACE_REFERENCE_OFFSET, mMaxNodeKeys.get(NAMESPACE_REFERENCE_OFFSET));
        }
        mNamespaces.removeName(key, pageTrx);
        break;
      case ATTRIBUTE:
        if (mAttributes == null) {
          mAttributes =
              Names.clone(pageTrx, ATTRIBUTES_REFERENCE_OFFSET, mMaxNodeKeys.get(ATTRIBUTES_REFERENCE_OFFSET));
        }
        mAttributes.removeName(key, pageTrx);
        break;
      case PROCESSING_INSTRUCTION:
        if (mPIs == null) {
          mPIs = Names.clone(pageTrx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET,
              mMaxNodeKeys.get(PROCESSING_INSTRUCTION_REFERENCE_OFFSET));
        }
        mPIs.removeName(key, pageTrx);
        break;
      case OBJECT_KEY:
        if (mJSONObjectKeys == null) {
          mJSONObjectKeys = Names.clone(pageTrx, JSON_OBJECT_KEY_REFERENCE_OFFSET,
              mMaxNodeKeys.get(JSON_OBJECT_KEY_REFERENCE_OFFSET));
        }
        mJSONObjectKeys.removeName(key, pageTrx);
        break;
      // $CASES-OMITTED$
      default:
        throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Initialize name index tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createNameIndexTree(final PageReadOnlyTrx pageReadTrx, final int index, final TransactionIntentLog log) {
    final PageReference reference = getReference(index);
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(reference, PageKind.NAMEPAGE, index, pageReadTrx, log);
      if (mMaxNodeKeys.get(index) == null) {
        mMaxNodeKeys.put(index, 0L);
      } else {
        mMaxNodeKeys.put(index, mMaxNodeKeys.get(index).longValue() + 1);
      }
      if (mCurrentMaxLevelsOfIndirectPages.get(index) == null) {
        mCurrentMaxLevelsOfIndirectPages.put(index, 1);
      } else {
        mCurrentMaxLevelsOfIndirectPages.put(index, mCurrentMaxLevelsOfIndirectPages.get(index) + 1);
      }
    }
  }

  /**
   * Get indirect page reference.
   *
   * @param offset the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(final int offset) {
    return getReference(offset);
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNumber the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNumber) {
    return mMaxNodeKeys.get(indexNumber);
  }

  public long incrementAndGetMaxNodeKey(final int indexNumber) {
    final long newMaxNodeKey = mMaxNodeKeys.get(indexNumber).longValue() + 1;
    mMaxNodeKeys.put(indexNumber, newMaxNodeKey);
    return newMaxNodeKey;
  }

  @Override
  protected Page delegate() {
    return mDelegate;
  }
}
