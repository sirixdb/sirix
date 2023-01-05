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

package org.sirix.page;

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.DatabaseType;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.cache.Cache;
import org.sirix.cache.NamesCacheKey;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.index.name.Names;
import org.sirix.node.NodeKind;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.nio.ByteBuffer;

/**
 * The name page holds all names and their keys for a revision. Furthermore it has references to name indexes.
 */
public final class NamePage extends AbstractForwardingPage {

  /**
   * Offset of reference to attributes index-tree.
   */
  public static final int ATTRIBUTES_REFERENCE_OFFSET = 0;

  /**
   * Offset of reference to elements index-tree.
   */
  public static final int ELEMENTS_REFERENCE_OFFSET = 1;

  /**
   * Offset of reference to namespace index-tree.
   */
  public static final int NAMESPACE_REFERENCE_OFFSET = 2;

  /**
   * Offset of reference to processing instruction index-tree.
   */
  public static final int PROCESSING_INSTRUCTION_REFERENCE_OFFSET = 3;

  /**
   * Offset of reference to processing instruction index-tree.
   */
  public static final int JSON_OBJECT_KEY_REFERENCE_OFFSET = 0;

  /**
   * Attribute names.
   */
  private Names attributes;

  /**
   * Element names.
   */
  private Names elements;

  /**
   * Namespace URIs.
   */
  private Names namespaces;

  /**
   * Processing instruction names.
   */
  private Names processingInstructions;

  /**
   * JSON Object key names.
   */
  private Names jsonObjectKeys;

  /**
   * The references page delegate instance.
   */
  private Page delegate;

  /**
   * The number of arrays stored.
   */
  private final int numberOfArrays;

  /**
   * Maximum node keys.
   */
  private final Int2LongMap maxNodeKeys;

  /**
   * Current maximum levels of indirect pages in the tree.
   */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Create name page.
   */
  public NamePage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    attributes = Names.getInstance(ATTRIBUTES_REFERENCE_OFFSET);
    elements = Names.getInstance(ELEMENTS_REFERENCE_OFFSET);
    namespaces = Names.getInstance(NAMESPACE_REFERENCE_OFFSET);
    processingInstructions = Names.getInstance(PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
    jsonObjectKeys = Names.getInstance(JSON_OBJECT_KEY_REFERENCE_OFFSET);
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
    numberOfArrays = 0;
  }

  /**
   * Read name page.
   *
   * @param in input bytes to read from
   */
  NamePage(final Bytes<ByteBuffer> in, final SerializationType type) {
    delegate = PageUtils.createDelegate(in, type);
    final int size = in.readInt();
    maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(size / 0.75));
    for (int i = 0; i < size; i++) {
      maxNodeKeys.put(i, in.readLong());
    }

    numberOfArrays = in.readInt();
    final int currentMaxLevelOfIndirectPages = in.readInt();
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPages / 0.75));
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      currentMaxLevelsOfIndirectPages.put(i, in.readByte() & 0xFF);
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
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        rawName = elements.getRawName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        rawName = namespaces.getRawName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        rawName = attributes.getRawName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        rawName = processingInstructions.getRawName(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        rawName = jsonObjectKeys.getRawName(key);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
    return rawName;
  }

  private Names getNames(PageReadOnlyTrx pageRtx, int offset) {
    final Cache<NamesCacheKey, Names> namesCache = pageRtx.getBufferManager().getNamesCache();
    final NamesCacheKey namesCacheKey = new NamesCacheKey(pageRtx.getRevisionNumber(), offset);
    Names names = namesCache.get(namesCacheKey);

    if (names == null) {
      names =
          Names.clone(pageRtx, offset, maxNodeKeys.getOrDefault(offset, 0L));
      namesCache.put(namesCacheKey, names);
    }

    return names;
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key, or {@code null} if not present
   */
  public String getName(final int key, @NonNull final NodeKind nodeKind, final PageReadOnlyTrx pageRtx) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getName(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getName(key);
      }
      case ARRAY -> "__array__";
      case OBJECT -> "__object__";
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Get number of nodes with the given name key.
   *
   * @param key name key identifying name
   * @return number of nodes with the given name key
   */
  public int getCount(final int key, @NonNull final NodeKind nodeKind, final PageReadOnlyTrx pageRtx) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getCount(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getCount(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getCount(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getCount(key);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getCount(key);
      }
      case ARRAY -> numberOfArrays;
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Create name key given a name.
   *
   * @param name     name to create key for
   * @param nodeKind kind of node
   * @return the created key
   */
  public int setName(final String name, final NodeKind nodeKind, final PageTrx pageRtx) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        return elements.setName(name, pageRtx);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        return namespaces.setName(name, pageRtx);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        return attributes.setName(name, pageRtx);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        return processingInstructions.setName(name, pageRtx);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        return jsonObjectKeys.setName(name, pageRtx);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  @Override
  public void serialize(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> out,
      final SerializationType type) {
    if (delegate instanceof ReferencesPage4) {
      out.writeByte((byte) 0);
    } else if (delegate instanceof BitmapReferencesPage) {
      out.writeByte((byte) 1);
    }
    super.serialize(pageReadOnlyTrx, out, type);
    final int size = maxNodeKeys.size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      final long keys = maxNodeKeys.get(i);
      out.writeLong(keys);
    }
    out.writeInt(numberOfArrays);
    final int currentMaxLevelOfIndirectPages = maxNodeKeys.size();
    out.writeInt(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      out.writeByte((byte) currentMaxLevelsOfIndirectPages.get(i));
    }
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("elements", elements)
                      .add("attributes", attributes)
                      .add("URIs", namespaces)
                      .add("PIs", processingInstructions)
                      .toString();
  }

  /**
   * Remove an attribute-name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final NodeKind nodeKind, final PageTrx pageRtx) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(pageRtx, ELEMENTS_REFERENCE_OFFSET);
        }
        elements.removeName(key, pageRtx);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(pageRtx, NAMESPACE_REFERENCE_OFFSET);
        }
        namespaces.removeName(key, pageRtx);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(pageRtx, ATTRIBUTES_REFERENCE_OFFSET);
        }
        attributes.removeName(key, pageRtx);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(pageRtx, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        processingInstructions.removeName(key, pageRtx);
      }
      case OBJECT_KEY -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(pageRtx, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        jsonObjectKeys.removeName(key, pageRtx);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Initialize name index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param index        the index number
   * @param log          the transaction intent log
   */
  public void createNameIndexTree(final DatabaseType databaseType, final PageReadOnlyTrx pageReadTrx, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.NAME, pageReadTrx, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
    }
  }

  /**
   * Get indirect page reference.
   *
   * @param offset the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(final int offset) {
    return getOrCreateReference(offset);
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNumber the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNumber) {
    return maxNodeKeys.get(indexNumber);
  }

  public long incrementAndGetMaxNodeKey(final int indexNumber) {
    final long newMaxNodeKey = maxNodeKeys.getOrDefault(indexNumber, 0L) + 1;
    maxNodeKeys.put(indexNumber, newMaxNodeKey);
    return newMaxNodeKey;
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }
}
