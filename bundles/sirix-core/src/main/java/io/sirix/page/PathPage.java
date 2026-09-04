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

package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Container page for path indexes.
 *
 * <p>
 * Each occupied delegate slot roots one versioned HOT tree. The sparse {@code maxHotPageKeys} map
 * is the only per-index metadata: it persists each tree's HOT page-key allocator without retaining
 * metadata from the removed keyed-trie secondary-index format.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class PathPage extends AbstractForwardingPage {

  /**
   * The references page instance.
   */
  private Page delegate;

  /**
   * Maximum HOT page keys per index number. Used by the canonical HOT index writer for persistent
   * page key allocation across transactions.
   */
  private final Int2LongMap maxHotPageKeys;

  /**
   * Constructor.
   */
  public PathPage() {
    delegate = new ReferencesPage4();
    maxHotPageKeys = new Int2LongOpenHashMap();
  }

  /**
   * Get or create the path HOT-tree root reference.
   *
   * @param offset the physical index number
   * @return HOT-tree root reference
   */
  public PageReference getIndirectPageReference(final int offset) {
    return getOrCreateIndexReference(offset);
  }

  /**
   * Read meta page.
   *
   * @param delegate The references page instance.
   * @param maxHotPageKeys maximum HOT page keys per index
   */
  public PathPage(final Page delegate, final Int2LongMap maxHotPageKeys) {
    this.delegate = requireNonNull(delegate);
    this.maxHotPageKeys = requireValidAllocatorMap(maxHotPageKeys);
  }

  /**
   * Copy constructor for write-side CoW. Deep-copies the delegate's reference array and the allocator
   * map so writer-side mutations don't bleed into the historical revision's view.
   */
  public PathPage(final PathPage other) {
    final Page otherDelegate = other.delegate;
    if (otherDelegate instanceof ReferencesPage4 ref) {
      this.delegate = new ReferencesPage4(ref);
    } else if (otherDelegate instanceof BitmapReferencesPage bmp) {
      this.delegate = new BitmapReferencesPage(otherDelegate, bmp.getBitmap());
    } else if (otherDelegate instanceof FullReferencesPage full) {
      this.delegate = new FullReferencesPage(full);
    } else {
      throw new IllegalStateException(
          "Unknown PathPage delegate type, cannot clone: " + otherDelegate.getClass().getName());
    }
    this.maxHotPageKeys = new Int2LongOpenHashMap(other.maxHotPageKeys);
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    checkIndex(offset);
    delegate = PageUtils.setReference(delegate, offset, pageReference);
    return false;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize the path index's HOT tree.
   *
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createPathIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getOrCreateIndexReference(index);
    if (reference.isVirginStructuralPlaceholder()) {
      refuseAllocatorOnlyState(index);
      PageUtils.createHOTTree(reference, IndexType.PATH, storageEngineReader, log);
    }
  }

  /**
   * Determine whether a physical path tree id has ever been initialized without creating a structural
   * reference.
   *
   * @param index the physical index number
   * @return {@code true} if allocator metadata or a non-virgin root reserves the id
   */
  public boolean isIndexInitialized(final int index) {
    checkIndex(index);
    return isIndexInitializedUnchecked(index);
  }

  /** Return the first physical path tree id that has never been initialized. */
  public int nextUnallocatedIndex() {
    return nextUnallocatedIndex(0);
  }

  /**
   * Return the first uninitialized physical path tree id at or after {@code fromInclusive}.
   *
   * <p>
   * The scan is read-only: neither sparse nor full delegates gain placeholder references.
   * </p>
   *
   * @param fromInclusive first physical id to inspect
   * @return the first uninitialized id
   * @throws IllegalStateException if the remaining reference space is exhausted
   */
  public int nextUnallocatedIndex(final int fromInclusive) {
    checkIndex(fromInclusive);
    for (int index = fromInclusive; index < Constants.INP_REFERENCE_COUNT; index++) {
      if (!isIndexInitializedUnchecked(index)) {
        return index;
      }
    }
    throw new IllegalStateException("Path index reference space exhausted at or after " + fromInclusive + ": "
        + Constants.INP_REFERENCE_COUNT + " physical ids available");
  }

  /**
   * Get the maximum HOT page key of the specified index by its index number.
   *
   * @param indexNo the index number
   * @return the maximum HOT page key stored
   */
  public long getMaxHotPageKey(final int indexNo) {
    checkIndex(indexNo);
    return maxHotPageKeys.get(indexNo);
  }

  /**
   * Get the size of maxHotPageKeys for serialization.
   *
   * @return number of entries
   */
  public int getMaxHotPageKeySize() {
    return maxHotPageKeys.size();
  }

  Int2LongMap maxHotPageKeysForSerialization() {
    return maxHotPageKeys;
  }

  /**
   * Increment and get the maximum HOT page key for the given index.
   *
   * @param indexNo the index number
   * @return the new maximum HOT page key
   */
  public long incrementAndGetMaxHotPageKey(final int indexNo) {
    checkIndex(indexNo);
    final long newKey = Math.incrementExact(maxHotPageKeys.get(indexNo));
    maxHotPageKeys.put(indexNo, newKey);
    return newKey;
  }

  private boolean isIndexInitializedUnchecked(final int index) {
    if (maxHotPageKeys.containsKey(index)) {
      return true;
    }
    final PageReference reference = getIndexReference(index);
    return reference != null && !reference.isVirginStructuralPlaceholder();
  }

  /** Return an existing path-tree root without creating a structural reference. */
  public @Nullable PageReference getIndexReference(final int index) {
    checkIndex(index);
    return switch (delegate) {
      case ReferencesPage4 references -> references.referenceAtOffset(index);
      case BitmapReferencesPage references -> references.referenceAtOffset(index);
      case FullReferencesPage references -> references.referenceAt(index);
      default -> throw new IllegalStateException("Unknown PathPage delegate type: " + delegate.getClass().getName());
    };
  }

  private PageReference getOrCreateIndexReference(final int index) {
    checkIndex(index);
    final PageReference existingOrCreated = delegate.getOrCreateReference(index);
    if (existingOrCreated != null) {
      return existingOrCreated;
    }
    final PageReference created = new PageReference();
    delegate = PageUtils.setReference(delegate, index, created);
    return created;
  }

  private static void checkIndex(final int index) {
    if (index < 0 || index >= Constants.INP_REFERENCE_COUNT) {
      throw new IndexOutOfBoundsException("Path index number out of range: " + index);
    }
  }

  private void refuseAllocatorOnlyState(final int index) {
    if (maxHotPageKeys.containsKey(index)) {
      throw new IllegalStateException(
          "Path index " + index + " has HOT allocator metadata but no physical root reference");
    }
  }

  private static Int2LongMap requireValidAllocatorMap(final Int2LongMap allocatorMap) {
    requireNonNull(allocatorMap);
    if (allocatorMap.defaultReturnValue() != 0L) {
      throw new IllegalArgumentException("Path HOT allocator map must default to zero");
    }
    for (final Int2LongMap.Entry entry : allocatorMap.int2LongEntrySet()) {
      checkIndex(entry.getIntKey());
      if (entry.getLongValue() < 0L) {
        throw new IllegalArgumentException("Negative path HOT page-key high-water mark: " + entry.getLongValue());
      }
    }
    return allocatorMap;
  }
}
