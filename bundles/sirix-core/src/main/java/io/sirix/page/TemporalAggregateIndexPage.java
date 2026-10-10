/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.IndexRootPage;
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/** Revision-owned HOT roots and page allocators for temporal aggregate indexes. */
public final class TemporalAggregateIndexPage extends AbstractForwardingPage implements IndexRootPage {

  private Page delegate;

  private final Int2LongMap maxHotPageKeys;

  public TemporalAggregateIndexPage() {
    delegate = new ReferencesPage4();
    maxHotPageKeys = new Int2LongOpenHashMap();
  }

  TemporalAggregateIndexPage(final Page delegate, final Int2LongMap maxHotPageKeys) {
    this.delegate = requireNonNull(delegate);
    this.maxHotPageKeys = requireValidAllocatorMap(maxHotPageKeys);
  }

  /**
   * Copy constructor for write-side CoW. Mirrors
   * {@link ProjectionIndexPage#ProjectionIndexPage(ProjectionIndexPage)}: the underlying delegate is
   * rebuilt with a fresh {@link PageReference} per occupied slot, so mutations to a child reference
   * (key, pageFragments, swizzled page) cannot bleed back into the historical revision's view through
   * cache aliasing. The allocator map is duplicated to decouple writer-side mutations from the
   * prior-revision's instance.
   */
  public TemporalAggregateIndexPage(final TemporalAggregateIndexPage other) {
    final Page otherDelegate = other.delegate;
    if (otherDelegate instanceof ReferencesPage4 ref) {
      this.delegate = new ReferencesPage4(ref);
    } else if (otherDelegate instanceof BitmapReferencesPage bmp) {
      this.delegate = new BitmapReferencesPage(otherDelegate, bmp.getBitmap());
    } else if (otherDelegate instanceof FullReferencesPage full) {
      this.delegate = new FullReferencesPage(full);
    } else {
      throw new IllegalStateException(
          "Unknown TemporalAggregateIndexPage delegate type, cannot clone: " + otherDelegate.getClass().getName());
    }
    this.maxHotPageKeys = new Int2LongOpenHashMap(other.maxHotPageKeys);
  }

  @Override
  public boolean setOrCreateReference(final int offset, final PageReference pageReference) {
    checkIndex(offset);
    delegate = PageUtils.setReference(delegate, offset, pageReference);
    return false;
  }

  /**
   * Get the HOT-tree root reference for the given physical tree id. Creates an empty reference slot
   * if none exists yet.
   */
  public PageReference getIndirectPageReference(final int index) {
    return getOrCreateIndexReference(index);
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize a physical temporal aggregate HOT tree.
   */
  public void createTemporalAggregateIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getOrCreateIndexReference(index);
    if (reference.isVirginStructuralPlaceholder()) {
      refuseAllocatorOnlyState(index);
      PageUtils.createHOTTree(reference, IndexType.TEMPORAL_AGGREGATE, storageEngineReader, log);
    }
  }

  /**
   * Determine whether a physical temporal aggregate tree id has ever been initialized without
   * creating a structural reference.
   *
   * @param index the physical index number
   * @return {@code true} if allocator metadata or a non-virgin root reserves the id
   */
  public boolean isIndexInitialized(final int index) {
    checkIndex(index);
    return isIndexInitializedUnchecked(index);
  }

  /** Return the first physical temporal aggregate tree id that has never been initialized. */
  public int nextUnallocatedIndex() {
    return nextUnallocatedIndex(0);
  }

  /**
   * Return the first uninitialized physical temporal aggregate tree id at or after
   * {@code fromInclusive}.
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
    throw new IllegalStateException("Temporal aggregate index reference space exhausted at or after " + fromInclusive
        + ": " + Constants.INP_REFERENCE_COUNT + " physical ids available");
  }

  public long getMaxHotPageKey(final int indexNo) {
    checkIndex(indexNo);
    return maxHotPageKeys.get(indexNo);
  }

  public int getMaxHotPageKeySize() {
    return maxHotPageKeys.size();
  }

  Int2LongMap maxHotPageKeysForSerialization() {
    return maxHotPageKeys;
  }

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

  /** Return an existing temporal aggregate-tree root without creating a structural reference. */
  public @Nullable PageReference getIndexReference(final int index) {
    checkIndex(index);
    return switch (delegate) {
      case ReferencesPage4 references -> references.referenceAtOffset(index);
      case BitmapReferencesPage references -> references.referenceAtOffset(index);
      case FullReferencesPage references -> references.referenceAt(index);
      default -> throw new IllegalStateException(
          "Unknown TemporalAggregateIndexPage delegate type: " + delegate.getClass().getName());
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
      throw new IndexOutOfBoundsException("Temporal aggregate index number out of range: " + index);
    }
  }

  private void refuseAllocatorOnlyState(final int index) {
    if (maxHotPageKeys.containsKey(index)) {
      throw new IllegalStateException(
          "Temporal aggregate index " + index + " has HOT allocator metadata but no physical root reference");
    }
  }

  private static Int2LongMap requireValidAllocatorMap(final Int2LongMap allocatorMap) {
    requireNonNull(allocatorMap);
    if (allocatorMap.defaultReturnValue() != 0L) {
      throw new IllegalArgumentException("Temporal aggregate HOT allocator map must default to zero");
    }
    for (final Int2LongMap.Entry entry : allocatorMap.int2LongEntrySet()) {
      checkIndex(entry.getIntKey());
      if (entry.getLongValue() < 0L) {
        throw new IllegalArgumentException(
            "Negative temporal aggregate HOT page-key high-water mark: " + entry.getLongValue());
      }
    }
    return allocatorMap;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }
}
