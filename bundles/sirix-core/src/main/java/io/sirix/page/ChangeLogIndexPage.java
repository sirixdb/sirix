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

/** Revision-owned HOT roots and page allocators for change log indexes. */
public final class ChangeLogIndexPage extends AbstractForwardingPage implements IndexRootPage {

  private Page delegate;

  private final Int2LongMap maxHotPageKeys;

  public ChangeLogIndexPage() {
    delegate = new ReferencesPage4();
    maxHotPageKeys = new Int2LongOpenHashMap();
  }

  ChangeLogIndexPage(final Page delegate, final Int2LongMap maxHotPageKeys) {
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
  public ChangeLogIndexPage(final ChangeLogIndexPage other) {
    final Page otherDelegate = other.delegate;
    if (otherDelegate instanceof ReferencesPage4 ref) {
      this.delegate = new ReferencesPage4(ref);
    } else if (otherDelegate instanceof BitmapReferencesPage bmp) {
      this.delegate = new BitmapReferencesPage(otherDelegate, bmp.getBitmap());
    } else if (otherDelegate instanceof FullReferencesPage full) {
      this.delegate = new FullReferencesPage(full);
    } else {
      throw new IllegalStateException(
          "Unknown ChangeLogIndexPage delegate type, cannot clone: " + otherDelegate.getClass().getName());
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
   * Initialize a physical change log HOT tree.
   */
  public void createChangeLogIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getOrCreateIndexReference(index);
    if (reference.isVirginStructuralPlaceholder()) {
      refuseAllocatorOnlyState(index);
      PageUtils.createHOTTree(reference, IndexType.CHANGE_LOG, storageEngineReader, log);
    }
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

  /** Return an existing change log-tree root without creating a structural reference. */
  public @Nullable PageReference getIndexReference(final int index) {
    checkIndex(index);
    return switch (delegate) {
      case ReferencesPage4 references -> references.referenceAtOffset(index);
      case BitmapReferencesPage references -> references.referenceAtOffset(index);
      case FullReferencesPage references -> references.referenceAt(index);
      default ->
        throw new IllegalStateException("Unknown ChangeLogIndexPage delegate type: " + delegate.getClass().getName());
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
        throw new IllegalArgumentException("Negative change log HOT page-key high-water mark: " + entry.getLongValue());
      }
    }
    return allocatorMap;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }
}
