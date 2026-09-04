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
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.jspecify.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Container page for projection indexes, keyed by {@code IndexDef#getID()}.
 *
 * <p>
 * The delegate holds one {@link PageReference} per registered projection index, each rooting a
 * versioned HOT tree whose leaves are
 * {@link io.sirix.index.projection.ProjectionIndexRowGroupRecord} entries. The sparse
 * {@code maxHotPageKeys} map is the only per-index metadata and persists each tree's HOT page-key
 * allocator. Projection rows use their logical row-group key directly; they do not need a second
 * leaf-record id allocator or keyed-trie level metadata.
 *
 * <p>
 * Placement in {@link RevisionRootPage} matches the CAS/PATH/NAME pattern: one sibling reference
 * offset, populated on fresh revisions via
 * {@link RevisionRootPage#getProjectionIndexPageReference()}.
 */
public final class ProjectionIndexPage extends AbstractForwardingPage {

  private Page delegate;

  private final Int2LongMap maxHotPageKeys;

  public ProjectionIndexPage() {
    delegate = new ReferencesPage4();
    maxHotPageKeys = new Int2LongOpenHashMap();
  }

  ProjectionIndexPage(final Page delegate, final Int2LongMap maxHotPageKeys) {
    this.delegate = requireNonNull(delegate);
    this.maxHotPageKeys = requireValidAllocatorMap(maxHotPageKeys);
  }

  /**
   * Copy constructor for write-side CoW. Mirrors {@link IndirectPage#IndirectPage(IndirectPage)}: the
   * underlying delegate is rebuilt with a fresh {@link PageReference} per occupied slot, so mutations
   * to a child reference (key, pageFragments, swizzled page) cannot bleed back into the historical
   * revision's view through cache aliasing. The allocator map is duplicated to decouple writer-side
   * mutations from the prior-revision's instance.
   */
  public ProjectionIndexPage(final ProjectionIndexPage other) {
    final Page otherDelegate = other.delegate;
    if (otherDelegate instanceof ReferencesPage4 ref) {
      this.delegate = new ReferencesPage4(ref);
    } else if (otherDelegate instanceof BitmapReferencesPage bmp) {
      this.delegate = new BitmapReferencesPage(otherDelegate, bmp.getBitmap());
    } else if (otherDelegate instanceof FullReferencesPage full) {
      this.delegate = new FullReferencesPage(full);
    } else {
      throw new IllegalStateException(
          "Unknown ProjectionIndexPage delegate type, cannot clone: " + otherDelegate.getClass().getName());
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
   * Get the HOT-tree root reference for the projection index with the given {@code IndexDef} id.
   * Creates an empty reference slot if none exists yet.
   */
  public PageReference getIndirectPageReference(final int index) {
    return getOrCreateProjectionReference(index);
  }

  /** Return an existing projection-tree reference without materializing a read-side placeholder. */
  public @Nullable PageReference getIndexReference(final int index) {
    checkIndex(index);
    return switch (delegate) {
      case ReferencesPage4 references -> references.referenceAtOffset(index);
      case BitmapReferencesPage references -> references.referenceAtOffset(index);
      case FullReferencesPage references -> references.referenceAt(index);
      default ->
        throw new IllegalStateException("Unknown ProjectionIndexPage delegate type: " + delegate.getClass().getName());
    };
  }

  /**
   * Return the first projection-index number whose physical tree has never been initialized.
   *
   * <p>
   * A catalog entry is not an allocation witness: dropping an index removes its catalog definition
   * while its versioned tree and bookkeeping entries remain reserved for historical revisions.
   * Conversely, a read-side structural placeholder is not an initialized tree and must not burn an
   * id. Either persisted bookkeeping or a non-virgin root reference is the physical initialization
   * witness. The lookup is non-mutating and allocation-free on the normal sparse and full paths.
   * </p>
   *
   * @return the first physically unallocated projection-index number
   * @throws IllegalStateException if all projection-index reference slots are allocated
   */
  public int nextUnallocatedIndex() {
    return nextUnallocatedIndex(0);
  }

  /**
   * Return the first uninitialized physical projection-tree id at or after {@code fromInclusive}.
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
    throw new IllegalStateException("Projection index reference space exhausted: all " + Constants.INP_REFERENCE_COUNT
        + " physical tree ids are initialized");
  }

  /**
   * Determine whether a physical projection-tree id has ever been initialized without creating a
   * structural reference.
   *
   * @param index the physical index number
   * @return {@code true} if allocator metadata or a non-virgin root reserves the id
   */
  public boolean isIndexInitialized(final int index) {
    checkIndex(index);
    return isIndexInitializedUnchecked(index);
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize the projection index's HOT tree.
   */
  public void createProjectionIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getOrCreateProjectionReference(index);
    if (reference.isVirginStructuralPlaceholder()) {
      refuseAllocatorOnlyState(index);
      PageUtils.createHOTTree(reference, IndexType.PROJECTION, storageEngineReader, log);
    }
  }

  /**
   * Create one projection-root reference while honoring both delegate growth thresholds.
   *
   * <p>
   * {@link BitmapReferencesPage#getOrCreateReference(int)} deliberately returns {@code null} when
   * adding the threshold entry, after the entry has already been installed. Treating every such
   * {@code null} as a {@link ReferencesPage4} overflow causes a class cast at the bitmap-to-full
   * transition. Routing the replacement through {@link PageUtils#setReference} performs the correct
   * sparse-to-bitmap or bitmap-to-full conversion.
   * </p>
   */
  private PageReference getOrCreateProjectionReference(final int index) {
    checkIndex(index);
    final PageReference existingOrCreated = delegate.getOrCreateReference(index);
    if (existingOrCreated != null) {
      return existingOrCreated;
    }
    final PageReference created = new PageReference();
    delegate = PageUtils.setReference(delegate, index, created);
    return created;
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

  private static void checkIndex(final int index) {
    if (index < 0 || index >= Constants.INP_REFERENCE_COUNT) {
      throw new IndexOutOfBoundsException("Projection index number out of range: " + index);
    }
  }

  private void refuseAllocatorOnlyState(final int index) {
    if (maxHotPageKeys.containsKey(index)) {
      throw new IllegalStateException(
          "Projection index " + index + " has HOT allocator metadata but no physical root reference");
    }
  }

  private static Int2LongMap requireValidAllocatorMap(final Int2LongMap allocatorMap) {
    requireNonNull(allocatorMap);
    if (allocatorMap.defaultReturnValue() != 0L) {
      throw new IllegalArgumentException("Projection HOT allocator map must default to zero");
    }
    for (final Int2LongMap.Entry entry : allocatorMap.int2LongEntrySet()) {
      checkIndex(entry.getIntKey());
      if (entry.getLongValue() < 0L) {
        throw new IllegalArgumentException("Negative projection HOT page-key high-water mark: " + entry.getLongValue());
      }
    }
    return allocatorMap;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }
}
