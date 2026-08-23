/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.utils.ToStringHelper;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

/**
 * Container page for projection indexes, keyed by {@code IndexDef#getID()}.
 *
 * <p>
 * Structurally identical to {@link CASPage} / {@link PathPage} / {@link NamePage}: the page
 * delegate holds one {@link PageReference} per registered projection index, each rooting a
 * versioned HOT sub-tree whose leaves are
 * {@link io.sirix.index.projection.ProjectionIndexRowGroupRecord} entries. Per-index book-keeping
 * mirrors the other secondary indexes:
 *
 * <ul>
 * <li>{@link #maxNodeKeys}: largest leaf-record nodeKey ever allocated in index {@code i}. Used by
 * the builder to stamp sequential leaf ids without a full trie walk.</li>
 * <li>{@link #maxHotPageKeys}: largest HOT page-key ever allocated, matches the persistence layer's
 * {@code HOTTrieWriter}.</li>
 * <li>{@link #currentMaxLevelsOfIndirectPages}: depth of the indirect-page chain per index. The
 * executor uses this to short-circuit empty indexes.</li>
 * </ul>
 *
 * <p>
 * Placement in {@link RevisionRootPage} matches the CAS/PATH/NAME pattern: one sibling reference
 * offset, populated on fresh revisions via {@link RevisionRootPage#getProjectionPageReference()}.
 */
public final class ProjectionIndexPage extends AbstractForwardingPage {

  private Page delegate;

  private final Int2LongMap maxNodeKeys;

  private final Int2LongMap maxHotPageKeys;

  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  public ProjectionIndexPage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    maxHotPageKeys = new Int2LongOpenHashMap();
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
  }

  ProjectionIndexPage(final Page delegate, final Int2LongMap maxNodeKeys, final Int2LongMap maxHotPageKeys,
      final Int2IntMap currentMaxLevelsOfIndirectPages) {
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.maxHotPageKeys = maxHotPageKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
  }

  /**
   * Copy constructor for write-side CoW. Mirrors {@link IndirectPage#IndirectPage(IndirectPage)}: the
   * underlying delegate is rebuilt with a fresh {@link PageReference} per occupied slot, so mutations
   * to a child reference (key, pageFragments, swizzled page) cannot bleed back into the historical
   * revision's view through cache aliasing. The bookkeeping maps are duplicated to decouple
   * writer-side mutations from the prior-revision's instance.
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
    this.maxNodeKeys = new Int2LongOpenHashMap(other.maxNodeKeys);
    this.maxHotPageKeys = new Int2LongOpenHashMap(other.maxHotPageKeys);
    this.currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap(other.currentMaxLevelsOfIndirectPages);
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);
    return false;
  }

  /**
   * Get the indirect-page reference for the projection index with the given {@code IndexDef} id.
   * Creates an empty reference slot if none exists yet.
   */
  public PageReference getIndirectPageReference(int index) {
    return getOrCreateProjectionReference(index);
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialise the HOT sub-tree for a projection index. Mirrors
   * {@link CASPage#createHOTCASIndexTree}.
   */
  public void createProjectionIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getOrCreateProjectionReference(index);
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createHOTTree(reference, IndexType.PROJECTION, storageEngineReader, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  /** Swap in a fresh empty sub-tree for {@code index}, preserving earlier revisions through CoW. */
  public void resetProjectionIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    getOrCreateProjectionReference(index);
    final PageReference fresh = new PageReference();
    delegate = PageUtils.setReference(delegate, index, fresh);
    PageUtils.createHOTTree(fresh, IndexType.PROJECTION, storageEngineReader, log);
    maxNodeKeys.put(index, 0L);
    maxHotPageKeys.put(index, 0L);
    currentMaxLevelsOfIndirectPages.put(index, 0);
  }

  // Kept for parity with CASPage — used by legacy index creation paths.
  @SuppressWarnings("unused")
  public void createLegacyProjectionIndexTree(final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader, final int index, final TransactionIntentLog log) {
    final PageReference reference = getOrCreateProjectionReference(index);
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.PROJECTION, storageEngineReader, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
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
    if (index < 0 || index >= Constants.INP_REFERENCE_COUNT) {
      throw new IndexOutOfBoundsException("projection index number out of range: " + index);
    }
    final PageReference existingOrCreated = delegate.getOrCreateReference(index);
    if (existingOrCreated != null) {
      return existingOrCreated;
    }
    final PageReference created = new PageReference();
    delegate = PageUtils.setReference(delegate, index, created);
    return created;
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  public int getCurrentMaxLevelOfIndirectPagesSize() {
    return currentMaxLevelsOfIndirectPages.size();
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  public long getMaxNodeKey(final int indexNo) {
    return maxNodeKeys.get(indexNo);
  }

  public int getMaxNodeKeySize() {
    return maxNodeKeys.size();
  }

  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = maxNodeKeys.get(indexNo) + 1;
    maxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }

  public long getMaxHotPageKey(final int indexNo) {
    return maxHotPageKeys.get(indexNo);
  }

  public int getMaxHotPageKeySize() {
    return maxHotPageKeys.size();
  }

  public long incrementAndGetMaxHotPageKey(final int indexNo) {
    final long newKey = maxHotPageKeys.get(indexNo) + 1;
    maxHotPageKeys.put(indexNo, newKey);
    return newKey;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("delegate", delegate).toString();
  }
}
