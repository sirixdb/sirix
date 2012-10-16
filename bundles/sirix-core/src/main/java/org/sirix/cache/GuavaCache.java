package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.EPage;

/**
 * Cache utilizing the Guava cache functionality.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class GuavaCache implements Cache<Tuple, PageContainer> {

  /**
   * Determines after how many seconds to expire entries after the last access.
   */
  private static final int EXPIRE_AFTER = 15;

  /**
   * Maximum cache size.
   */
  private static final int MAX_SIZE = 20;

  /**
   * {@link LoadingCache} reference.
   */
  private final LoadingCache<Tuple, PageContainer> mCache;
  
  /**
   * Second cache.
   */
  private final Cache<Tuple, PageContainer> mSecondCache;

  /**
   * Constructor with second cache.
   * 
   * @param pPageReadTransaction
   *          {@link PageReadTrx} implementation
   * @param pSecondCache
   *          second fallback cache
   */
  public GuavaCache(final @Nonnull PageReadTrx pPageReadTransaction,
    final @Nonnull Cache<Tuple, PageContainer> pSecondCache) {
    checkNotNull(pPageReadTransaction);
    mSecondCache = checkNotNull(pSecondCache);

    final CacheBuilder<Object, Object> builder =
      CacheBuilder.newBuilder().maximumSize(MAX_SIZE).expireAfterAccess(
        EXPIRE_AFTER, TimeUnit.SECONDS);
    builder.removalListener(new RemovalListener<Tuple, PageContainer>() {
      @Override
      public void onRemoval(
        @Nullable final RemovalNotification<Tuple, PageContainer> pRemoval) {
        if (pRemoval != null) {
          final Tuple tuple = pRemoval.getKey();
          final PageContainer pageCont = pRemoval.getValue();
          if (tuple != null && pageCont != null)
            pSecondCache.put(tuple, pageCont);
        }
      }
    });
    mCache = builder.build(new CacheLoader<Tuple, PageContainer>() {
      @Override
      public PageContainer load(final @Nullable Tuple key) throws SirixIOException {
        if (key == null) {
          return PageContainer.EMPTY_INSTANCE;
        }
        final long nodePageKey = key.getKey();
        final EPage pageType = key.getPage();
        if (pageType == null) {
          return PageContainer.EMPTY_INSTANCE;
        } else {
          return pPageReadTransaction.getNodeFromPage(nodePageKey, pageType);
        }
      }
    });
  }

  /**
   * Constructor with an always empty second cache.
   * 
   * @param pPageReadTransaction
   *          {@link PageReadTrx} implementation to read pages
   */
  public GuavaCache(@Nonnull final PageReadTrx pPageReadTransaction) {
    this(pPageReadTransaction, new EmptyCache<Tuple, PageContainer>());
  }

  @Override
  public void clear() {
    mCache.invalidateAll();
    mCache.cleanUp();
  }

  @Override
  public synchronized PageContainer get(@Nonnull final Tuple pKey) {
    try {
      if (pKey.getKey() < 0) {
        return PageContainer.EMPTY_INSTANCE;
      }
      PageContainer container = mCache.getIfPresent(pKey);
      if (container != null && container.equals(PageContainer.EMPTY_INSTANCE)) {
        mCache.invalidate(pKey);
        container = mCache.get(pKey);
      } else if (container == null) {
        container = mCache.get(pKey);
        if (container == null) {
        	container = mSecondCache.get(pKey);
        }
      }
      assert container != null;
      return container;
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void
    put(@Nonnull final Tuple pKey, @Nonnull final PageContainer pValue) {
    mCache.put(pKey, pValue);
  }

  @Override
  public ImmutableMap<Tuple, PageContainer> getAll(
    @Nonnull Iterable<? extends Tuple> keys) {
    try {
      return mCache.getAll(keys);
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void toSecondCache() {
    final ConcurrentMap<Tuple, PageContainer> cached = mCache.asMap();
    mSecondCache.putAll(cached);
  }

  @Override
  public void putAll(final @Nonnull Map<Tuple, PageContainer> pMap) {
    mCache.putAll(pMap);
  }
  
  @Override
  public void remove(final @Nonnull Tuple pKey) {
    mCache.invalidate(pKey);
  }

	@Override
	public void close() {
		mCache.cleanUp();
		mSecondCache.close();
	}
}
