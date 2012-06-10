package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.sirix.api.IPageReadTrx;
import org.sirix.exception.TTIOException;

/**
 * Cache utilizing the Guava cache functionality.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class GuavaCache implements ICache<Long, NodePageContainer> {

  /**
   * Pool for prefetching.
   */
  private final ExecutorService mPool = Executors.newSingleThreadExecutor();

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
  private final LoadingCache<Long, NodePageContainer> mCache;

  /**
   * Constructor with no second cache.
   * 
   * @param pPageRadTransaction
   *          {@link IPageReadTrx} implementation
   */
  public GuavaCache(final IPageReadTrx pPageReadTransaction) {
    this(pPageReadTransaction, new NullCache<Long, NodePageContainer>());
  }

  /**
   * Constructor with second cache.
   * 
   * @param pPageReadTransaction
   *          {@link IPageReadTrx} implementation
   * @param pSecondCache
   *          second fallback cache
   */
  public GuavaCache(final IPageReadTrx pPageReadTransaction,
    final ICache<Long, NodePageContainer> pSecondCache) {
    checkNotNull(pPageReadTransaction);
    checkNotNull(pSecondCache);

    final CacheBuilder<Object, Object> builder =
      CacheBuilder.newBuilder().maximumSize(MAX_SIZE).expireAfterAccess(EXPIRE_AFTER, TimeUnit.SECONDS);
    if (!(pSecondCache instanceof NullCache)) {
      RemovalListener<Long, NodePageContainer> removalListener =
        new RemovalListener<Long, NodePageContainer>() {
          @Override
          public void onRemoval(final RemovalNotification<Long, NodePageContainer> pRemoval) {
            pSecondCache.put(pRemoval.getKey(), pRemoval.getValue());
          }
        };
      builder.removalListener(removalListener);
    }
    mCache = builder.build(new CacheLoader<Long, NodePageContainer>() {
      @Override
      public NodePageContainer load(final Long key) throws TTIOException {
        return pPageReadTransaction.getNodeFromPage(key);
      }
    });
    if (pPageReadTransaction.getUberPage().getRevisionNumber() == 0) {
      mPool.submit(new Callable<Void>() {
        @Override
        public Void call() throws ExecutionException {
          // Preload 20 nodePages.
          mCache.getAll(Arrays.asList(0l, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l,
            16l, 17l, 18l, 19l));
          return null;
        }
      });
    }
  }

  @Override
  public void clear() {
    mCache.invalidateAll();
    mCache.cleanUp();
    mPool.shutdownNow();
    try {
      mPool.awaitTermination(50, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public NodePageContainer get(final Long pKey) {
    try {
      if (pKey < 0) {
        return NodePageContainer.EMPTY_INSTANCE;
      }
      NodePageContainer container = mCache.getIfPresent(pKey);
      if (container != null && container.equals(NodePageContainer.EMPTY_INSTANCE)) {
        mCache.invalidate(pKey);
        container = mCache.get(pKey);
      }
      if (container == null) {
        container = mCache.get(pKey);
      }
      return container;
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void put(final Long pKey, final NodePageContainer pValue) {
    mCache.put(pKey, pValue);
  }

  @Override
  public ImmutableMap<Long, NodePageContainer> getAll(Iterable<? extends Long> keys) {
    try {
      return mCache.getAll(keys);
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }
}
