// package org.sirix.cache;
//
// import static com.google.common.base.Preconditions.checkNotNull;
// import com.google.common.cache.CacheBuilder;
// import com.google.common.cache.CacheLoader;
// import com.google.common.cache.LoadingCache;
// import com.google.common.cache.RemovalListener;
// import com.google.common.cache.RemovalNotification;
// import com.google.common.collect.ImmutableMap;
//
// import java.util.Map;
// import java.util.concurrent.ConcurrentMap;
// import java.util.concurrent.ExecutionException;
// import java.util.concurrent.TimeUnit;
//
// import org.checkerframework.checker.nullness.qual.NonNull;
// import org.checkerframework.checker.nullness.qual.Nullable;
//
// import org.sirix.api.PageReadTrx;
// import org.sirix.exception.SirixIOException;
// import org.sirix.page.PageKind;
//
/// **
// * Cache utilizing the Guava cache functionality.
// *
// * @author Johannes Lichtenberger, University of Konstanz
// *
// */
// public class GuavaCache implements Cache<Tuple, RecordPageContainer> {
//
// /**
// * Determines after how many seconds to expire entries after the last access.
// */
// private static final int EXPIRE_AFTER = 15;
//
// /**
// * Maximum cache size.
// */
// private static final int MAX_SIZE = 20;
//
// /**
// * {@link LoadingCache} reference.
// */
// private final LoadingCache<Tuple, RecordPageContainer> mCache;
//
// /**
// * Second cache.
// */
// private final Cache<Tuple, RecordPageContainer> mSecondCache;
//
// /**
// * Constructor with second cache.
// *
// * @param pageReadTransaction
// * {@link PageReadTrx} implementation
// * @param secondCache
// * second fallback cache
// */
// public GuavaCache(final PageReadTrx pageReadTransaction,
// final Cache<Tuple, RecordPageContainer> secondCache) {
// checkNotNull(pageReadTransaction);
// mSecondCache = checkNotNull(secondCache);
//
// final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
// .maximumSize(MAX_SIZE)
// .expireAfterAccess(EXPIRE_AFTER, TimeUnit.SECONDS);
// builder.removalListener(new RemovalListener<Tuple, RecordPageContainer>() {
// @Override
// public void onRemoval(
// @Nullable final RemovalNotification<Tuple, RecordPageContainer> pRemoval) {
// if (pRemoval != null) {
// final Tuple tuple = pRemoval.getKey();
// final RecordPageContainer pageCont = pRemoval.getValue();
// if (tuple != null && pageCont != null)
// secondCache.put(tuple, pageCont);
// }
// }
// });
// mCache = builder.build(new CacheLoader<Tuple, RecordPageContainer>() {
// @Override
// public RecordPageContainer load(final @Nullable Tuple key)
// throws SirixIOException {
// if (key == null) {
// return RecordPageContainer.EMPTY_INSTANCE;
// }
// final long nodePageKey = key.getKey();
// final PageKind pageType = key.getPage();
// if (pageType == null) {
// return RecordPageContainer.EMPTY_INSTANCE;
// } else {
// return pageReadTransaction.getNodeFromPage(nodePageKey, pageType);
// }
// }
// });
// }
//
// /**
// * Constructor with an always empty second cache.
// *
// * @param pageReadTransaction
// * {@link PageReadTrx} implementation to read pages
// */
// public GuavaCache(final PageReadTrx pageReadTransaction) {
// this(pageReadTransaction, new EmptyCache<Tuple, RecordPageContainer>());
// }
//
// @Override
// public void clear() {
// mCache.invalidateAll();
// mCache.cleanUp();
// }
//
// @Override
// public synchronized RecordPageContainer get(final Tuple key) {
// try {
// if (key.getKey() < 0) {
// return RecordPageContainer.EMPTY_INSTANCE;
// }
// RecordPageContainer container = mCache.getIfPresent(key);
// if (container != null
// && container.equals(RecordPageContainer.EMPTY_INSTANCE)) {
// mCache.invalidate(key);
// container = mCache.get(key);
// } else if (container == null) {
// container = mCache.get(key);
// if (container == null) {
// container = mSecondCache.get(key);
// }
// }
// assert container != null;
// return container;
// } catch (final ExecutionException e) {
// throw new IllegalStateException(e);
// }
// }
//
// @Override
// public void put(final Tuple key,
// final RecordPageContainer value) {
// mCache.put(key, value);
// }
//
// @Override
// public ImmutableMap<Tuple, RecordPageContainer> getAll(
// final Iterable<? extends Tuple> keys) {
// try {
// return mCache.getAll(keys);
// } catch (final ExecutionException e) {
// throw new IllegalStateException(e);
// }
// }
//
// @Override
// public void toSecondCache() {
// final ConcurrentMap<Tuple, RecordPageContainer> cached = mCache.asMap();
// mSecondCache.putAll(cached);
// }
//
// @Override
// public void putAll(final Map<Tuple, RecordPageContainer> map) {
// mCache.putAll(map);
// }
//
// @Override
// public void remove(final Tuple key) {
// mCache.invalidate(key);
// }
//
// @Override
// public void close() {
// mCache.cleanUp();
// mSecondCache.close();
// }
// }
