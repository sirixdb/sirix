/**
 * Copyright (c) 2018, Sirix
 * <p>
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the <organization> nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
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
package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.page.RevisionRootPage;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class RevisionRootPageCache implements Cache<RevisionRootPageCacheKey, RevisionRootPage> {
  private final com.github.benmanes.caffeine.cache.Cache<RevisionRootPageCacheKey, RevisionRootPage> cache;

  public RevisionRootPageCache(final int maxSize) {
    cache = Caffeine.newBuilder()
                    .initialCapacity(maxSize)
                    .maximumSize(maxSize)
                    .scheduler(scheduler)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public RevisionRootPage get(RevisionRootPageCacheKey key) {
    var revisionRootPage = cache.getIfPresent(key);

    if (revisionRootPage != null) {
      revisionRootPage = new RevisionRootPage(revisionRootPage, revisionRootPage.getRevision());
    }

    return revisionRootPage;
  }

  @Override
  public RevisionRootPage get(RevisionRootPageCacheKey key,
      BiFunction<? super RevisionRootPageCacheKey, ? super RevisionRootPage, ? extends RevisionRootPage> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void putIfAbsent(RevisionRootPageCacheKey key, RevisionRootPage value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void put(RevisionRootPageCacheKey key, @NonNull RevisionRootPage value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends RevisionRootPageCacheKey, ? extends RevisionRootPage> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<RevisionRootPageCacheKey, RevisionRootPage> getAll(Iterable<? extends RevisionRootPageCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(RevisionRootPageCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<RevisionRootPageCacheKey, RevisionRootPage> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {
  }
}
