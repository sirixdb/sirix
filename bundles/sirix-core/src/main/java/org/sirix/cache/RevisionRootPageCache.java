/**
 * Copyright (c) 2018, Sirix
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
package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sirix.page.RevisionRootPage;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
public final class RevisionRootPageCache implements Cache<Integer, RevisionRootPage> {
  private final com.github.benmanes.caffeine.cache.Cache<Integer, RevisionRootPage> pageCache;

  public RevisionRootPageCache() {
    pageCache = Caffeine.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite(30, TimeUnit.SECONDS)
                        .expireAfterAccess(30, TimeUnit.SECONDS)
                        .build();
  }

  @Override
  public void clear() {
    pageCache.invalidateAll();
  }

  @Override
  public RevisionRootPage get(Integer key) {
    return pageCache.getIfPresent(key);
  }

  @Override
  public void put(Integer key, RevisionRootPage value) {
    pageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends RevisionRootPage> map) {
    pageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Integer, RevisionRootPage> getAll(Iterable<? extends Integer> keys) {
    return pageCache.getAllPresent(keys);
  }

  @Override
  public void remove(Integer key) {
    pageCache.invalidate(key);
  }

  @Override
  public void close() {}
}
