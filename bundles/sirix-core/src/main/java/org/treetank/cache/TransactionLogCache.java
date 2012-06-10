/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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

package org.treetank.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.io.File;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.treetank.access.conf.DatabaseConfiguration;
import org.treetank.api.IPageReadTrx;
import org.treetank.exception.TTIOException;

/**
 * Transactionlog for storing all upcoming nodes in either the ram cache or a
 * persistent second cache.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class TransactionLogCache implements ICache<Long, NodePageContainer> {

  /**
   * RAM-Based first cache.
   */
  private final LRUCache<Long, NodePageContainer> mFirstCache;

  /**
   * Constructor including the {@link DatabaseConfiguration} for persistent
   * storage.
   * 
   * @param pFile
   *          the config for having a storage-place
   * @param pRevision
   *          revision number
   * @throws TTIOException
   *           if I/O is not successful
   */
  public TransactionLogCache(@Nonnull final IPageReadTrx pPageReadTransaction, @Nonnull final File pFile,
    @Nonnegative final long pRevision) throws TTIOException {
    final BerkeleyPersistenceCache secondCache = new BerkeleyPersistenceCache(pFile, pRevision);
    mFirstCache = new LRUCache<>(secondCache);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("cache", mFirstCache).toString();
  }

  @Override
  public ImmutableMap<Long, NodePageContainer> getAll(@Nonnull final Iterable<? extends Long> pKeys) {
    final ImmutableMap.Builder<Long, NodePageContainer> builder =
      new ImmutableMap.Builder<Long, NodePageContainer>();
    for (final Long key : pKeys) {
      if (mFirstCache.get(key) != null) {
        builder.put(key, mFirstCache.get(key));
      }
    }
    return builder.build();
  }

  @Override
  public void clear() {
    mFirstCache.clear();
  }

  @Override
  public NodePageContainer get(Long pKey) {
    return mFirstCache.get(pKey);
  }

  @Override
  public void put(@Nonnull Long pKey, @Nonnull NodePageContainer pValue) {
    mFirstCache.put(pKey, pValue);
  }
}
