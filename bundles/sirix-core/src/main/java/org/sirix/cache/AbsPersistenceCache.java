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

package org.sirix.cache;

import java.io.File;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;

/**
 * Abstract class for holding all persistence caches. Each instance of this
 * class stores the data in a place related to the {@link DatabaseConfiguration} at a different subfolder.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public abstract class AbsPersistenceCache<K, V> implements ICache<K, V> {

  /**
   * Place to store the data.
   */
  protected final File mPlace;

  /**
   * Type of log (path, value, node or general page-log).
   */
  private final String mLogType;

  /**
   * Determines if directory has been created.
   */
  private final boolean mCreated;

  /**
   * Constructor with the place to store the data.
   * 
   * @param pFile
   *          {@link File} which holds the place to store
   *          the data
   * @param pPageWriteTrx
   *          page write transaction
   * @param pRevision
   *          revision number        
   * @param pLogType
   *          type of log to append to the path of the log
   */
  protected AbsPersistenceCache(final @Nonnull File pFile,
    final @Nonnull IPageWriteTrx pPageWriteTrx,
    final @Nonnegative long pRevision, final @Nonnull String pLogType) throws SirixException {
    mPlace =
      new File(new File(new File(pFile, ResourceConfiguration.Paths.TransactionLog
        .getFile().getName()), Long.toString(pRevision)), pLogType);
    mCreated = mPlace.mkdirs();
    mLogType = pLogType;
  }
  
  /**
   * Determines if the directory is newly created or not.
   * 
   * @return {@code true} if it is newly created, {@code false} otherwise
   */
  public boolean isCreated() {
    return mCreated;
  }

  @Override
  public final void put(@Nonnull final K pKey, @Nonnull final V pPage) {
    try {
      putPersistent(pKey, pPage);
    } catch (final SirixIOException exc) {
      throw new IllegalStateException(exc);
    }
  }

  @Override
  public final void clear() {
    try {
      clearPersistent();
      for (final File file : mPlace.listFiles()) {
        if (!file.delete()) {
          throw new SirixIOException("Couldn't delete!");
        }
      }
      if (!mPlace.delete()) {
        throw new SirixIOException("Couldn't delete!");
      }
    } catch (final SirixIOException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  @Override
  public final V get(@Nonnull final K pKey) {
    try {
      return getPersistent(pKey);
    } catch (final SirixIOException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  /**
   * Clearing a persistent cache.
   * 
   * @throws SirixIOException
   *           if something odd happens
   */
  public abstract void clearPersistent() throws SirixIOException;

  /**
   * Putting a page into a persistent log.
   * 
   * @param pKey
   *          to be put
   * @param pPage
   *          to be put
   * @throws SirixIOException
   *           if something odd happens
   */
  public abstract void putPersistent(@Nonnull final K pKey,
    @Nonnull final V pPage) throws SirixIOException;

  /**
   * Getting a NodePage from the persistent cache.
   * 
   * @param pKey
   *          to get the page
   * @return the Nodepage to be fetched
   * @throws SirixIOException
   *           if something odd happens.
   */
  public abstract V getPersistent(@Nonnull final K pKey) throws SirixIOException;

}
