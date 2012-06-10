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

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.TTIOException;

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
   * Counter to give every instance a different place.
   */
  private static int mCounter;

  /**
   * Constructor with the place to store the data.
   * 
   * @param pFile
   *          {@link File} which holds the place to store
   *          the data
   */
  protected AbsPersistenceCache(final File pFile) {
    mPlace =
      new File(new File(pFile, ResourceConfiguration.Paths.TransactionLog.getFile().getName()), Integer
        .toString(mCounter));
    mPlace.mkdirs();
    mCounter++;
  }

  @Override
  public final void put(final K pKey, final V pPage) {
    try {
      putPersistent(pKey, pPage);
    } catch (final TTIOException exc) {
      throw new IllegalStateException(exc);
    }
  }

  @Override
  public final void clear() {
    try {
      clearPersistent();
      for (final File file : mPlace.listFiles()) {
        if (!file.delete()) {
          throw new TTIOException("Couldn't delete!");
        }
      }
      if (!mPlace.delete()) {
        throw new TTIOException("Couldn't delete!");
      }
    } catch (final TTIOException exc) {
      throw new IllegalStateException(exc);
    }
  }

  @Override
  public final V get(final K pKey) {
    try {
      return getPersistent(pKey);
    } catch (final TTIOException exc) {
      throw new IllegalStateException(exc);
    }
  }

  /**
   * Clearing a persistent cache.
   * 
   * @throws TTIOException
   *           if something odd happens
   */
  public abstract void clearPersistent() throws TTIOException;

  /**
   * Putting a page into a persistent log.
   * 
   * @param pKey
   *          to be put
   * @param pPage
   *          to be put
   * @throws TTIOException
   *           if something odd happens
   */
  public abstract void putPersistent(final K pKey, final V pPage) throws TTIOException;

  /**
   * Getting a NodePage from the persistent cache.
   * 
   * @param pKey
   *          to get the page
   * @return the Nodepage to be fetched
   * @throws TTIOException
   *           if something odd happens.
   */
  public abstract V getPersistent(final K pKey) throws TTIOException;

}
