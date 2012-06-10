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

package org.treetank.utils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1>WeakHashMap</h1>
 * 
 * <p>
 * Based on the SoftHashMap implemented by Dr. Heinz Kabutz.
 * </p>
 * 
 * <p>
 * Hash map based on weak references.
 * </p>
 * 
 * <p>
 * Note that the put and remove methods always return null.
 * </p>
 * 
 * @param <K>
 *          Key object of type K.
 * @param <V>
 *          Value object of type V.
 */
@SuppressWarnings("unchecked")
public final class FastWeakHashMap<K, V> extends AbstractMap<K, V> {

  /** The internal HashMap that will hold the WeakReference. */
  private final Map<K, WeakReference<V>> mInternalMap;

  /** Reference queue for cleared WeakReference objects. */
  private final ReferenceQueue mQueue;

  /**
   * Default constructor internally using 32 strong references.
   * 
   */
  public FastWeakHashMap() {
    mInternalMap = new ConcurrentHashMap();
    mQueue = new ReferenceQueue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(final Object mKey) {
    V value = null;
    final WeakReference<V> weakReference = mInternalMap.get(mKey);
    if (weakReference != null) {
      // Weak reference was garbage collected.
      value = weakReference.get();
      if (value == null) {
        // Reflect garbage collected weak reference in internal hash
        // map.
        mInternalMap.remove(mKey);
      }
    }
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V put(final K mKey, final V mValue) {
    processQueue();
    mInternalMap.put(mKey, new WeakValue<V>(mValue, mKey, mQueue));
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V remove(final Object mKey) {
    processQueue();
    mInternalMap.remove(mKey);
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void clear() {
    processQueue();
    mInternalMap.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int size() {
    processQueue();
    return mInternalMap.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove garbage collected weak values with the help of the reference
   * queue.
   * 
   */
  private void processQueue() {
    WeakValue<V> weakValue;
    while ((weakValue = (WeakValue<V>)mQueue.poll()) != null) {
      mInternalMap.remove(weakValue.mKey);
    }
  }

  /**
   * Internal subclass to store keys and values for more convenient lookups.
   */
  @SuppressWarnings("hiding")
  private final class WeakValue<V> extends WeakReference<V> {
    private final K mKey;

    /**
     * Constructor.
     * 
     * @param mInitValue
     *          Value wrapped as weak reference.
     * @param mInitKey
     *          Key for given value.
     * @param mInitReferenceQueue
     *          Reference queue for cleanup.
     */
    private WeakValue(final V mInitValue, final K mInitKey, final ReferenceQueue mInitReferenceQueue) {
      super(mInitValue, mInitReferenceQueue);
      mKey = mInitKey;
    }
  }

}
