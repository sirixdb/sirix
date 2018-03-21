package org.sirix.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An LRU cache, based on <code>LinkedHashMap</code> holding last key changes for a user.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public final class KeyCache {

  /**
   * Capacity of the cache.
   */
  static final int CACHE_CAPACITY = 100;

  /**
   * The collection to hold the maps.
   */
  private final Map<String, List<Long>> mMap;

  /**
   * Constructor creates a new key cache.
   */
  public KeyCache() {
    mMap = new LinkedHashMap<String, List<Long>>(CACHE_CAPACITY) {
      private static final long serialVersionUID = 1;

      @Override
      protected boolean removeEldestEntry(final Map.Entry<String, List<Long>> mEldest) {
        boolean returnVal = false;
        if (size() > CACHE_CAPACITY) {
          returnVal = true;
        }
        return returnVal;
      }
    };
  }

  /**
   * Returns the stored <code>LinkedList</code> of corresponding user.
   * 
   * @param user User key.
   * @return linked list of user.
   */
  public final List<Long> get(final String user) {
    final List<Long> list = mMap.get(user);
    return list; // returns null if no value for this user exists in cache.
  }

  /**
   * Stores a new entry in cache consisting of a user name as key and a linked list for storing node
   * keys as value.
   * 
   * @param user user name as key.
   * @param list linked list as values.
   */
  public final void put(final String user, final List<Long> list) {

    mMap.put(user, list);
  }

  /**
   * Clears the cache.
   */
  public final void clear() {
    mMap.clear();
  }

  /**
   * Returns the number of used entries in the cache.
   * 
   * @return the number of entries currently in the cache.
   */
  public final int usedEntries() {
    return mMap.size();
  }

  /**
   * Returns a <code>Collection</code> that contains a copy of all cache entries.
   * 
   * @return a <code>Collection</code> with a copy of the cache content.
   */

  public final Collection<Map.Entry<String, List<Long>>> getAll() {
    return new ArrayList<Map.Entry<String, List<Long>>>(mMap.entrySet());

  }
}
