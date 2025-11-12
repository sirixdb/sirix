/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing to the start byte in
 * a file, including the length in bytes, and the checksum of the serialized page. Or it might be an
 * immediate reference to an in-memory instance of the deserialized page.
 * </p>
 */
public final class PageReference {

  /** In-memory deserialized page instance. */
  private volatile Page page;

  /** Key in persistent storage. */
  private long key = Constants.NULL_ID_LONG;

  /** Log key. */
  private int logKey = Constants.NULL_ID_INT;

  /** Unique database ID to distinguish pages from different databases in global BufferManager. */
  private long databaseId = Constants.NULL_ID_LONG;

  /** Unique resource ID to distinguish pages from different resources in global BufferManager. */
  private long resourceId = Constants.NULL_ID_LONG;

  /** The hash in bytes, generated from the referenced page-fragment. */
  private byte[] hashInBytes;

  private List<PageFragmentKey> pageFragments;

  private int hash;

  /**
   * Guard count tracks active PageGuards referencing this page.
   * Pages can only be evicted when guardCount == 0.
   */
  private final AtomicInteger guardCount = new AtomicInteger(0);

  /**
   * Default constructor setting up an uninitialized page reference.
   */
  public PageReference() {
    pageFragments = new ArrayList<>();
  }

  /**
   * Copy constructor.
   *
   * @param reference {@link PageReference} to copy
   */
  public PageReference(final PageReference reference) {
    logKey = reference.logKey;
    page = reference.page;
    key = reference.key;
    databaseId = reference.databaseId;
    resourceId = reference.resourceId;
    hashInBytes = reference.hashInBytes;
    pageFragments = reference.pageFragments;
    hash = reference.hash;
  }

  /**
   * Set in-memory instance of deserialized page.
   *
   * @param page deserialized page
   */
  public void setPage(final @Nullable Page page) {
    this.page = page;
  }

  /**
   * Get in-memory instance of deserialized page.
   *
   * @return in-memory instance of deserialized page
   */
  public Page getPage() {
    return page;
  }

  /**
   * Get start byte offset in file.
   *
   * @return start offset in file
   */
  public long getKey() {
    return key;
  }

  /**
   * Set start byte offset in file.
   *
   * @param key key of this reference set by the persistent storage
   */
  public PageReference setKey(final long key) {
    hash = 0;
    this.key = key;
    return this;
  }

  /**
   * Add a page fragment key.
   * @param key the page fragment key to add.
   * @return this instance
   */
  public PageReference addPageFragment(final PageFragmentKey key) {
    pageFragments.add(key);
    return this;
  }

  /**
   * Get the page fragments keys.
   * @return the page fragments keys
   */
  public List<PageFragmentKey> getPageFragments() {
    return pageFragments;
  }

  /**
   * Set the page fragment keys.
   * @param previousPageFragmentKeys the previous page fragment keys to set
   * @return this instance
   */
  public PageReference setPageFragments(final List<PageFragmentKey> previousPageFragmentKeys) {
    pageFragments = previousPageFragmentKeys;
    return this;
  }

  /**
   * Get in-memory log-key.
   *
   * @return log key
   */
  public int getLogKey() {
    return logKey;
  }

  /**
   * Set in-memory log-key.
   *
   * @param key key of this reference set by the transaction intent log.
   * @return this instance
   */
  public PageReference setLogKey(final int key) {
    hash = 0;  // Clear cached hashCode since it includes logKey
    logKey = key;
    return this;
  }
  
  /**
   * Clear the cached hashCode.
   * Must be called before changing key, logKey, databaseId, or resourceId if the PageReference
   * is already in a HashMap, since the hash depends on these values.
   */
  public void clearCachedHash() {
    hash = 0;
  }

  /**
   * Get the unique database ID.
   *
   * @return database ID
   */
  public long getDatabaseId() {
    return databaseId;
  }

  /**
   * Set the unique database ID.
   *
   * @param databaseId the database ID
   * @return this instance
   */
  public PageReference setDatabaseId(final long databaseId) {
    hash = 0;
    this.databaseId = databaseId;
    return this;
  }

  /**
   * Get the unique resource ID.
   *
   * @return resource ID
   */
  public long getResourceId() {
    return resourceId;
  }

  /**
   * Set the unique resource ID.
   *
   * @param resourceId the resource ID
   * @return this instance
   */
  public PageReference setResourceId(final long resourceId) {
    hash = 0;
    this.resourceId = resourceId;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("databaseId", databaseId)
                      .add("resourceId", resourceId)
                      .add("logKey", logKey)
                      .add("key", key)
                      .add("page", page)
                      .add("pageFragments", pageFragments)
                      .toString();
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(databaseId, resourceId, logKey, key);
    }
    return hash;
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (other instanceof PageReference otherPageRef) {
      return otherPageRef.databaseId == databaseId 
          && otherPageRef.resourceId == resourceId
          && otherPageRef.logKey == logKey 
          && otherPageRef.key == key;
    }
    return false;
  }

  public void setHash(byte[] hashInBytes) {
    this.hashInBytes = hashInBytes;
  }

  public byte[] getHash() {
    return hashInBytes;
  }

  /**
   * Acquire a guard on this page reference.
   * Increments the guard count to indicate an active guard is holding this page.
   */
  public void acquireGuard() {
    guardCount.incrementAndGet();
  }

  /**
   * Release a guard on this page reference.
   * Decrements the guard count when a PageGuard is closed.
   */
  public void releaseGuard() {
    int newCount = guardCount.decrementAndGet();
    if (newCount < 0) {
      throw new IllegalStateException("Guard count underflow for page reference");
    }
  }

  /**
   * Get the current guard count.
   * Used by eviction logic to check if page can be safely evicted.
   *
   * @return current number of active guards
   */
  public int getGuardCount() {
    return guardCount.get();
  }
}
