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

package org.sirix.page;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

import org.sirix.page.interfaces.Page;
import org.sirix.page.interfaces.PageFragmentKey;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing to the start byte in
 * a file, including the length in bytes, and the checksum of the serialized page. Or it might be an
 * immediate reference to an in-memory instance of the deserialized page.
 * </p>
 */
public final class PageReference {

  /** In-memory deserialized page instance. */
  private Page page;

  /** Key in persistent storage. */
  private long key = Constants.NULL_ID_LONG;

  /** Log key. */
  private int logKey = Constants.NULL_ID_INT;

  /** Persistent log key. */
  private long persistentLogKey = Constants.NULL_ID_LONG;

  /** The hash in bytes, generated from the referenced page-fragment. */
  private byte[] hashInBytes;

  private List<PageFragmentKey> pageFragments;

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
    hashInBytes = reference.hashInBytes;
    persistentLogKey = reference.persistentLogKey;
    pageFragments = reference.pageFragments;
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
    this.key = key;
    return this;
  }

  public PageReference addPageFragment(final PageFragmentKey key) {
    pageFragments.add(key);
    return this;
  }

  public List<PageFragmentKey> getPageFragments() {
    return pageFragments;
  }

  public PageReference setPageFragments(List<PageFragmentKey> previousPageFragmentKeys) {
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
   */
  public PageReference setLogKey(final int key) {
    logKey = key;
    return this;
  }

  /**
   * Get log-key.
   *
   * @return log key
   */
  public long getPersistentLogKey() {
    return persistentLogKey;
  }

  /**
   * Set log-key.
   *
   * @param key key of this reference set by the transaction intent log.
   */
  public PageReference setPersistentLogKey(final long key) {
    persistentLogKey = key;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("logKey", logKey)
                      .add("persistentLogKey", persistentLogKey)
                      .add("key", key)
                      .add("page", page)
                      .add("pageFragments", pageFragments)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(logKey, key, persistentLogKey);
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (other instanceof PageReference otherPageRef) {
      return otherPageRef.logKey == logKey && otherPageRef.key == key
          && otherPageRef.persistentLogKey == persistentLogKey;
    }
    return false;
  }

  public void setHash(byte[] hashInBytes) {
    this.hashInBytes = hashInBytes;
  }

  public byte[] getHash() {
    return hashInBytes;
  }
}
