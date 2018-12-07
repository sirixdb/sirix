/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
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
import java.util.Objects;
import javax.annotation.Nullable;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * <h1>PageReference</h1>
 *
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing to the start byte in
 * a file, including the length in bytes, and the checksum of the serialized page. Or it might be an
 * immediate reference to an in-memory instance of the deserialized page.
 * </p>
 */
public final class PageReference {

  /** In-memory deserialized page instance. */
  private Page mPage;

  /** Key in persistent storage. */
  private long mKey = Constants.NULL_ID_LONG;

  /** Log key. */
  private int mLogKey = Constants.NULL_ID_INT;

  /** Persistent log key. */
  private long mPersistentLogKey = Constants.NULL_ID_LONG;

  /** Length in bytes. */
  private int mLength;

  private byte[] mHashInBytes;

  /**
   * Default constructor setting up an uninitialized page reference.
   */
  public PageReference() {}

  /**
   * Copy constructor.
   *
   * @param reference {@link PageReference} to copy
   */
  public PageReference(final PageReference reference) {
    mLogKey = reference.mLogKey;
    mPage = reference.mPage;
    mKey = reference.mKey;
    mPersistentLogKey = reference.mPersistentLogKey;
    mLength = reference.mLength;
  }

  /**
   * Set in-memory instance of deserialized page.
   *
   * @param page deserialized page
   */
  public void setPage(final @Nullable Page page) {
    mPage = page;
  }

  /**
   * Get in-memory instance of deserialized page.
   *
   * @return in-memory instance of deserialized page
   */
  public Page getPage() {
    return mPage;
  }

  /**
   * Set the length of a referenced page in bytes.
   *
   * @param length the length
   * @return this page reference
   */
  public PageReference setLength(final int length) {
    checkArgument(length > 0, "Length must be > 0.");
    mLength = length;
    return this;
  }

  /**
   * Get the length of a referenced page in the persistent storage (in bytes).
   *
   * @return the length of a referenced page in the persistent storage (in bytes)
   */
  public int getLength() {
    return mLength;
  }

  /**
   * Get start byte offset in file.
   *
   * @return start offset in file
   */
  public long getKey() {
    return mKey;
  }

  /**
   * Set start byte offset in file.
   *
   * @param key key of this reference set by the persistent storage
   */
  public PageReference setKey(final long key) {
    mKey = key;
    return this;
  }

  /**
   * Get in-memory log-key.
   *
   * @return log key
   */
  public int getLogKey() {
    return mLogKey;
  }

  /**
   * Set in-memory log-key.
   *
   * @param key key of this reference set by the transaction intent log.
   */
  public PageReference setLogKey(final int key) {
    mLogKey = key;
    return this;
  }

  /**
   * Get log-key.
   *
   * @return log key
   */
  public long getPersistentLogKey() {
    return mPersistentLogKey;
  }

  /**
   * Set log-key.
   *
   * @param key key of this reference set by the transaction intent log.
   */
  public PageReference setPersistentLogKey(final long key) {
    mPersistentLogKey = key;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("logKey", mLogKey)
                      .add("persistentLogKey", mPersistentLogKey)
                      .add("key", mKey)
                      .add("page", mPage)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(mLogKey, mKey, mPersistentLogKey);
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (other instanceof PageReference) {
      final PageReference otherPageRef = (PageReference) other;
      return otherPageRef.mLogKey == mLogKey && otherPageRef.mKey == mKey
          && otherPageRef.mPersistentLogKey == mPersistentLogKey;
    }
    return false;
  }

  public void setHash(byte[] hashInBytes) {
    mHashInBytes = hashInBytes;
  }

  public byte[] getHash() {
    return mHashInBytes;
  }
}
