/*
 * Copyright (c) 2024, SirixDB
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
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import org.jspecify.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base class for HOT index readers.
 *
 * <p>
 * Provides common functionality for tree navigation, root reference lookup, and iteration.
 * Subclasses implement key serialization/deserialization.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Lock-free reads with guard management</li>
 * <li>Pre-allocated traversal arrays via {@link HOTTrieReader}</li>
 * </ul>
 *
 * @param <K> the key type exposed by the reader
 * @author Johannes Lichtenberger
 */
public abstract class AbstractHOTIndexReader<K> {

  protected final StorageEngineReader storageEngineReader;
  protected final IndexType indexType;
  protected final int indexNumber;

  /**
   * Protected constructor.
   *
   * @param storageEngineReader the storage engine reader
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  protected AbstractHOTIndexReader(StorageEngineReader storageEngineReader, IndexType indexType, int indexNumber) {
    this.storageEngineReader = requireNonNull(storageEngineReader);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
  }

  /**
   * Get the storage engine reader.
   *
   * @return the storage engine reader
   */
  public StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  /**
   * Get the index type.
   *
   * @return the index type
   */
  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * Get the index number.
   *
   * @return the index number
   */
  public int getIndexNumber() {
    return indexNumber;
  }

  /**
   * Get the root reference for the index.
   *
   * @return the root page reference, or null if not found
   */
  protected @Nullable PageReference getRootReference() {
    final RevisionRootPage rootPage = storageEngineReader.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = storageEngineReader.getPathPage(rootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferencesCount()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = storageEngineReader.getCASPage(rootPage);
        if (casPage == null || indexNumber >= casPage.getReferencesCount()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = storageEngineReader.getNamePage(rootPage);
        if (namePage == null || indexNumber >= namePage.getReferencesCount()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      case VALIDTIME -> {
        final io.sirix.page.ValidTimeIndexPage vtPage = storageEngineReader.getValidTimeIndexPage(rootPage);
        if (vtPage == null || indexNumber >= vtPage.getReferencesCount()) {
          yield null;
        }
        yield vtPage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  /**
   * Navigate to the leaf page containing the key. Uses {@link HOTTrieReader} for proper tree
   * traversal.
   *
   * <p><b>The returned page is NOT guarded.</b> The trie reader is closed on the way out, and its
   * {@code close()} releases the guard it held on this very leaf — so the page is evictable the
   * instant this method returns, and {@code ClockSweeper} may free its off-heap frame before the
   * caller touches it. Dereferencing it is a use-after-free waiting to happen.
   *
   * <p>Currently only test code calls this. Do NOT wire it into a production read path: hold the
   * {@link HOTTrieReader} open for as long as you need the leaf (that is what its single-guarded-leaf
   * discipline is for), or go through {@code HOTRangeCursor}, which owns the guard for the lifetime
   * of the cursor.
   *
   * @param rootRef the root reference
   * @param key the search key bytes
   * @return the leaf page, or null if not found; unguarded — see above
   */
  protected @Nullable HOTLeafPage navigateToLeaf(PageReference rootRef, byte[] key) {
    try (var trieReader = new HOTTrieReader(storageEngineReader)) {
      return trieReader.navigateToLeaf(rootRef, key);
    }
  }

  /**
   * Serialize a key to bytes.
   *
   * @param key the key to serialize
   * @param buffer the buffer to write to
   * @param offset the offset in the buffer
   * @return the number of bytes written
   */
  protected abstract int serializeKey(K key, byte[] buffer, int offset);

  /**
   * Deserialize a key from bytes.
   *
   * @param buffer the buffer to read from
   * @param offset the offset in the buffer
   * @param length the number of bytes to read
   * @return the deserialized key, or null if invalid
   */
  protected abstract @Nullable K deserializeKey(byte[] buffer, int offset, int length);

  /**
   * Compare two serialized keys.
   *
   * @param key1 first key bytes
   * @param offset1 offset in first key
   * @param length1 length of first key
   * @param key2 second key bytes
   * @param offset2 offset in second key
   * @param length2 length of second key
   * @return negative if key1 < key2, zero if equal, positive if key1 > key2
   */
  protected abstract int compareKeys(byte[] key1, int offset1, int length1, byte[] key2, int offset2, int length2);

  /**
   * Get the thread-local key buffer.
   *
   * @return the key buffer
   */
  protected abstract byte[] getKeyBuffer();

  /**
   * Set a new key buffer if the current one is too small.
   *
   * @param newBuffer the new buffer
   */
  protected abstract void setKeyBuffer(byte[] newBuffer);

  /**
   * Create an iterator over all entries in the HOT index.
   *
   * <p>Implementations must return an iterator whose page access is guard-scoped for as long as
   * it holds a leaf. Both concrete readers do this by building on {@code HOTRangeCursor}, which is
   * {@link AutoCloseable} and holds its leaf through the trie reader's single-guard discipline.
   *
   * <p>This is deliberately abstract rather than a default implementation. The previous default
   * (an inner {@code HOTLeafIterator}) was overridden by every subclass and therefore unreachable,
   * while still carrying two real defects: it dereferenced an UNGUARDED page from
   * {@code getHOTLeafPage} across iterator steps, and it released its trie reader's guard only
   * when iteration ran to exhaustion, so any early exit pinned a page for the life of the process.
   * A broken default that nobody calls is worse than no default — it reads as live code. Forcing
   * subclasses to supply one keeps that from silently coming back.
   *
   * @return iterator over all key-value pairs
   */
  public abstract Iterator<Map.Entry<K, NodeReferences>> iterator();

}

