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
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

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
   * Pooled trie reader for point-lookup chunk walks: constructing a {@link HOTTrieReader} allocates
   * its path-stack arrays, which on the lookup path was per-call garbage. One instance is parked here
   * between calls ({@link HOTTrieReader#close()} releases the leaf guard and clears the path but
   * keeps the object reusable). Handed out via {@link AtomicReference#getAndSet} so concurrent
   * lookups through the same reader instance stay correct — a loser simply constructs a fresh one.
   */
  private final AtomicReference<HOTTrieReader> pooledTrieReader = new AtomicReference<>();

  /** Pooled chunk accumulator for the lookup walk — same handoff discipline as the trie reader. */
  private final AtomicReference<NodeReferencesSerializer.ChunkAccumulator> pooledAccumulator = new AtomicReference<>();

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
   * Point-lookup chunk collection shared by the CAS/NAME and PATH readers: seek
   * {@code lowerBound(prefix ‖ chunk0)} and walk forward, merging every slot whose composite key is
   * exactly {@code prefix ‖ chunkIdx}, until the first key whose leading {@code prefixLen} bytes
   * exceed the prefix — lex order makes that stop authoritative, every later key exceeds it too.
   * Entries that merely EXTEND the prefix (longer logical keys sharing its bytes) are skipped, not
   * stopped on: their composites sort between this prefix's chunk slots whenever their next byte is
   * below the chunk trailer's.
   *
   * <p>
   * Zero-copy by construction: slot filtering reads off the leaf via
   * {@link HOTLeafPage#compareKeyPrefix} / {@link HOTLeafPage#getKeyLength}, the chunkIdx via
   * {@link HOTLeafPage#readKeyIntBE}, and the chunk payload merges straight from slot memory via
   * {@link NodeReferencesSerializer#mergeChunkInto} — no key or value is ever materialized. The trie
   * reader is pooled across calls.
   *
   * @param prefixBuf buffer holding the serialized logical key
   * @param prefixLen serialized length of the logical key
   * @return the reassembled references — compact for small results — or {@code null} when no chunk
   *         holds a live reference
   */
  protected final @Nullable NodeReferences collectChunksViaLowerBoundWalk(final byte[] prefixBuf, final int prefixLen) {
    final PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }
    final int compositeLen = prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES;
    final byte[] fromBytes = new byte[compositeLen];
    System.arraycopy(prefixBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    HOTTrieReader trie = pooledTrieReader.getAndSet(null);
    if (trie == null) {
      trie = new HOTTrieReader(storageEngineReader);
    }
    NodeReferencesSerializer.ChunkAccumulator accumulator = pooledAccumulator.getAndSet(null);
    if (accumulator == null) {
      accumulator = new NodeReferencesSerializer.ChunkAccumulator();
    }
    try {
      final HOTTrieReader.LowerBoundResult lowerBound = trie.lowerBound(rootRef, fromBytes);
      HOTLeafPage leaf = lowerBound.leaf;
      int idx = lowerBound.indexInLeaf;
      walk: while (leaf != null) {
        final int entryCount = leaf.getEntryCount();
        while (idx < entryCount) {
          final int cmp = leaf.compareKeyPrefix(idx, prefixBuf, prefixLen);
          if (cmp > 0) {
            break walk;
          }
          if (cmp == 0 && leaf.getKeyLength(idx) == compositeLen) {
            final long chunkIdx = leaf.readKeyIntBE(idx, prefixLen) & 0xFFFFFFFFL;
            accumulator.addChunk(leaf, leaf.valueRef(idx), chunkIdx << 16);
          }
          idx++;
        }
        leaf = trie.advanceToNextLeaf();
        idx = 0;
      }
      return accumulator.toNodeReferencesAndReset();
    } finally {
      accumulator.reset(); // no-op on the success path; drops partial state if the walk threw
      trie.close(); // releases the leaf guard + clears the path; the object stays reusable
      pooledTrieReader.compareAndSet(null, trie);
      pooledAccumulator.compareAndSet(null, accumulator);
    }
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
   * @param rootRef the root reference
   * @param key the search key bytes
   * @return the leaf page, or null if not found
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
   * Upper bound, in bytes, on what {@link #serializeKey} writes for {@code key}, so callers can size
   * the buffer before the write rather than discover the overflow after it.
   *
   * @param key the key about to be serialized
   * @return a value {@code >=} the length {@code serializeKey} will return
   */
  protected abstract int maxSerializedKeyLength(K key);

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
   * {@link Map.Entry} whose key deserializes on first {@link #getKey()} — the chunk-aggregating
   * iterators emit these so value-only consumers never pay key materialization at all. The unfiltered
   * CAS {@code openIndex} path, for one, iterates the whole index and reads ONLY {@code getValue()}:
   * with eager keys that was one decoded atomic + key object per logical entry, all garbage.
   * Immutable ({@link #setValue} throws), {@code equals}/{@code hashCode} follow the
   * {@link Map.Entry} contract (and therefore force the key).
   */
  protected final class LazyKeyEntry implements Map.Entry<K, NodeReferences> {
    private final byte[] keyBytes;
    private final int keyLen;
    private final NodeReferences refs;
    private @Nullable K key;
    private boolean keyDeserialized;

    protected LazyKeyEntry(final byte[] keyBytes, final int keyLen, final NodeReferences refs) {
      this.keyBytes = keyBytes;
      this.keyLen = keyLen;
      this.refs = refs;
    }

    @Override
    public @Nullable K getKey() {
      if (!keyDeserialized) {
        key = deserializeKey(keyBytes, 0, keyLen);
        keyDeserialized = true;
      }
      return key;
    }

    @Override
    public NodeReferences getValue() {
      return refs;
    }

    @Override
    public NodeReferences setValue(final NodeReferences value) {
      throw new UnsupportedOperationException("immutable index entry");
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof Map.Entry<?, ?> other)) {
        return false;
      }
      final K k = getKey();
      return (k == null
          ? other.getKey() == null
          : k.equals(other.getKey())) && refs.equals(other.getValue());
    }

    @Override
    public int hashCode() {
      final K k = getKey();
      return (k == null
          ? 0
          : k.hashCode()) ^ refs.hashCode();
    }
  }

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
   * @return iterator over all key-value pairs
   */
  public Iterator<Map.Entry<K, NodeReferences>> iterator() {
    return new HOTLeafIterator();
  }

  /**
   * Iterator over all entries in a HOT index, handling tree navigation.
   */
  protected class HOTLeafIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable PageReference rootRef;

    protected HOTLeafIterator() {
      this.rootRef = getRootReference();
      if (rootRef != null) {
        this.trieReader = new HOTTrieReader(storageEngineReader);
        // Navigate to leftmost leaf
        this.currentLeaf = trieReader.navigateToLeftmostLeaf(rootRef);
      } else {
        this.trieReader = null;
        // Fallback to simple case
        this.currentLeaf = storageEngineReader.getHOTLeafPage(indexType, indexNumber);
      }
      this.currentIndex = 0;
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<K, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<K, NodeReferences> result = nextEntry;
      advance();
      return result;
    }

    private void advance() {
      nextEntry = null;
      while (currentLeaf != null) {
        if (currentIndex < currentLeaf.getEntryCount()) {
          byte[] keyBytes = currentLeaf.getKey(currentIndex);
          byte[] valueBytes = currentLeaf.getValue(currentIndex);
          currentIndex++;

          if (!NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
            K key = deserializeKey(keyBytes, 0, keyBytes.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
            if (key != null && refs != null) {
              nextEntry = Map.entry(key, refs);
              return;
            }
          }
        } else {
          // No more entries in current leaf - try to advance to next leaf
          currentIndex = 0;
          if (trieReader != null) {
            currentLeaf = trieReader.advanceToNextLeaf();
          } else {
            currentLeaf = null;
          }
        }
      }

      // Clean up trie reader when done
      if (trieReader != null && currentLeaf == null) {
        trieReader.close();
      }
    }
  }

  /**
   * Range iterator over HOT entries.
   *
   * <p>
   * <strong>Implementation note (lower-bound primitive missing in Sirix's HOT).</strong> A
   * navigate-to-fromKey range scan via {@link HOTRangeCursor} returns wrong results for non-existent
   * fromKeys on this Sirix HOT implementation: {@code HOTTrieReader.navigateToLeaf} does PEXT-based
   * exact-or-best-guess routing and does NOT implement a true lower-bound primitive over the lex
   * order. For an existing key it lands correctly; for a non-existent key (the common range-scan
   * case, e.g. {@code GREATER_OR_EQUAL 2500}) it may land in a leaf whose entries are NOT lex-greater
   * than fromKey, and walking forward from there visits keys in HOT-trie-order rather than lex order
   * — verified empirically by
   * {@code HOTMultiLayerIndirectPageTest.testCrossTransactionWriteAfterSplitPreservesEntries} (range
   * &ge; 2500 returned 4489 instead of 2501).
   *
   * <p>
   * The HOT paper / Binna 2018 reference implementation in C++ does provide a proper lower_bound
   * iterator; Sirix's HOT does not yet expose one. Until that primitive lands, the correct fallback
   * is leftmost-and-filter — start at {@code navigateToLeftmostLeaf}, walk every leaf via the parent
   * stack, skip entries before {@code fromBytes}. {@code O(total
   * trie entries)} per query, but correct on every key shape (chunked PROJECTION-style keys,
   * composite CAS path-value pairs, etc.).
   *
   * <p>
   * If/when {@code HOTTrieReader} gains a true lower-bound navigation, this iterator can be rewritten
   * to use it; the current implementation pins the correct semantics.
   */
  protected class RangeIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private final byte[] fromBytes;
    private final byte @Nullable [] toBytes; // null means no upper bound
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    private final @Nullable HOTTrieReader trieReader;

    protected RangeIterator(byte[] fromBytes, byte @Nullable [] toBytes) {
      this.fromBytes = fromBytes;
      this.toBytes = toBytes;

      PageReference rootRef = getRootReference();
      if (rootRef != null) {
        this.trieReader = new HOTTrieReader(storageEngineReader);
        this.currentLeaf = trieReader.navigateToLeftmostLeaf(rootRef);
      } else {
        this.trieReader = null;
        this.currentLeaf = storageEngineReader.getHOTLeafPage(indexType, indexNumber);
      }

      this.currentIndex = 0;
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<K, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<K, NodeReferences> result = nextEntry;
      advance();
      return result;
    }

    private void advance() {
      nextEntry = null;
      while (currentLeaf != null) {
        if (currentIndex < currentLeaf.getEntryCount()) {
          byte[] key = currentLeaf.getKey(currentIndex);

          if (toBytes != null && compareKeys(key, 0, key.length, toBytes, 0, toBytes.length) >= 0) {
            currentLeaf = null;
            break;
          }

          if (compareKeys(key, 0, key.length, fromBytes, 0, fromBytes.length) < 0) {
            currentIndex++;
            continue;
          }

          byte[] value = currentLeaf.getValue(currentIndex);
          currentIndex++;

          if (!NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
            K deserializedKey = deserializeKey(key, 0, key.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(value);
            if (deserializedKey != null && refs != null) {
              nextEntry = Map.entry(deserializedKey, refs);
              return;
            }
          }
        } else {
          currentIndex = 0;
          if (trieReader != null) {
            currentLeaf = trieReader.advanceToNextLeaf();
          } else {
            currentLeaf = null;
          }
        }
      }

      if (trieReader != null && currentLeaf == null) {
        trieReader.close();
      }
    }
  }

}

