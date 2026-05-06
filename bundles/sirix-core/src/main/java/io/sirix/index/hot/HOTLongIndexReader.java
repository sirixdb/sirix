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

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Primitive-specialized HOT index reader for long keys (PATH index).
 *
 * <p>
 * Uses primitive {@code long} keys to avoid boxing overhead. This is the high-performance variant
 * for PATH index read operations.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Primitive long parameters (no boxing)</li>
 * <li>Thread-local byte buffers for serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Lock-free reads with guard management</li>
 * <li>Proper tree traversal via {@link AbstractHOTIndexReader}</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 * @see HOTIndexReader
 * @see AbstractHOTIndexReader
 */
public final class HOTLongIndexReader extends AbstractHOTIndexReader<Long> {

  /**
   * Thread-local buffer for composite key serialization (8 bytes long + 4 bytes chunkIdx_be4).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE]);

  private final HOTLongKeySerializer keySerializer;

  /**
   * Private constructor.
   *
   * @param storageEngineReader the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   */
  private HOTLongIndexReader(StorageEngineReader storageEngineReader, HOTLongKeySerializer keySerializer, IndexType indexType,
      int indexNumber) {
    super(storageEngineReader, indexType, indexNumber);
    this.keySerializer = keySerializer;
  }

  /**
   * Creates a new HOTLongIndexReader for PATH index.
   *
   * @param storageEngineReader the storage engine reader
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexReader instance
   */
  public static HOTLongIndexReader create(StorageEngineReader storageEngineReader, IndexType indexType, int indexNumber) {
    return new HOTLongIndexReader(storageEngineReader, PathKeySerializer.INSTANCE, indexType, indexNumber);
  }

  /**
   * Reassemble all chunks of a primitive long key. See {@link HOTIndexReader#get} for the
   * algorithm; this is the long-key mirror.
   */
  public @Nullable NodeReferences get(long key, SearchMode mode) {
    final byte[] keyBuf = KEY_BUFFER.get();
    final int prefixLen = keySerializer.serialize(key, keyBuf, 0);
    return reassembleChunksForLongPrefix(keyBuf, prefixLen);
  }

  private @Nullable NodeReferences reassembleChunksForLongPrefix(byte[] prefixBuf, int prefixLen) {
    final PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }

    final byte[] fromBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    final byte[] toBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, toBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(toBytes, prefixLen, 0xFFFFFFFF);

    Roaring64Bitmap merged = null;
    try (HOTTrieReader reader = new HOTTrieReader(getStorageEngineReader());
        HOTRangeCursor cursor = reader.range(rootRef, fromBytes, toBytes)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int idx = cursor.currentEntryIndex();
        final byte[] composite = leaf.getKey(idx);
        if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
            || Arrays.compareUnsigned(composite, 0, prefixLen, prefixBuf, 0, prefixLen) != 0) {
          cursor.advance();
          continue;
        }
        final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
        final byte[] chunkBytes = leaf.getValue(idx);
        if (NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
          cursor.advance();
          continue;
        }
        final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(chunkBytes);
        final Roaring64Bitmap chunkBitmap = chunkRefs.getNodeKeys();
        if (chunkBitmap.isEmpty()) {
          cursor.advance();
          continue;
        }
        if (merged == null) {
          merged = new Roaring64Bitmap();
        }
        final long high = ((long) chunkIdx) << 16;
        final LongIterator bIt = chunkBitmap.getLongIterator();
        while (bIt.hasNext()) {
          merged.add(high | (bIt.next() & 0xFFFFL));
        }
        cursor.advance();
      }
    }
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return new NodeReferences(merged);
  }

  /**
   * Check if any chunk exists for {@code key}. Cheaper than {@link #get} because we only need
   * to find the first non-tombstone chunk slot, not reassemble the full bitmap.
   */
  public boolean containsKey(long key) {
    return get(key, SearchMode.EQUAL) != null;
  }

  /**
   * Range iterator from {@code fromKey} (inclusive) with no upper bound. Used for PATH
   * sub-tree scans (e.g., "all paths under prefix X" when path-keys are encoded so that
   * sub-trees have contiguous long ranges).
   */
  public Iterator<Map.Entry<Long, NodeReferences>> iteratorFrom(long fromKey) {
    final byte[] keyBuf = KEY_BUFFER.get();
    final int fromLen = keySerializer.serialize(fromKey, keyBuf, 0);
    final byte[] fromPrefix = Arrays.copyOf(keyBuf, fromLen);
    final byte[] fromComposite = new byte[fromLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(fromPrefix, 0, fromComposite, 0, fromLen);
    HOTKeySerializer.writeChunkIdxBE(fromComposite, fromLen, 0);
    return new ChunkAggregatingLongIterator(fromComposite, null, fromPrefix);
  }

  /**
   * Range iterator over {@code [fromKey, toKey]} inclusive on the logical long-key axis.
   */
  public Iterator<Map.Entry<Long, NodeReferences>> range(long fromKey, long toKey) {
    final byte[] keyBuf = KEY_BUFFER.get();
    final int fromLen = keySerializer.serialize(fromKey, keyBuf, 0);
    final byte[] fromPrefix = Arrays.copyOf(keyBuf, fromLen);
    final byte[] fromComposite = new byte[fromLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(fromPrefix, 0, fromComposite, 0, fromLen);
    HOTKeySerializer.writeChunkIdxBE(fromComposite, fromLen, 0);

    final int toLen = keySerializer.serialize(toKey, keyBuf, 0);
    final byte[] toComposite = new byte[toLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(keyBuf, 0, toComposite, 0, toLen);
    HOTKeySerializer.writeChunkIdxBE(toComposite, toLen, 0xFFFFFFFF);

    return new ChunkAggregatingLongIterator(fromComposite, toComposite, fromPrefix);
  }

  @Override
  public Iterator<Map.Entry<Long, NodeReferences>> iterator() {
    return new ChunkAggregatingLongIterator(new byte[0], null, null);
  }

  /**
   * Iterator that walks chunked composite-key range and groups consecutive same-prefix slots
   * into logical {@link Map.Entry}{@code <Long, NodeReferences>}. See
   * {@code HOTIndexReader.ChunkAggregatingIterator} for the K=Object mirror.
   */
  private final class ChunkAggregatingLongIterator implements Iterator<Map.Entry<Long, NodeReferences>> {
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable HOTRangeCursor cursor;
    /** See {@code HOTIndexReader.ChunkAggregatingIterator.fromPrefixFilter} for rationale. */
    private final byte @Nullable [] fromPrefixFilter;
    private Map.@Nullable Entry<Long, NodeReferences> nextEntry;

    ChunkAggregatingLongIterator(byte[] fromComposite, byte @Nullable [] toComposite,
        byte @Nullable [] fromPrefixFilter) {
      this.fromPrefixFilter = fromPrefixFilter;
      final PageReference rootRef = getRootReference();
      if (rootRef == null) {
        this.trieReader = null;
        this.cursor = null;
        this.nextEntry = null;
        return;
      }
      this.trieReader = new HOTTrieReader(getStorageEngineReader());
      this.cursor = trieReader.range(rootRef, fromComposite, toComposite);
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<Long, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      final Map.Entry<Long, NodeReferences> result = nextEntry;
      advance();
      if (nextEntry == null) {
        closeQuietly();
      }
      return result;
    }

    private void closeQuietly() {
      if (cursor != null) {
        cursor.close();
      }
      if (trieReader != null) {
        trieReader.close();
      }
    }

    private void advance() {
      if (cursor == null) {
        nextEntry = null;
        return;
      }
      // Skip lex-pre-fromPrefix groups (HOT path-stack walk isn't lex-monotonic; see
      // HOTIndexReader.ChunkAggregatingIterator.fromPrefixFilter for the full rationale).
      while (cursor.hasNext()) {
        final byte[] composite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
        if (composite.length < HOTKeySerializer.CHUNK_IDX_BYTES) {
          cursor.advance();
          continue;
        }
        if (fromPrefixFilter != null) {
          final int candidatePrefixLen = composite.length - HOTKeySerializer.CHUNK_IDX_BYTES;
          if (Arrays.compareUnsigned(composite, 0, candidatePrefixLen,
              fromPrefixFilter, 0, fromPrefixFilter.length) < 0) {
            cursor.advance();
            continue;
          }
        }
        break;
      }
      if (!cursor.hasNext()) {
        nextEntry = null;
        return;
      }
      byte[] groupComposite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
      if (groupComposite.length < HOTKeySerializer.CHUNK_IDX_BYTES) {
        cursor.advance();
        advance();
        return;
      }
      final int prefixLen = groupComposite.length - HOTKeySerializer.CHUNK_IDX_BYTES;
      Roaring64Bitmap merged = null;
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int idx = cursor.currentEntryIndex();
        final byte[] composite = leaf.getKey(idx);
        if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
            || Arrays.compareUnsigned(composite, 0, prefixLen, groupComposite, 0, prefixLen) != 0) {
          break;
        }
        final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
        final byte[] chunkBytes = leaf.getValue(idx);
        if (!NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
          final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(chunkBytes);
          final Roaring64Bitmap chunkBitmap = chunkRefs.getNodeKeys();
          if (!chunkBitmap.isEmpty()) {
            if (merged == null) {
              merged = new Roaring64Bitmap();
            }
            final long high = ((long) chunkIdx) << 16;
            final LongIterator bIt = chunkBitmap.getLongIterator();
            while (bIt.hasNext()) {
              merged.add(high | (bIt.next() & 0xFFFFL));
            }
          }
        }
        cursor.advance();
      }
      if (merged == null || merged.isEmpty()) {
        advance();
        return;
      }
      final Long logicalKey = keySerializer.deserialize(groupComposite, 0, prefixLen);
      nextEntry = new AbstractMap.SimpleImmutableEntry<>(logicalKey, new NodeReferences(merged));
    }
  }

  @Override
  protected int serializeKey(Long key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }

  @Override
  protected @Nullable Long deserializeKey(byte[] buffer, int offset, int length) {
    return keySerializer.deserialize(buffer, offset, length);
  }

  @Override
  protected int compareKeys(byte[] key1, int offset1, int length1, byte[] key2, int offset2, int length2) {
    return keySerializer.compare(key1, offset1, length1, key2, offset2, length2);
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }
}
