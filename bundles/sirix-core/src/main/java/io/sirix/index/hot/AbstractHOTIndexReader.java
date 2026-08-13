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
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
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
 * <li>Lock-free reads via optimistic stamp validation — leaves stay evictable, no pins</li>
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
   * between calls ({@link HOTTrieReader#close()} drops the leaf snapshot and clears the path but
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
   * {@link NodeReferencesSerializer.ChunkAccumulator#addChunk} — no key or value is ever
   * materialized. The trie reader is pooled across calls.
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
    // The seek key must be an EXACTLY-sized array, so it is built fresh rather than appended to the
    // caller's oversized buffer in place. Not an oversight: the whole descent takes the search key's
    // length from the array itself — HOTLeafPage.compareKeyWithBound reads bound.length and
    // DiscriminativeBitComputer.computeDifferingBit reads both operands' lengths — so handing it a
    // 512-byte thread-local buffer means "a 512-byte key padded with zeros", which routes and
    // verifies against the wrong key and silently misses stored entries (measured: 13% of point
    // lookups). Removing this allocation needs a length-parameterized HOTTrieReader.lowerBound, not
    // a buffer trick.
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
      // The whole walk runs against UNPINNED leaves under optimistic stamps: each leaf's read
      // batch — the per-slot prefix compares, the chunkIdx reads, the payload merges — is
      // validated once before its outcome (stop, or advance to the next leaf) takes effect. A
      // torn batch poisons the accumulator, so recovery is wholesale: reset and re-walk from a
      // fresh lower-bound descent. Content per PageReference is immutable, so every retry
      // re-derives the identical result.
      for (int walkAttempt = 0; walkAttempt < MAX_TORN_WALK_RETRIES; walkAttempt++) {
        accumulator.reset();
        final HOTTrieReader.LowerBoundResult lowerBound = trie.lowerBound(rootRef, fromBytes);
        HOTLeafPage leaf = lowerBound.leaf;
        int idx = lowerBound.indexInLeaf;
        boolean torn = false;
        while (leaf != null) {
          boolean stop = false;
          try {
            final int entryCount = leaf.getEntryCount();
            while (idx < entryCount) {
              final int cmp = leaf.compareKeyPrefix(idx, prefixBuf, prefixLen);
              if (cmp > 0) {
                stop = true;
                break;
              }
              if (cmp == 0 && leaf.getKeyLength(idx) == compositeLen) {
                final long chunkIdx = leaf.readKeyIntBE(idx, prefixLen) & 0xFFFFFFFFL;
                if (!accumulator.addChunk(leaf, leaf.valueRef(idx), chunkIdx << 16, trie)) {
                  torn = true;
                  break;
                }
              }
              idx++;
            }
          } catch (RuntimeException e) {
            if (trie.validateCurrentLeaf()) {
              throw e; // stable bytes — genuine corruption, not a torn read
            }
            torn = true;
          }
          if (torn || !trie.validateCurrentLeaf()) {
            torn = true;
            break;
          }
          if (stop) {
            break;
          }
          leaf = trie.advanceToNextLeaf();
          idx = 0;
        }
        if (torn) {
          continue;
        }
        return accumulator.toNodeReferencesAndReset();
      }
      throw new IllegalStateException("HOT: chunk walk failed stamp validation on every one of " + MAX_TORN_WALK_RETRIES
          + " attempts — sustained allocator thrashing");
    } finally {
      accumulator.reset(); // no-op on the success path; drops partial state if the walk threw
      trie.close(); // clears the reader's leaf snapshot + path; the object stays reusable
      pooledTrieReader.compareAndSet(null, trie);
      pooledAccumulator.compareAndSet(null, accumulator);
    }
  }

  /**
   * Bound on whole-walk retries after a torn read poisoned an aggregate. Each retry races eviction
   * independently on freshly reloaded copies, so consecutive failures imply allocator thrashing, not
   * a logic error.
   */
  private static final int MAX_TORN_WALK_RETRIES = 64;

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
  /**
   * Root reference, memoized after the first resolution for read-only snapshots: the chain
   * {@code RevisionRootPage -> index page -> child reference} is immutable for a committed revision,
   * yet the profile showed every point lookup re-walking it (~16% of a hit). A writer-backed reader
   * resolves fresh per call — an in-flight commit can replace the index page and its child
   * references.
   */
  private volatile @Nullable PageReference cachedRootReference;

  protected @Nullable PageReference getRootReference() {
    PageReference root = cachedRootReference;
    if (root != null) {
      return root;
    }
    root = resolveRootReference();
    if (root != null && !(storageEngineReader instanceof StorageEngineWriter)) {
      cachedRootReference = root; // benign race: resolution is idempotent for a snapshot
    }
    return root;
  }

  private @Nullable PageReference resolveRootReference() {
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
        final ValidTimeIndexPage vtPage = storageEngineReader.getValidTimeIndexPage(rootPage);
        if (vtPage == null || indexNumber >= vtPage.getReferencesCount()) {
          yield null;
        }
        yield vtPage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  // navigateToLeaf(rootRef, key) was removed: no caller anywhere, and under optimistic stamps a
  // bare unpinned leaf with no validation contract attached is exactly the API shape that invites
  // silent torn reads. Leaf access goes through the validated walk/iterator primitives above.

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
   * A {@link Map.Entry} that can hand out the entry's serialized key without materializing it, so a
   * per-entry filter that only needs a fixed-offset field (the CAS path class record, say) can read
   * it in place instead of paying a full key deserialization per entry.
   */
  public interface RawKeyBytes {
    /** Buffer holding the serialized logical key; valid for {@code [0, rawKeyLength())}. */
    byte[] rawKeyBytes();

    /** Length of the serialized logical key inside {@link #rawKeyBytes()}. */
    int rawKeyLength();
  }

  /**
   * {@link Map.Entry} whose key deserializes on first {@link #getKey()} — the chunk-aggregating
   * iterators emit these so value-only consumers never pay key materialization at all. The unfiltered
   * CAS {@code openIndex} path, for one, iterates the whole index and reads ONLY {@code getValue()}:
   * with eager keys that was one decoded atomic + key object per logical entry, all garbage.
   * Immutable ({@link #setValue} throws), {@code equals}/{@code hashCode} follow the
   * {@link Map.Entry} contract (and therefore force the key).
   */
  protected final class LazyKeyEntry implements Map.Entry<K, NodeReferences>, RawKeyBytes {
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
    public byte[] rawKeyBytes() {
      return keyBytes;
    }

    @Override
    public int rawKeyLength() {
      return keyLen;
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
   * Iterate every logical entry in the index.
   *
   * <p>
   * Abstract because "all entries" is a per-reader notion: with chunked-bitmap storage one logical
   * entry spans several slots, so every concrete reader answers with a
   * {@link ChunkAggregatingIterator} over an unbounded range rather than a per-slot walk.
   *
   * @return iterator over all key-value pairs
   */
  public abstract Iterator<Map.Entry<K, NodeReferences>> iterator();

  /**
   * Serialize {@code key} into a right-sized array, growing the shared buffer first when the key
   * needs more room than it has. Sizing BEFORE the write is the point: checking the returned length
   * afterwards is too late, the overrun has already happened. Range constructors need the bytes to
   * outlive the shared buffer (two bounds are live at once), hence the copy — once per cursor, never
   * per entry.
   *
   * @param key the key to serialize
   * @return the serialized bytes, exactly {@code serializeKey}'s length
   */
  protected final byte[] serializeKeyToArray(final K key) {
    requireNonNull(key, "key");
    byte[] buffer = getKeyBuffer();
    final int required = maxSerializedKeyLength(key);
    if (required > buffer.length) {
      buffer = new byte[required];
      setKeyBuffer(buffer);
    }
    final int length = serializeKey(key, buffer, 0);
    return Arrays.copyOf(buffer, length);
  }

  /** {@code prefix ‖ chunkIdx_be4} — a composite bound for the range cursor. */
  private static byte[] compositeBound(final byte[] prefix, final int chunkIdx) {
    requireNonNull(prefix, "prefix");
    final byte[] composite = new byte[prefix.length + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefix, 0, composite, 0, prefix.length);
    HOTKeySerializer.writeChunkIdxBE(composite, prefix.length, chunkIdx);
    return composite;
  }

  /**
   * Iterator over logical entries in a bounded composite-key range, grouping the chunk slots of one
   * logical key into a single {@link Map.Entry}. Shared by every HOT reader — the object-key
   * (CAS/NAME) and primitive-long (PATH) readers differ only in {@link #deserializeKey}, which
   * {@link LazyKeyEntry} already routes through.
   *
   * <h2>Why bounds are checked here and not by the caller</h2>
   * <p>
   * The cursor's composite bounds are a coarse <em>byte</em> window, not the logical key range: index
   * keys are not prefix-free (a CAS string value is raw UTF-8 with no terminator), so the upper
   * composite {@code serialize(max) ‖ 0xFFFFFFFF} also covers every key that byte-extends {@code max}
   * — "carpet" sorts below the ceiling built for "car". Callers that trimmed bounds positionally
   * afterwards (skip the first group if it equals min, the last if it equals max) were wrong twice
   * over: they let prefix-extensions through, and the equal group need not be first or last. The
   * exact bound test therefore lives here, comparing each group's LOGICAL key bytes (composite minus
   * the chunk trailer) against the serialized bound — at most two {@link Arrays#compareUnsigned} per
   * emitted group, and no key deserialization, so {@link LazyKeyEntry}'s laziness survives.
   *
   * <p>
   * Out-of-range groups are skipped rather than terminating the sweep, because the path-stack forward
   * walk is not strictly lex-monotonic across sibling subtrees (see {@link #lowerBoundKey}); the
   * composite ceiling still bounds the scan.
   */
  protected final class ChunkAggregatingIterator implements Iterator<Map.Entry<K, NodeReferences>>, AutoCloseable {
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable HOTRangeCursor cursor;
    /**
     * Serialized logical lower bound, or {@code null} for unbounded. Doubles as the cheap per-slot
     * pre-filter that suppresses the lex-residue the PEXT-routed seek can leave at the head of the
     * sweep: HOT sibling subtrees can have overlapping lex ranges (a bit that varies at the parent
     * level and inside a sibling's subtree cannot be a parent disc bit, so it lives deeper), which
     * makes the forward walk non-monotonic in the small.
     */
    private final byte @Nullable [] lowerBoundKey;
    private final boolean lowerInclusive;
    /** Serialized logical upper bound, or {@code null} for unbounded. */
    private final byte @Nullable [] upperBoundKey;
    private final boolean upperInclusive;
    /** Per-iterator chunk accumulator, reset per logical group. */
    private final NodeReferencesSerializer.ChunkAccumulator accumulator =
        new NodeReferencesSerializer.ChunkAccumulator();
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    private boolean closed;

    /**
     * @param lowerBoundKey serialized logical lower bound, or {@code null} for unbounded
     * @param lowerInclusive whether {@code lowerBoundKey} itself is in range
     * @param upperBoundKey serialized logical upper bound, or {@code null} for unbounded
     * @param upperInclusive whether {@code upperBoundKey} itself is in range
     */
    ChunkAggregatingIterator(final byte @Nullable [] lowerBoundKey, final boolean lowerInclusive,
        final byte @Nullable [] upperBoundKey, final boolean upperInclusive) {
      this.lowerBoundKey = lowerBoundKey;
      this.lowerInclusive = lowerInclusive;
      this.upperBoundKey = upperBoundKey;
      this.upperInclusive = upperInclusive;
      final PageReference rootRef = getRootReference();
      if (rootRef == null) {
        // Empty trie — no entries.
        this.trieReader = null;
        this.cursor = null;
        this.nextEntry = null;
        return;
      }
      // The cursor's byte window is DERIVED here rather than passed in, so it can never disagree
      // with the logical bounds — the invariant `fromComposite == lowerBoundKey ‖ chunk0` is what
      // lets slotWithinBounds skip the inclusive lower-bound compare. An unbounded lower end is
      // null, not an empty array: HOTRangeCursor only takes its deterministic navigateToLeftmostLeaf
      // branch on null, and an empty array instead routes through a PEXT descent on an all-zero
      // partial key plus a never-firing bound compare per entry for the whole sweep.
      final byte[] fromComposite = lowerBoundKey == null
          ? null
          : compositeBound(lowerBoundKey, 0);
      final byte[] toComposite = upperBoundKey == null
          ? null
          : compositeBound(upperBoundKey, 0xFFFFFFFF);
      this.trieReader = new HOTTrieReader(storageEngineReader);
      // range() is INSIDE the try: its constructor descends, so an exception thrown there would
      // otherwise strand a half-built iterator nothing can reach — this catch routes it through
      // closeQuietly so the reader and cursor are torn down.
      try {
        this.cursor = trieReader.range(rootRef, fromComposite, toComposite);
        advance();
      } catch (RuntimeException | Error e) {
        closeQuietly();
        throw e;
      }
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
      final Map.Entry<K, NodeReferences> result = nextEntry;
      try {
        advance();
      } catch (RuntimeException | Error e) {
        closeQuietly();
        throw e;
      }
      return result;
    }

    /**
     * Tear down the cursor and the trie reader. Idempotent, and safe to call at any point: an abandoned
     * scan MUST route here, because nothing else will — exhaustion and a throw close themselves, but a
     * consumer that simply stops pulling (a limit, a short-circuiting predicate) would otherwise keep
     * the leaf object and path stack reachable for the iterator's lifetime.
     */
    @Override
    public void close() {
      closeQuietly();
    }

    /** Idempotent: exhaustion, abandonment and a throw all route here. */
    private void closeQuietly() {
      if (closed) {
        return;
      }
      closed = true;
      // Drop the pending entry too: hasNext() is `nextEntry != null`, so leaving it set after a
      // throw would let a caller that catches and keeps draining receive the same group twice and
      // then re-enter the cursor after it was closed.
      nextEntry = null;
      if (cursor != null) {
        cursor.close();
      }
      if (trieReader != null) {
        trieReader.close();
      }
    }

    /**
     * Whether the slot's logical key (composite minus the chunk trailer) lies inside both bounds, read
     * straight off the leaf. Zero-copy on purpose: the composite ceiling is wider than the logical
     * range (index keys are not prefix-free), so a bounded scan visits groups it must reject, and
     * rejecting them here means {@link HOTLeafPage#getKey} is only ever paid for a group that is
     * actually emitted.
     */
    private boolean slotWithinBounds(final HOTLeafPage leaf, final int idx) {
      final byte[] lower = lowerBoundKey;
      // Only the EXCLUSIVE case needs a logical compare. The cursor's seek bound is
      // `lower ‖ chunk0` (derived in the constructor, so they cannot drift apart) and
      // HOTRangeCursor#isOutOfRange rejects every slot below it on every step — and
      // `composite >= lower ‖ 0` is exactly `logical >= lower`, since the composite is the logical
      // key followed by the chunk trailer. So for an inclusive lower bound this compare could only
      // ever repeat the cursor's answer, once per slot, over the whole sweep.
      if (lower != null && !lowerInclusive) {
        if (leaf.compareKeyPrefixPart(idx, HOTKeySerializer.CHUNK_IDX_BYTES, lower, lower.length) <= 0) {
          return false;
        }
      }
      final byte[] upper = upperBoundKey;
      if (upper != null) {
        final int cmp = leaf.compareKeyPrefixPart(idx, HOTKeySerializer.CHUNK_IDX_BYTES, upper, upper.length);
        return cmp < 0 || (cmp == 0 && upperInclusive);
      }
      return true;
    }

    private void advance() {
      // Cleared up front, not just on the exits: a throw out of the loop below (corrupt slot
      // payload) must not leave a stale entry behind for hasNext() to report.
      nextEntry = null;
      if (cursor == null) {
        return;
      }
      while (true) {
        // Head skip: move past slots that cannot start a group. Every skip/stop decision reads
        // the UNPINNED leaf and is validated before it takes effect; a torn read re-evaluates
        // the SAME slot on a refreshed copy — cursor position is derived only from validated
        // decisions, so it survives the reload.
        int tornRounds = 0;
        while (cursor.hasNext()) {
          final HOTLeafPage leaf = cursor.currentLeafPage();
          final int idx = cursor.currentEntryIndex();
          final boolean skip;
          try {
            // <=, not <: a composite of exactly CHUNK_IDX_BYTES carries a ZERO-length logical key, which
            // would emit an entry whose deserializeKey reads past the buffer. Such a slot is not a
            // well-formed chunk composite either way.
            skip = leaf.getKeyLength(idx) <= HOTKeySerializer.CHUNK_IDX_BYTES || !slotWithinBounds(leaf, idx);
          } catch (RuntimeException e) {
            if (cursor.validateLeaf()) {
              throw e; // stable bytes — genuine corruption, not a torn read
            }
            recoverTorn(++tornRounds);
            continue;
          }
          if (!cursor.validateLeaf()) {
            recoverTorn(++tornRounds);
            continue;
          }
          tornRounds = 0;
          if (!skip) {
            break;
          }
          cursor.advance();
        }
        if (!cursor.hasNext()) {
          closeQuietly(); // an empty or fully-rejected sweep must release its resources too
          return;
        }

        // Materialize the group's composite key ONCE — it doubles as the logical-key bytes for
        // deserializeKey at emit. Only in-bounds groups ever get here. The copy is validated
        // before anything derives from it: it names the group for the merge compares below, for
        // the emitted entry, and for the re-seek target when a torn read voids the merge.
        byte[] groupComposite = null;
        while (groupComposite == null) {
          final byte[] candidate;
          try {
            candidate = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
          } catch (RuntimeException e) {
            if (cursor.validateLeaf()) {
              throw e;
            }
            recoverTorn(++tornRounds);
            continue;
          }
          if (!cursor.validateLeaf()) {
            recoverTorn(++tornRounds);
            continue;
          }
          if (candidate == null) {
            // getKey() returns null for a slot the slot table does not address — and the stamp
            // just validated, so this is storage corruption, reported rather than dereferenced
            // into an opaque NullPointerException.
            throw new IllegalStateException(
                "HOT leaf slot " + cursor.currentEntryIndex() + " does not address a readable key");
          }
          groupComposite = candidate;
        }
        final int prefixLen = groupComposite.length - HOTKeySerializer.CHUNK_IDX_BYTES;
        final int compositeLen = groupComposite.length;

        // Merge the group's chunk slots. A torn read voids the WHOLE aggregate (the merge writes
        // into the accumulator as it goes), so recovery is wholesale: reset the accumulator,
        // re-seek the group's first slot by its validated composite, and re-merge from scratch.
        group: for (int groupAttempt = 0;; groupAttempt++) {
          checkTornBound(groupAttempt);
          while (cursor.hasNext()) {
            final HOTLeafPage leaf = cursor.currentLeafPage();
            final int idx = cursor.currentEntryIndex();
            final boolean groupEnd;
            try {
              groupEnd =
                  leaf.getKeyLength(idx) != compositeLen || leaf.compareKeyPrefix(idx, groupComposite, prefixLen) != 0;
              if (!groupEnd) {
                final long chunkIdx = leaf.readKeyIntBE(idx, prefixLen) & 0xFFFFFFFFL;
                if (!accumulator.addChunk(leaf, leaf.valueRef(idx), chunkIdx << 16, trieReader)) {
                  accumulator.reset();
                  cursor.restartAtComposite(groupComposite);
                  continue group;
                }
              }
            } catch (RuntimeException e) {
              if (cursor.validateLeaf()) {
                throw e;
              }
              accumulator.reset();
              cursor.restartAtComposite(groupComposite);
              continue group;
            }
            if (!cursor.validateLeaf()) {
              accumulator.reset();
              cursor.restartAtComposite(groupComposite);
              continue group;
            }
            if (groupEnd) {
              break;
            }
            cursor.advance();
          }
          break;
        }

        final NodeReferences groupRefs = accumulator.toNodeReferencesAndReset();
        if (groupRefs != null) {
          nextEntry = new LazyKeyEntry(groupComposite, prefixLen, groupRefs);
          return;
        }
      }
    }

    /** Bounded torn-recovery: refresh the cursor's leaf and let the caller re-evaluate in place. */
    private void recoverTorn(final int round) {
      checkTornBound(round);
      cursor.refreshLeaf();
    }

    private void checkTornBound(final int round) {
      if (round > MAX_TORN_WALK_RETRIES) {
        throw new IllegalStateException("HOT: chunk aggregation failed stamp validation on " + MAX_TORN_WALK_RETRIES
            + " consecutive rounds — sustained allocator thrashing");
      }
    }
  }
}
