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

import io.sirix.access.DatabaseType;
import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.HOTLookupCache;
import io.sirix.cache.HOTLookupKey;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.ProjectionIndexPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.ValidTimeIndexPage;
import io.sirix.utils.LogWrapper;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
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

  /** Only ever used to report a refused memoization, which nothing else surfaces. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(AbstractHOTIndexReader.class));

  protected final StorageEngineReader storageEngineReader;
  protected final IndexType indexType;
  protected final int indexNumber;

  /**
   * The mutable state one point-lookup chunk walk needs: a trie reader (whose construction allocates
   * path-stack arrays), a chunk accumulator (whose construction allocates a key array), and the rare
   * fallback seek buffer needed when a caller has no room for the chunk trailer. They are pooled as
   * ONE object because the pool handoff is atomic and the atomics are not free — the profile put the
   * pair of {@code getAndSet}/{@code compareAndSet} at 4.6% of a point lookup, and a walk never wants
   * one without the others.
   */
  private static final class ChunkWalkState {
    private static final byte[] EMPTY_SEEK_SCRATCH = new byte[0];

    private final HOTTrieReader trie;
    private final NodeReferencesSerializer.ChunkAccumulator accumulator;
    private byte[] seekScratch = EMPTY_SEEK_SCRATCH;

    private ChunkWalkState(final StorageEngineReader storageEngineReader) {
      trie = new HOTTrieReader(storageEngineReader);
      accumulator = new NodeReferencesSerializer.ChunkAccumulator();
    }

    private HOTTrieReader trie() {
      return trie;
    }

    private NodeReferencesSerializer.ChunkAccumulator accumulator() {
      return accumulator;
    }

    /**
     * Return a buffer capable of holding {@code prefix || chunkIdx_be4}. Reuse the caller's spare
     * capacity whenever possible; otherwise retain one fallback buffer with the pooled walk state.
     */
    private byte[] compositeSeekBuffer(final byte[] prefixBuf, final int prefixLen, final int compositeLen) {
      if (prefixBuf.length >= compositeLen) {
        return prefixBuf;
      }
      if (seekScratch.length < compositeLen) {
        seekScratch = new byte[compositeLen];
      }
      System.arraycopy(prefixBuf, 0, seekScratch, 0, prefixLen);
      return seekScratch;
    }
  }

  /**
   * Pooled per-walk state: constructing it is per-call garbage on the lookup path, so one instance is
   * parked here between calls ({@link HOTTrieReader#close()} drops the leaf snapshot and clears the
   * path but keeps the object reusable). Handed out via {@link AtomicReference#getAndSet} so
   * concurrent lookups through the same reader instance stay correct — a loser simply constructs a
   * fresh one, and only one walk can ever hold a given instance.
   */
  private final AtomicReference<ChunkWalkState> pooledWalkState = new AtomicReference<>();

  /**
   * The session's memoized point lookups, or {@code null} when this reader must not use them.
   *
   * <p>
   * Null for a WRITER-backed reader, and the reason is the same one that makes the cache free of an
   * invalidation protocol: entries are keyed by revision number and a committed revision's index
   * content is immutable, but an uncommitted transaction mutates the index UNDER a revision number
   * that is already a key here. A writer consulting the cache could therefore read its own
   * pre-modification answer back. It is exactly the distinction {@link #getRootReference} already
   * draws for the memoized root reference.
   * </p>
   */
  private final @Nullable HOTLookupCache lookupCache;

  /**
   * Whether the backing reader resolves pages through a transaction intent log.
   *
   * <p>
   * Captured once because it decides BOTH memoizations this class performs — the point-lookup cache
   * above and {@link #getRootReference}'s cached root — and they must never drift apart: an
   * intent-log reader resolves uncommitted pages under an already-committed revision number, so
   * either memoization would pin or serve content an in-flight commit can still replace.
   * </p>
   */
  private final boolean usesTrxIntentLog;

  /**
   * The reader's snapshot coordinates, captured once.
   *
   * <p>
   * Each of the three accessors runs {@code assertNotClosed()} and the last dereferences the revision
   * root page, so reading them per lookup put three guarded virtual calls on the hot path for values
   * that cannot change over a reader's lifetime.
   * </p>
   */
  private final long databaseId;

  private final long resourceId;

  private final int revisionNumber;

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
    // hasTrxIntentLog() rather than `instanceof StorageEngineWriter`: the writer hands out a plain
    // NodeStorageEngineReader carrying its intent log (StorageEngineWriter#getStorageEngineReader),
    // which is NOT a StorageEngineWriter yet resolves uncommitted pages under an already-committed
    // revision number. Nothing constructs an index reader over one today, but one call would poison
    // that revision process-wide; the intent-log test is exactly the property that matters.
    //
    // Taken straight off the reader's own buffer manager: routing through getResourceSession() is
    // the longer way round to the very same object, and would have meant a new method on the public
    // ResourceSession interface for a cache that is not per-resource at all.
    this.usesTrxIntentLog = storageEngineReader.hasTrxIntentLog();
    // DEGRADE, never throw. Before memoization an index reader needed nothing but a
    // StorageEngineReader, so requiring a wired-up buffer manager here would turn a working index
    // open into an NPE from a constructor for any reader that has none — a test double, a forwarding
    // decorator, an embedded wiring path. Missing infrastructure is a reason to skip the cache, not
    // to fail the read; the two branches below are the same "no cache" outcome as a writer-backed
    // reader, which is the behaviour that shipped before this class memoized anything.
    final HOTLookupCache sessionCache;
    if (usesTrxIntentLog) {
      sessionCache = null;
    } else {
      final BufferManager bufferManager = storageEngineReader.getBufferManager();
      sessionCache = bufferManager == null
          ? null
          : bufferManager.getHOTLookupCache();
    }
    this.databaseId = storageEngineReader.getDatabaseId();
    this.resourceId = storageEngineReader.getResourceId();
    this.revisionNumber = storageEngineReader.getRevisionNumber();
    // A disabled cache is treated as no cache at all. Otherwise every lookup still built two keys,
    // copied the key bytes and drained the whole posting list into a long[] only to have put()
    // refuse it — which also meant maxEntries=0 did not actually measure the uncached path.
    //
    // databaseId == 0 disables it too, and that one is a correctness guard rather than a
    // micro-optimisation: ResourceConfiguration#getDatabaseId returns 0 when no DatabaseConfiguration
    // was attached, real ids are random POSITIVE longs, and resource ids restart at 0 in every
    // database — so an unattached configuration would file two unrelated databases under the same
    // (0, 0, revision, ...) slice of a JVM-GLOBAL table and answer one resource's lookup with
    // another's posting list. Every production open path attaches one; this is what makes that a
    // property the cache enforces rather than one it assumes.
    this.lookupCache = sessionCache != null && sessionCache.isEnabled() && databaseId != 0L
        ? sessionCache
        : null;
  }

  /**
   * Sentinel array length marking a memoized ABSENT key.
   *
   * <p>
   * A present key always has at least one node key — {@code collectChunksViaLowerBoundWalk} returns
   * {@code null} rather than an empty result — so zero length is free to mean "asked before, not
   * there". Worth memoizing precisely because a miss is the EXPENSIVE case: an absent key cannot be
   * answered by the landing-leaf fast path and falls through to the full lower bound, which
   * re-descends the trie comparing per-child first keys.
   * </p>
   */
  private static final long[] ABSENT = new long[0];

  /**
   * Reject a search mode a memoized point lookup cannot answer.
   *
   * <p>
   * ONE copy, in the class that owns {@link #pointLookup}, rather than one per {@code get} overload:
   * the invariant belongs to the cache key — {@link HOTLookupKey} does not carry the mode, so a
   * mode-sensitive answer would be served across modes — and a subclass that adds an overload must
   * inherit the guard rather than remember to restate it.
   * </p>
   *
   * @param mode the caller's search mode
   * @throws IllegalArgumentException if {@code mode} is anything but {@link SearchMode#EQUAL}
   */
  protected static void requireEqualMode(final SearchMode mode) {
    if (mode != SearchMode.EQUAL) {
      throw new IllegalArgumentException("get supports only SearchMode.EQUAL, got " + mode + "; use the range cursors");
    }
  }

  /** Borrowing cache key over the caller's serialization buffer — see {@code HOTLookupKey.probe}. */
  private HOTLookupKey probeKey(final byte[] keyBuf, final int keyLen) {
    return HOTLookupKey.probe(databaseId, resourceId, revisionNumber, indexType, indexNumber, keyBuf, 0, keyLen);
  }

  /**
   * Answer a point lookup for {@code keyBuf[0, keyLen)}, memoizing the result for this revision.
   *
   * <p>
   * The whole point of the cache: for a committed revision the walk below is deterministic, so
   * repeating it for a key already asked about is pure CPU waste. On a miss the freshly computed
   * answer is admitted — including the absent case — subject to
   * {@link HOTLookupCache#MAX_CACHED_NODE_KEYS}.
   * </p>
   *
   * @param keyBuf buffer holding the serialized logical key
   * @param keyLen serialized length of the key
   * @return the references for the key, or {@code null} when it has none
   */
  protected final @Nullable NodeReferences pointLookup(final byte[] keyBuf, final int keyLen) {
    // Unconditionally, not as a side effect of building a probe key: HOTLookupKey.probe runs the
    // identical checkFromIndexSize, so without this the SAME malformed (keyBuf, keyLen) is diagnosed
    // or waved through depending on whether a cache happens to be configured — a writer-backed
    // reader or sirix.hotLookupCache.maxEntries=0 took the unchecked path.
    Objects.checkFromIndexSize(0, keyLen, requireNonNull(keyBuf, "keyBuf").length);
    final HOTLookupCache cache = lookupCache;
    if (cache == null) {
      return collectChunksViaLowerBoundWalk(keyBuf, keyLen);
    }
    // ONE key object, hashed ONCE, reused for the probe and for the admission. Building it per phase
    // instead cost three allocations and three full passes over the key bytes on every miss — and a
    // miss is the path that must not end up slower than the walk the cache exists to avoid.
    final HOTLookupKey probe = probeKey(keyBuf, keyLen);
    final long[] hit = cache.get(probe);
    if (hit != null) {
      // A fresh NodeReferences over a COPY per hit. Both halves are load-bearing: the class is
      // mutable and getNodeKeys() hands out the live set, so sharing the cached instance would turn
      // "every lookup returns a fresh object" into an aliasing contract the first mutating consumer
      // would break for everyone else — and `hit` is the cache's own stored array, which a consumer
      // must never be handed a reference to.
      return hit.length == 0
          ? null
          : NodeReferences.copyOfSortedUnchecked(hit);
    }
    // Snapshot the key bytes BEFORE the walk, not after it. `probe` BORROWS the caller's reusable
    // serialization buffer and froze its hash at construction. Owning the admission key up front
    // keeps lookup and admission inseparable even if the canonical walk later starts reusing that
    // same scratch buffer. Copying afterwards could otherwise file the entry under
    // hash(bytes-before-the-walk) while carrying bytes-after. The copy costs one byte[] per cache
    // miss, and only on the committed-reader path where an answer may be admitted.
    final HOTLookupKey owned = probe.owned();
    // Captured BEFORE the walk reads a single page, and handed back at admission. An invalidation
    // sweep that overlaps this walk bumps the generation, so the answer below — which may have been
    // built from pages the sweep is discarding — is refused rather than resurrected under a revision
    // number truncateTo is about to re-issue over different content. Ordering the sweep's own steps
    // cannot achieve this; see HOTLookupCache#generation.
    final long generation = cache.generation();
    final NodeReferences computed = collectChunksViaLowerBoundWalk(keyBuf, keyLen);
    memoize(cache, owned, computed, generation);
    return computed;
  }

  /**
   * Admit a freshly computed answer, absent results included.
   *
   * @param cache the cache to admit into
   * @param key the OWNING key, copied off the probe before the walk ran — see {@link #pointLookup}
   * @param computed the answer, or {@code null} when the key has none
   * @param generation the cache's sweep generation, read before the answer was computed
   */
  private static void memoize(final HOTLookupCache cache, final HOTLookupKey key,
      final @Nullable NodeReferences computed, final long generation) {
    if (computed == null) {
      cache.put(key, ABSENT, generation);
      return;
    }
    // Gate BEFORE exporting, so an oversized posting list is never materialized here.
    if (computed.cardinality() > HOTLookupCache.MAX_CACHED_NODE_KEYS) {
      return; // too big to be worth copying on every hit — see MAX_CACHED_NODE_KEYS
    }
    // Sized and filled by ONE read of the representation. Sizing from cardinality() and then filling
    // through forEachNodeKey is two independent reads, and a disagreement between them yields a
    // TRUNCATED posting list that is indistinguishable from a complete one — the failure this cache
    // must never produce, since a wrong answer here is served silently for the rest of the revision.
    // The bound itself is re-checked by put(), which refuses an oversized array outright, so there is
    // no second length test here: the gate above exists to avoid MATERIALIZING a huge posting list,
    // not to enforce the bound twice.
    final long[] nodeKeys = computed.toSortedArray();
    // A zero-length array is the ABSENT sentinel, so a non-null-but-empty result must NOT be stored:
    // it would come back as null on the next hit, and NameIndex/PathIndex distinguish those two
    // outcomes. Both producers guarantee non-empty today (the accumulator returns null for an empty
    // result rather than an empty NodeReferences), but the sentinel makes the invariant
    // load-bearing, so it is checked here rather than assumed.
    if (nodeKeys.length == 0) {
      return;
    }
    // The ordering contract is enforced HERE, once per array ever cached, rather than by
    // NodeReferences.copyOfSortedUnchecked on every hit — the hit path would re-derive a property of an
    // immutable array it had already established, at up to MAX_CACHED_NODE_KEYS serial unsigned
    // comparisons a time. Not dropped, because copyOfSortedUnchecked's own javadoc explains why an
    // assert is
    // not enough: an out-of-order run makes contains()'s unsigned binary search report present node
    // keys as absent, with no exception anywhere.
    //
    // REFUSED, not thrown. Memoization is an optimization the caller never asked for, so a run this
    // cannot admit must cost a recomputation and nothing else — the same bargain the class makes for
    // an evicted entry. Throwing instead would fail a query that had already
    // computed its answer, and only on the configurations where a HOTLookupCache exists: the same
    // lookup would keep succeeding on a writer-backed reader, under EmptyBufferManager, or with the
    // cache sized to zero. That is the configuration-dependent diagnosis this read path is written to
    // avoid, and it would be reached with the result in hand.
    if (!NodeReferences.isSortedAscending(nodeKeys)) {
      LOGGER.warn("Refusing to memoize a posting list that is not strictly ascending unsigned; key={}", key);
      return;
    }
    cache.put(key, nodeKeys, generation);
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
    Objects.checkFromIndexSize(0, prefixLen, requireNonNull(prefixBuf, "prefixBuf").length);
    final int compositeLen = Math.addExact(prefixLen, HOTKeySerializer.CHUNK_IDX_BYTES);
    final PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }

    final ChunkWalkState pooled = pooledWalkState.getAndSet(null);
    final ChunkWalkState state = pooled != null
        ? pooled
        : new ChunkWalkState(storageEngineReader);
    final HOTTrieReader trie = state.trie();
    final NodeReferencesSerializer.ChunkAccumulator accumulator = state.accumulator();
    try {
      // HOTTrieReader's valid-length lowerBound makes spare capacity safe: routing, leaf lookup and
      // mismatch-bit logic all ignore bytes after compositeLen. The common generic and long readers
      // therefore append the zero chunk trailer straight into their thread-local buffers. Only an
      // exactly-sized caller buffer uses the pooled fallback scratch, allocating at most when it grows.
      final byte[] fromBytes = state.compositeSeekBuffer(prefixBuf, prefixLen, compositeLen);
      HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);
      // The whole walk runs against UNPINNED leaves under optimistic stamps: each leaf's read
      // batch — the per-slot prefix compares, the chunkIdx reads, the payload merges — is
      // validated once before its outcome (stop, or advance to the next leaf) takes effect. A
      // torn batch poisons the accumulator, so recovery is wholesale: reset and re-walk from a
      // fresh lower-bound descent. Content per PageReference is immutable, so every retry
      // re-derives the identical result.
      for (int walkAttempt = 0; walkAttempt < HOTTrieReader.MAX_STAMP_RETRIES; walkAttempt++) {
        accumulator.reset();
        // One decision route: every logical lookup starts at canonical PEXT lowerBound. Do not
        // duplicate its navigate/find/insertion-point subset here; that shortcut diverged exactly
        // when chunk 0 was absent and the first live chunk sat in the next leaf.
        final HOTTrieReader.LowerBoundResult lowerBound = trie.lowerBound(rootRef, fromBytes, compositeLen);
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
      throw new IllegalStateException("HOT: chunk walk failed stamp validation on every one of "
          + HOTTrieReader.MAX_STAMP_RETRIES + " attempts — sustained allocator thrashing");
    } finally {
      accumulator.reset(); // no-op on the success path; drops partial state if the walk threw
      trie.close(); // clears the reader's leaf snapshot + path; the object stays reusable
      pooledWalkState.compareAndSet(null, state);
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
  /**
   * Root reference, memoized after the first resolution for read-only snapshots: the chain
   * {@code RevisionRootPage -> index page -> child reference} is immutable for a committed revision,
   * yet the profile showed every point lookup re-walking it (~16% of a hit). A writer-backed reader
   * resolves fresh per call — an in-flight commit can replace the index page and its child
   * references.
   */
  private volatile @Nullable PageReference cachedRootReference;

  public @Nullable PageReference getRootReference() {
    PageReference root = cachedRootReference;
    if (root != null) {
      return root;
    }
    root = resolveRootReference();
    // The SAME captured predicate as lookupCache, so the two memoization decisions cannot drift: a
    // reader carrying a transaction intent log resolves uncommitted pages, so memoizing its root
    // reference would pin a chain an in-flight commit can replace. (`instanceof StorageEngineWriter`
    // missed the plain reader the writer hands out over its own intent log.)
    if (root != null && !usesTrxIntentLog) {
      cachedRootReference = root; // benign race: resolution is idempotent for a snapshot
    }
    return root;
  }

  private @Nullable PageReference resolveRootReference() {
    final RevisionRootPage rootPage = storageEngineReader.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = storageEngineReader.getPathPage(rootPage);
        yield pathPage == null
            ? null
            : pathPage.getIndexReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = storageEngineReader.getCASPage(rootPage);
        yield casPage == null
            ? null
            : casPage.getIndexReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = storageEngineReader.getNamePage(rootPage);
        yield namePage == null
            ? null
            : namePage.getIndexReference(nameDatabaseType(), indexNumber);
      }
      case PROJECTION -> {
        final ProjectionIndexPage projectionPage = storageEngineReader.getProjectionIndexPage(rootPage);
        yield projectionPage == null
            ? null
            : projectionPage.getIndexReference(indexNumber);
      }
      case VALIDTIME -> {
        final ValidTimeIndexPage vtPage = storageEngineReader.getValidTimeIndexPage(rootPage);
        yield vtPage == null
            ? null
            : vtPage.getIndexReference(indexNumber);
      }
      default -> null;
    };
  }

  private DatabaseType nameDatabaseType() {
    final var resourceSession = storageEngineReader.getResourceSession();
    if (resourceSession instanceof JsonResourceSession) {
      return DatabaseType.JSON;
    }
    if (resourceSession instanceof XmlResourceSession) {
      return DatabaseType.XML;
    }
    throw new IllegalStateException("Cannot determine the database type for NAME index " + indexNumber
        + " from resource session " + resourceSession);
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
   * Out-of-range groups are skipped defensively because the composite byte window is deliberately
   * coarser than the logical-key interval. The canonical HOT path walk itself is lex-monotonic under
   * the writer-enforced disjoint-subtree invariant.
   */
  protected final class ChunkAggregatingIterator implements Iterator<Map.Entry<K, NodeReferences>>, AutoCloseable {
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable HOTRangeCursor cursor;
    /**
     * Serialized logical lower bound, or {@code null} for unbounded. Doubles as the cheap per-slot
     * filter required because the composite byte window is not the logical-key interval: serializers
     * such as raw UTF-8 CAS strings are not prefix-free.
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
      cursor.recoverTorn(round, "HOT chunk aggregation");
    }

    private void checkTornBound(final int round) {
      if (round > HOTTrieReader.MAX_STAMP_RETRIES) {
        throw HOTTrieReader.stampRetriesExhausted("HOT chunk aggregation");
      }
    }
  }
}
