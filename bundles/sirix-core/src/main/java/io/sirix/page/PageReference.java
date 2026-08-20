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

import io.sirix.io.HashAlgorithm;
import io.sirix.utils.ToStringHelper;
import org.jspecify.annotations.Nullable;
import io.sirix.page.interfaces.Page;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.Constants;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * Page reference pointing to a page. This might be on stable storage pointing to the start byte in
 * a file, including the length in bytes, and the checksum of the serialized page. Or it might be an
 * immediate reference to an in-memory instance of the deserialized page.
 * </p>
 */
public final class PageReference {

  private static final VarHandle PAGE;

  static {
    try {
      PAGE = MethodHandles.lookup().findVarHandle(PageReference.class, "page", Page.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** In-memory deserialized page instance. */
  private volatile Page page;

  /** Key in persistent storage. */
  private long key = Constants.NULL_ID_LONG;

  /** Log key. */
  private int logKey = Constants.NULL_ID_INT;

  /** TIL generation counter for epoch-based snapshot disambiguation. */
  private volatile int activeTilGeneration = -1;

  /** Sentinel marking a fresh immutable side page owned by the background append batch. */
  private static final int PENDING_PAGE_WRITE_GENERATION = Integer.MIN_VALUE;

  /**
   * Reachability-scoped resolution for copies made while this reference belongs to the transaction
   * intent log.
   *
   * <p>A page-reference copy cannot be found by the log when the original reference is later
   * flushed or rebound to a newer epoch. Historically the log retained a transaction-wide map
   * entry for every flushed page so such a copy could resolve eventually. A large bulk import is a
   * single transaction, making that history proportional to every page ever written even though
   * almost all of the references that needed it were already unreachable.</p>
   *
   * <p>Copies now share this small handle instead. The original drops it as soon as the durable key
   * has been installed; only a genuinely reachable stale copy keeps the resolution alive. A
   * superseded handle forwards to the new identity, preserving the same epoch-boundary semantics as
   * the old forwarding map without retaining unrelated history.</p>
   */
  private @Nullable TransactionLogReference transactionLogReference;

  /** Unique database ID to distinguish pages from different databases in global BufferManager. */
  private long databaseId = Constants.NULL_ID_LONG;

  /** Unique resource ID to distinguish pages from different resources in global BufferManager. */
  private long resourceId = Constants.NULL_ID_LONG;

  /** XXH3-64 of the referenced page fragment. Valid only when {@link #pageHashPresent} is true. */
  private long pageHash;

  /** Explicit checksum presence; every {@code long} value, including zero, is a valid XXH3 result. */
  private boolean pageHashPresent;

  private List<PageFragmentKey> pageFragments;

  private int hash;

  /**
   * Default constructor setting up an uninitialized page reference.
   */
  public PageReference() {
    pageFragments = Collections.emptyList();
  }

  /**
   * Copy constructor. Creates an independent copy; the {@code pageFragments} list is deep-copied so
   * mutations to the copy do not affect the original.
   *
   * @param reference {@link PageReference} to copy
   */
  public PageReference(final PageReference reference) {
    if (reference.hasPendingPageWrite()) {
      throw new IllegalStateException("A pending immutable-page reference must preserve object identity; "
          + "its owning HOT leaf copy must share the reference until the background append completes");
    }
    logKey = reference.logKey;
    activeTilGeneration = reference.activeTilGeneration;
    transactionLogReference = reference.transactionLogReference;
    // Do NOT copy the swizzled in-memory pointer when this reference carries a durable
    // resolution path (TIL logKey or disk key). A copy is invisible to the page's
    // lifecycle owner: when the TIL or the buffer cache closes/replaces the page it can
    // clear the ORIGINAL reference, but it cannot reach copies — an eagerly copied
    // pointer can only go stale, and WAS read after free through recycled frames (HOT
    // split cascades reading garbage through CoW'd indirect pages). Equality is
    // value-based, so a copy lazily re-resolves to the same warm cache entry or TIL
    // container. Only a fresh page that is in NEITHER the log NOR on disk keeps the
    // pointer — there it is the only path, and such pages are still private to the
    // single-threaded writer doing the cloning.
    page = (reference.logKey == Constants.NULL_ID_INT && reference.key == Constants.NULL_ID_LONG)
        ? reference.page
        : null;
    key = reference.key;
    databaseId = reference.databaseId;
    resourceId = reference.resourceId;
    pageHash = reference.pageHash;
    pageHashPresent = reference.pageHashPresent;
    // Preserve the shared immutable empty sentinel. The overwhelmingly common reference has no
    // fragments, and allocating an empty ArrayList for every CoW/reference copy retained roughly
    // one million otherwise-useless lists in a 10M-row ClickBench live-heap histogram. A later
    // addPageFragment call already replaces any empty list with a private mutable list.
    pageFragments = reference.pageFragments == null || reference.pageFragments.isEmpty()
        ? Collections.emptyList()
        : new ArrayList<>(reference.pageFragments);
    hash = reference.hash;
  }

  /**
   * Set in-memory instance of deserialized page.
   * 
   * Note: This just swaps the reference. The old page is NOT closed here. Pages are owned by the
   * cache and will be closed by the cache's removal listener when evicted, or by explicit operations
   * (TIL, transaction close).
   *
   * @param page deserialized page
   */
  public void setPage(final @Nullable Page page) {
    this.page = page;
  }

  /**
   * Clear the swizzled page only if it is still the exact instance the caller observed.
   *
   * <p>The identity CAS prevents stale cache cleanup from erasing a replacement installed between
   * observing the old page and clearing it. It is allocation-free and has the same volatile
   * publication semantics as {@link #setPage(Page)}.</p>
   *
   * @param expectedPage the exact page instance to clear
   * @return {@code true} if that instance was still installed and was cleared
   */
  public boolean clearPageIfSame(final Page expectedPage) {
    return PAGE.compareAndSet(this, expectedPage, null);
  }

  /**
   * Get in-memory instance of deserialized page.
   *
   * <p>A swizzled {@link io.sirix.page.HOTLeafPage} that has been closed is treated as a cache
   * miss: the swizzle is cleared and {@code null} is returned so the caller re-resolves through
   * the transaction-intent log ({@code logKey}) or persistent storage. This situation is routine
   * under copy-on-write: indirect pages copy their child reference arrays (including the swizzled
   * page pointer), and when the TIL later overwrites the entry at the shared {@code logKey} it
   * closes the replaced page instance — releasing its off-heap frame slot for reuse. Any stale
   * copy that kept the old pointer would otherwise read recycled frame memory (garbage keys and
   * values). The TIL keeps the container at the same {@code logKey} exactly so such copies can
   * re-resolve to the current page; returning the closed instance here would bypass that design.
   *
   * @return in-memory instance of deserialized page
   */
  public Page getPage() {
    final Page p = page;
    if (p instanceof HOTLeafPage hotLeaf && hotLeaf.isClosed()) {
      clearPageIfSame(p);
      return null;
    }
    return p;
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
   * 
   * @param key the page fragment key to add.
   * @return this instance
   */
  public PageReference addPageFragment(final PageFragmentKey key) {
    if (pageFragments.isEmpty()) {
      pageFragments = new ArrayList<>(2);
    }
    pageFragments.add(key);
    return this;
  }

  /**
   * Get the page fragments keys.
   * 
   * @return the page fragments keys
   */
  public List<PageFragmentKey> getPageFragments() {
    return pageFragments;
  }

  /**
   * Set the page fragment keys.
   * 
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
    hash = 0; // Clear cached hashCode since it includes logKey
    logKey = key;
    return this;
  }

  /**
   * Get the TIL generation this reference belongs to.
   *
   * @return TIL generation counter, or -1 if not in any TIL
   */
  public int getActiveTilGeneration() {
    return activeTilGeneration;
  }

  /**
   * Set the TIL generation this reference belongs to.
   *
   * @param generation the TIL generation counter
   * @return this instance
   */
  public PageReference setActiveTilGeneration(final int generation) {
    this.activeTilGeneration = generation;
    return this;
  }

  /**
   * Bind this reference to a fresh transaction-log identity.
   *
   * @param newLogKey array index in the destination log region
   * @param generation destination log generation
   * @param forwardPrior whether copies of the prior identity must follow this new identity
   * @return {@code true} if a prior shared handle was available and was forwarded
   */
  public boolean bindToTransactionLog(final int newLogKey, final int generation, final boolean forwardPrior) {
    final TransactionLogReference prior = transactionLogReference;
    if (hasPendingPageWrite()) {
      throw new IllegalStateException("A page reference with a pending immutable-page write cannot enter the "
          + "transaction intent log before that write is completed");
    }
    final TransactionLogReference next = new TransactionLogReference(newLogKey, generation);

    if (forwardPrior && prior != null) {
      TransactionLogReference terminal = prior;
      while (terminal.forwardedTo != null) {
        terminal = terminal.forwardedTo;
      }
      terminal.forwardedTo = next;
    }

    hash = 0;
    key = Constants.NULL_ID_LONG;
    logKey = newLogKey;
    activeTilGeneration = generation;
    transactionLogReference = next;
    return forwardPrior && prior != null;
  }

  /**
   * Share the transaction-log resolution of {@code source}. Used by the one trie-growth path that
   * fills an already-created reference rather than invoking this class's copy constructor.
   *
   * @param source reference whose resolution is being copied
   */
  public void shareTransactionLogReference(final PageReference source) {
    transactionLogReference = source.transactionLogReference;
  }

  /**
   * Capture the exact log identity currently carried by this reference. Snapshot cleanup uses the
   * captured handle because the reference object itself may be rebound while the snapshot is being
   * flushed.
   *
   * @return current shared handle, or {@code null} for a non-log reference
   */
  public @Nullable TransactionLogReference transactionLogReference() {
    return transactionLogReference;
  }

  /**
   * Whether a captured transaction-log identity has been superseded by a newer identity.
   *
   * <p>A copied {@link PageReference} shares this handle but has independent ordinary fields. When
   * that copy is rebound, the captured handle is forwarded while the original reference's raw log
   * key and generation remain unchanged. Snapshot cleanup must therefore consult the handle, not
   * just those raw fields, before publishing or re-owning the captured page.</p>
   *
   * @param reference captured transaction-log identity, or {@code null} for a legacy reference
   * @return {@code true} if a newer transaction-log identity supersedes it
   */
  public static boolean isSupersededTransactionLogReference(
      final @Nullable TransactionLogReference reference) {
    return reference != null && reference.forwardedTo != null;
  }

  /**
   * Publish a durable resolution for a captured transaction-log identity.
   *
   * <p>If the identity was forwarded while its snapshot was in flight, the flushed image is stale
   * and is deliberately ignored.</p>
   *
   * @param reference captured identity
   * @param persistentKey durable page offset
   * @param persistentHash durable page hash, if hashing is enabled
   * @param persistentHashPresent whether {@code persistentHash} is present
   * @return {@code true} if this identity was terminal and accepted the durable result
   */
  public static boolean completeTransactionLogReference(final @Nullable TransactionLogReference reference,
      final long persistentKey, final long persistentHash, final boolean persistentHashPresent) {
    if (reference == null || reference.forwardedTo != null) {
      return false;
    }
    // Publish the key last. A reader that observes it through the volatile read in
    // refreshTransactionLogReference() must also observe the hash and its presence bit.
    reference.persistentHash = persistentHash;
    reference.persistentHashPresent = persistentHashPresent;
    reference.persistentKey = persistentKey;
    return true;
  }

  /** Publish a durable transaction-log resolution with a present checksum. */
  public static boolean completeTransactionLogReference(final @Nullable TransactionLogReference reference,
      final long persistentKey, final long persistentHash) {
    return completeTransactionLogReference(reference, persistentKey, persistentHash, true);
  }

  /** Publish a durable transaction-log resolution without a checksum. */
  public static boolean completeTransactionLogReference(final @Nullable TransactionLogReference reference,
      final long persistentKey) {
    return completeTransactionLogReference(reference, persistentKey, 0L, false);
  }

  /**
   * Compatibility boundary for callers that still hold the canonical big-endian checksum bytes.
   * Production write, snapshot and spill paths use the primitive overloads above.
   */
  @Deprecated
  public static boolean completeTransactionLogReference(final @Nullable TransactionLogReference reference,
      final long persistentKey, final byte @Nullable [] persistentHash) {
    return persistentHash == null
        ? completeTransactionLogReference(reference, persistentKey)
        : completeTransactionLogReference(reference, persistentKey, HashAlgorithm.bytesToLong(persistentHash));
  }

  /**
   * Refresh this reference from its shared resolution handle.
   *
   * <p>Forwarding is path-compressed on the reference. A durable terminal result is copied into the
   * ordinary fields and the handle is dropped, leaving no per-page side object on the common,
   * already-resolved path.</p>
   *
   * @return {@code true} if the reference now points to durable storage
   */
  public boolean refreshTransactionLogReference() {
    TransactionLogReference resolution = transactionLogReference;
    if (resolution == null) {
      return false;
    }
    TransactionLogReference terminal = resolution;
    while (terminal.forwardedTo != null) {
      terminal = terminal.forwardedTo;
    }

    final long persistentKey = terminal.persistentKey;
    if (persistentKey != Constants.NULL_ID_LONG) {
      hash = 0;
      key = persistentKey;
      pageHash = terminal.persistentHash;
      pageHashPresent = terminal.persistentHashPresent;
      logKey = Constants.NULL_ID_INT;
      activeTilGeneration = -1;
      transactionLogReference = null;
      return true;
    }

    if (terminal != resolution || logKey != terminal.logKey || activeTilGeneration != terminal.generation) {
      hash = 0;
      logKey = terminal.logKey;
      activeTilGeneration = terminal.generation;
      transactionLogReference = terminal;
    }
    return false;
  }

  /**
   * Whether this object is a provably unused placeholder in a structural reference array.
   *
   * <p>{@code FullReferencesPage}'s copy constructor materializes an empty {@code PageReference}
   * for every unused array slot. A bottom-up serializer may omit only that exact state. Checking via
   * {@link #getPage()} is insufficient because it deliberately clears a closed HOT-leaf swizzle;
   * inspecting the raw field here ensures any in-memory page, live/snapshot handle, pending marker,
   * fragment chain, hash, or durable key remains an unresolved signal that blocks omission.</p>
   *
   * @return {@code true} only for a virgin unused structural placeholder
   */
  public boolean isVirginStructuralPlaceholder() {
    return key == Constants.NULL_ID_LONG && logKey == Constants.NULL_ID_INT && activeTilGeneration == -1
        && page == null && transactionLogReference == null && !hasPendingPageWrite()
        && pageFragments.isEmpty() && !pageHashPresent;
  }

  /**
   * Resolve a completed transaction-log handle and prove that this reference is now an ordinary
   * durable child with no live, frozen, or pending transaction claim.
   *
   * @return {@code true} only when a parent can safely serialize this reference as a disk offset
   */
  public boolean refreshesToUnclaimedDurableReference() {
    if (hasPendingPageWrite()) {
      return false;
    }
    refreshTransactionLogReference();
    return !hasPendingPageWrite() && key != Constants.NULL_ID_LONG && logKey == Constants.NULL_ID_INT
        && activeTilGeneration == -1 && transactionLogReference == null;
  }

  /**
   * Allocation-free, non-mutating admission check for a side reference retained by a canonical HOT
   * cache leaf. Unlike {@link #refreshesToUnclaimedDurableReference()}, this never refreshes a TIL
   * handle: cache admission accepts only the already-materialized, shallow durable shape.
   */
  boolean isRawCanonicalHOTCacheReference() {
    return page == null && pageFragments != null && pageFragments.isEmpty() && key != Constants.NULL_ID_LONG
        && logKey == Constants.NULL_ID_INT && activeTilGeneration == -1
        && transactionLogReference == null && !hasPendingPageWrite();
  }

  /**
   * Mark an immutable, uncommitted page as owned by the bounded background append batch.
   *
   * <p>HOT leaf CoW shares this exact reference while the marker is set. Publishing the result on
   * the foreground thread therefore updates every in-transaction leaf copy without allocating one
   * coordination object per projection segment. Once resolved, later leaf copies return to normal
   * independent PageReference copies.</p>
   *
   * <p>Only immutable pages may use this path. The append worker reads the page concurrently with
   * the foreground transaction; mutating it after this call is a data race and would make the
   * published hash and bytes disagree.</p>
   *
   * @param immutablePage page whose bytes will never change
   * @throws IllegalStateException if this reference is already durable, logged, or pending
   */
  public void bindPendingPageWrite(final Page immutablePage) {
    replaceAndBindPendingPageWrite(immutablePage, immutablePage);
  }

  /**
   * Replace a producer's immutable page with its bounded staging representation and publish the
   * pending marker as one validated state transition.
   *
   * <p>All checks happen before either field changes. The page pointer is replaced first and the
   * volatile marker is published last, so a HOT leaf copy that observes pending always shares the
   * exact reference whose page already names the staged representation.</p>
   *
   * @param expectedPage page currently owned by this reference
   * @param stagedPage immutable representation owned by the append batch
   */
  public void replaceAndBindPendingPageWrite(final Page expectedPage, final Page stagedPage) {
    if (expectedPage == null) {
      throw new IllegalArgumentException("expected pending page must not be null");
    }
    if (stagedPage == null) {
      throw new IllegalArgumentException("staged immutable page must not be null");
    }
    if (key != Constants.NULL_ID_LONG || logKey != Constants.NULL_ID_INT || transactionLogReference != null
        || hasPendingPageWrite()) {
      throw new IllegalStateException("Only a fresh, unresolved page reference can be staged for an immutable write");
    }
    if (page != expectedPage) {
      throw new IllegalStateException("The staged page must be the page currently owned by this reference");
    }
    page = stagedPage;
    // Volatile publication last: a thread that observes pending also observes the already-installed
    // immutable page pointer.
    activeTilGeneration = PENDING_PAGE_WRITE_GENERATION;
  }

  /** Whether this reference still owns an unresolved immutable-page write. */
  public boolean hasPendingPageWrite() {
    return activeTilGeneration == PENDING_PAGE_WRITE_GENERATION;
  }

  /**
   * Publish a flushed immutable-page write and release its heap payload.
   *
   * <p>Hash and key are installed first, the volatile page pointer is cleared next, and the volatile
   * pending marker is cleared last. Any observer that sees a resolved reference therefore also sees
   * its durable coordinates and can safely switch to disk.</p>
   */
  public void completePendingPageWrite(final long persistentKey, final long persistentHash,
      final boolean persistentHashPresent) {
    if (persistentKey == Constants.NULL_ID_LONG) {
      throw new IllegalArgumentException("A completed immutable-page write requires a durable key");
    }
    if (!hasPendingPageWrite()) {
      throw new IllegalStateException("This page reference has no pending immutable-page write");
    }
    final Page pendingPage = page;
    pageHash = persistentHash;
    pageHashPresent = persistentHashPresent;
    setKey(persistentKey);
    page = null;
    activeTilGeneration = -1;
    if (pendingPage instanceof OverflowPage overflowPage) {
      overflowPage.close();
    }
  }

  /** Publish a flushed immutable-page write with a present checksum. */
  public void completePendingPageWrite(final long persistentKey, final long persistentHash) {
    completePendingPageWrite(persistentKey, persistentHash, true);
  }

  /** Publish a flushed immutable-page write without a checksum. */
  public void completePendingPageWrite(final long persistentKey) {
    completePendingPageWrite(persistentKey, 0L, false);
  }

  /**
   * Compatibility boundary for callers that still hold the canonical big-endian checksum bytes.
   */
  @Deprecated
  public void completePendingPageWrite(final long persistentKey, final byte @Nullable [] persistentHash) {
    if (persistentHash == null) {
      completePendingPageWrite(persistentKey);
    } else {
      completePendingPageWrite(persistentKey, HashAlgorithm.bytesToLong(persistentHash));
    }
  }

  /** Drop an uncommitted pending payload while aborting/closing a failed transaction. */
  public void cancelPendingPageWrite() {
    if (hasPendingPageWrite()) {
      final Page pendingPage = page;
      page = null;
      activeTilGeneration = -1;
      if (pendingPage instanceof OverflowPage overflowPage) {
        overflowPage.close();
      }
    }
  }

  /**
   * Clear the cached hashCode. Must be called before changing key, logKey, databaseId, or resourceId
   * if the PageReference is already in a HashMap, since the hash depends on these values.
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
    return ToStringHelper.of(this)
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
      int h = (int) (databaseId ^ (databaseId >>> 32));
      h = 31 * h + (int) (resourceId ^ (resourceId >>> 32));
      h = 31 * h + logKey;
      h = 31 * h + (int) (key ^ (key >>> 32));
      hash = h;
    }
    return hash;
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (other instanceof PageReference otherPageRef) {
      return otherPageRef.databaseId == databaseId && otherPageRef.resourceId == resourceId
          && otherPageRef.logKey == logKey && otherPageRef.key == key;
    }
    return false;
  }

  /** Store a present checksum without allocating. */
  public void setHash(final long pageHash) {
    this.pageHash = pageHash;
    pageHashPresent = true;
  }

  /** Remove the checksum and reset its primitive storage. */
  public void clearHash() {
    pageHash = 0L;
    pageHashPresent = false;
  }

  /** Whether this reference carries a checksum. */
  public boolean hasHash() {
    return pageHashPresent;
  }

  /**
   * Return the primitive checksum value. Callers that distinguish absence must first use
   * {@link #hasHash()}; zero is a valid present checksum.
   */
  public long getHashAsLong() {
    return pageHash;
  }

  /** Copy checksum value and presence from another reference without materializing bytes. */
  public void copyHashFrom(final PageReference source) {
    if (source == null) {
      throw new IllegalArgumentException("source page reference must not be null");
    }
    pageHash = source.pageHash;
    pageHashPresent = source.pageHashPresent;
  }

  /**
   * Compatibility boundary for callers that still hold canonical big-endian checksum bytes.
   */
  @Deprecated
  public void setHash(final byte @Nullable [] hashInBytes) {
    if (hashInBytes == null) {
      clearHash();
    } else {
      setHash(HashAlgorithm.bytesToLong(hashInBytes));
    }
  }

  /**
   * Compatibility boundary that materializes the canonical big-endian checksum representation.
   * Production page I/O uses {@link #getHashAsLong()}.
   */
  @Deprecated
  public byte @Nullable [] getHash() {
    return pageHashPresent
        ? HashAlgorithm.longToBytes(pageHash)
        : null;
  }

  /**
   * Shared, reachability-scoped resolution for transaction-log reference copies.
   *
   * <p>The fields are intentionally private: only {@link PageReference} may mutate or interpret a
   * handle. The log treats instances as opaque snapshot tokens.</p>
   */
  public static final class TransactionLogReference {
    private final int logKey;
    private final int generation;
    private volatile @Nullable TransactionLogReference forwardedTo;
    private long persistentHash;
    private boolean persistentHashPresent;
    /** Volatile publication written after the two checksum fields above. */
    private volatile long persistentKey = Constants.NULL_ID_LONG;

    private TransactionLogReference(final int logKey, final int generation) {
      this.logKey = logKey;
      this.generation = generation;
    }
  }
}
