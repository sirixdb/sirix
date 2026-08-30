package io.sirix.query.json;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;

/**
 * The elements of one JSON array that live in a given range of record pages.
 *
 * <p>
 * This is the unit of parallel work behind {@code SplittableSequence}: N of these cover an array's
 * children exactly once between them, and each can be iterated on its own thread.
 *
 * <p>
 * <b>Why pages and not indices.</b> The obvious split is by element position, but positions are not
 * addressable here — reaching element {@code i} means walking {@code i} siblings, so computing
 * chunk boundaries would cost a full serial pass. Measured on a 3.5 M element corpus that pass is
 * 307.8 ms against a 449 ms serial scan, which by Amdahl's law would cap the entire parallel design
 * near 1.4x. The page key space, by contrast, is known from {@code maxNodeKey} before anything is
 * read, so ranges are handed out in constant time and no serial pass exists to bound the speedup.
 *
 * <p>
 * The cost of that choice is real and worth stating: a record page holds every node, not only array
 * elements, so this inspects far more slots than there are elements — about 15x on the reference
 * corpus, whose movie objects each carry five fields plus two nested arrays. It is affordable only
 * because rejecting a slot is a parent-key read from the page image rather than a node
 * materialization. Measured end to end, page ranges beat the index-split design 91 ms to ~330 ms at
 * 16 threads despite scanning 15x the slots.
 *
 * <p>
 * <b>Order is not preserved.</b> Elements arrive in page order, which coincides with document order
 * only by construction accident. Callers requiring document order must not use this.
 */
final class ArrayPageRangeSequence extends LazySequence {

  /**
   * Node kinds that can appear as a JSON array member, indexed by kind id.
   *
   * <p>
   * The fused {@code OBJECT_NAMED_*} kinds are deliberately absent: those are an object's fields,
   * never an array's members, and on a record-oriented corpus they are the overwhelming majority of
   * slots. Rejecting them on a directory byte is what keeps the parent-key decode off the hot path.
   */
  private static final boolean[] ELEMENT_KIND = new boolean[256];

  static {
    for (final NodeKind kind : new NodeKind[] {NodeKind.OBJECT, NodeKind.ARRAY, NodeKind.STRING_VALUE,
        NodeKind.NUMBER_VALUE, NodeKind.BOOLEAN_VALUE, NodeKind.NULL_VALUE}) {
      ELEMENT_KIND[kind.getId() & 0xFF] = true;
    }
  }

  private final JsonResourceSession session;
  private final int revision;
  private final long arrayNodeKey;
  private final JsonDBCollection collection;
  private final JsonItemFactory itemFactory;
  private final long pageKeyFrom;
  private final long pageKeyTo;

  /**
   * @param pageKeyFrom first record page key, inclusive
   * @param pageKeyTo last record page key, exclusive
   */
  ArrayPageRangeSequence(final JsonResourceSession session, final int revision, final long arrayNodeKey,
      final JsonDBCollection collection, final JsonItemFactory itemFactory, final long pageKeyFrom,
      final long pageKeyTo) {
    this.session = session;
    this.revision = revision;
    this.arrayNodeKey = arrayNodeKey;
    this.collection = collection;
    this.itemFactory = itemFactory;
    this.pageKeyFrom = pageKeyFrom;
    this.pageKeyTo = pageKeyTo;
  }

  @Override
  public Iter iterate() {
    return new PageRangeIter();
  }

  private final class PageRangeIter extends BaseIter {

    /**
     * One transaction and one reader for the whole range, opened on first use.
     *
     * <p>
     * Per-chunk transaction open was the dominant fixed cost that held SirixDB's earlier parallel array
     * materialization to 2-3x on 19 cores; amortizing both over an entire page range is what keeps that
     * cost off the measurement here.
     */
    private JsonNodeReadOnlyTrx rtx;
    private StorageEngineReader reader;

    private long pageKey = pageKeyFrom;
    private KeyValueLeafPage page;
    private long pageBaseNodeKey;
    private int wordIndex;
    private long word;
    private Iter nested;
    private boolean closed;

    @Override
    public Item next() {
      if (closed) {
        return null;
      }
      if (rtx == null) {
        rtx = session.beginNodeReadOnlyTrx(revision);
        reader = session.createStorageEngineReader(revision);
      }
      while (true) {
        // An element that is itself a sequence rather than a single item — drain it first.
        if (nested != null) {
          final Item item = nested.next();
          if (item != null) {
            return item;
          }
          nested.close();
          nested = null;
        }
        if (page == null && !advancePage()) {
          return null;
        }
        final int slot = nextMatchingSlot();
        if (slot < 0) {
          page = null;
          continue;
        }
        final long nodeKey = pageBaseNodeKey | slot;
        if (!rtx.moveTo(nodeKey)) {
          continue;
        }
        final Sequence sequence = itemFactory.getSequence(rtx, collection);
        if (sequence == null) {
          continue;
        }
        if (sequence instanceof Item item) {
          return item;
        }
        nested = sequence.iterate();
      }
    }

    /** @return the next slot in the current page belonging to this array, or -1 when exhausted. */
    private int nextMatchingSlot() {
      while (true) {
        while (word == 0) {
          if (++wordIndex >= PageLayout.BITMAP_WORDS) {
            return -1;
          }
          word = page.logicalSlotBitmapWord(wordIndex);
        }
        final int slot = (wordIndex << 6) | Long.numberOfTrailingZeros(word);
        word &= word - 1;
        // Kind first, parent second, and the order is the difference between a viable scan and an
        // unusable one. The kind is a directory byte; the parent is a varint that has to be located
        // through the record's offset table and delta-decoded against the node key. On this corpus
        // the kind test rejects the roughly 14-in-15 slots that are object FIELDS rather than array
        // members, so the expensive test runs on a small fraction of what the loop visits.
        final int kindId = page.getSlotNodeKindId(slot) & 0xFF;
        if (kindId == 0) {
          // Legacy and reference-only overflow carriers have no directory kind/parent bytes. They
          // are rare, so resolve only that cold case through the point-read path; current flyweight
          // and sidecar records retain the directory-only filter above.
          if (!rtx.moveTo(pageBaseNodeKey | slot)) {
            continue;
          }
          final int resolvedKindId = rtx.getKind().getId() & 0xFF;
          if (ELEMENT_KIND[resolvedKindId] && rtx.getParentKey() == arrayNodeKey) {
            return slot;
          }
          continue;
        }
        if (ELEMENT_KIND[kindId] && page.getSlotParentKey(slot) == arrayNodeKey) {
          return slot;
        }
      }
    }

    /** @return {@code false} once the range is exhausted. */
    private boolean advancePage() {
      while (pageKey < pageKeyTo) {
        final long current = pageKey++;
        final var result = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, current, 0, revision));
        if (result == null || !(result.page() instanceof KeyValueLeafPage leaf)) {
          continue;
        }
        // Read bitmap words directly from the page: no 128-byte copy per page. The method is one
        // inline bitmap load on ordinary pages and merges cold overflow carriers only when present.
        for (int firstWord = 0; firstWord < PageLayout.BITMAP_WORDS; firstWord++) {
          final long logicalWord = leaf.logicalSlotBitmapWord(firstWord);
          if (logicalWord == 0L) {
            continue;
          }
          page = leaf;
          pageBaseNodeKey = leaf.getPageKey() << PageLayout.SLOT_COUNT_EXPONENT;
          wordIndex = firstWord;
          word = logicalWord;
          return true;
        }
      }
      return false;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      if (nested != null) {
        nested.close();
        nested = null;
      }
      page = null;
      // The items handed out reference this transaction, so it stays open for exactly as long as
      // the iteration that produced them.
      //
      // Holding it open longer was tried and does NOT help: 62 % of worker CPU is
      // PageKind#deserializeSlottedPage even though the underlying bytes are read from the OS only
      // once, so deserialized pages are not surviving between queries — but leaking the reader
      // instead of closing it measured 216 ms against 196 ms, slightly worse. Whatever drops those
      // pages, it is not this teardown.
      if (reader != null) {
        reader.close();
        reader = null;
      }
      if (rtx != null) {
        rtx.close();
        rtx = null;
      }
    }
  }
}
