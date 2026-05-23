/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;

import java.util.Arrays;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.LongSupplier;

/**
 * Pure bulk-build / rebuild primitive for HOT (Height-Optimized Trie) secondary indexes.
 *
 * <p>Given a list of {@code (key, value)} entries sorted ascending by unsigned big-endian
 * (lexicographic) key, {@link #build} materializes a correct HOT subtree out of real
 * {@link HOTLeafPage} and {@link HOTIndirectPage} objects and returns its root. The function is
 * a faithful port of the formally-verified model in {@code HOTFormalModelTest} (the
 * {@code buildR} / {@code bulk} / sparse-path encoding triple) — see
 * {@code docs/HOT_FORMAL_FOUNDATION.md} (Theorem 1). It is property-test-verified there over
 * thousands of adversarial key sets.
 *
 * <p><b>Purity.</b> {@code build} allocates exclusively new pages and never mutates any page
 * passed in or referenced elsewhere. It performs no I/O. Page keys are drawn from the supplied
 * {@link LongSupplier} — the caller controls allocation so the result can be spliced in via
 * copy-on-write with full path-copy to the root (Sirix's persistent-page discipline).
 *
 * <p><b>Algorithm.</b> Two deterministic phases (foundation §3.2):
 * <ol>
 *   <li>Build {@code R(S)}, Binna's binary Patricia trie of the key set, by the standard MSDB
 *       recursion. The most-significant differing bit (MSDB) between two variable-length keys
 *       compared as unsigned big-endian is the first differing byte, then the most significant
 *       differing bit within that byte.</li>
 *   <li>Compress {@code R(S)}: cut a {@link HOTLeafPage} at the highest {@code R(S)} subtree
 *       whose key group fits a page (&le; {@link HOTLeafPage#MAX_ENTRIES} entries <em>and</em>
 *       &le; page byte capacity); SMHP-partition the remaining upper structure into
 *       {@link HOTIndirectPage} compound nodes of &le; {@link HOTIndirectPage#MAX_NODE_ENTRIES}
 *       children; sparse-path-encode the stored partial keys.</li>
 * </ol>
 * Because a leaf page stores a <em>complete</em> {@code R(S)} subtree, every ancestor
 * discriminative bit is constant across the leaf's keys (Fact R1), so invariant I5 holds
 * regardless of leaf cardinality.
 *
 * <p><b>Tombstones.</b> A tombstone (a {@code NodeReferences} value tagged {@code 0xFE}) is a
 * key and is included like any other entry — it occupies a leaf slot and participates in
 * {@code R(S)} construction.
 *
 * <p><b>MultiMask encoding consistency.</b> The stored partial of child {@code i} is the
 * sparse-path encoding: bit {@code j} is set iff the block BiNode {@code discBits[j]} lies on
 * the path to child {@code i} <em>and</em> that path takes the 1-side. Discriminative bits are
 * packed MSB-first by absolute bit position — {@code discBits[0]} (smallest absolute position,
 * most significant) gets weight {@code 1 << (m-1)}. This is exactly the bit order
 * {@link HOTIndirectPage}'s routing produces from {@code Long.compress} (SingleMask) and from
 * the chunked {@code computeMultiMaskPartialKey} (MultiMask): the lowest long-bit / lowest
 * result-bit corresponds to the least-significant captured key bit. The builder chooses the
 * SingleMask layout when every discriminative bit fits an 8-byte window and the MultiMask
 * layout otherwise; both decode the partials identically.
 *
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 */
public final class HOTBulkBuilder {

  /** Maximum children per indirect compound node (= HOT fanout {@code k}). */
  private static final int MAX_FANOUT = HOTIndirectPage.MAX_NODE_ENTRIES;

  /** Maximum entries that fit a leaf page by entry count (a byte-capacity check is separate). */
  private static final int MAX_LEAF_ENTRIES = HOTLeafPage.MAX_ENTRIES;

  private HOTBulkBuilder() {
    throw new AssertionError("no instances");
  }

  /**
   * A single {@code (key, value)} entry. Both arrays are treated as immutable; the builder
   * never modifies them and never retains references beyond the {@link #build} call other than
   * by copying their bytes into freshly allocated leaf pages.
   *
   * @param key   the full index key (variable-length, compared unsigned big-endian)
   * @param value the value payload (a tombstone is a 1-byte {@code 0xFE})
   */
  public record Entry(byte[] key, byte[] value) {
    public Entry {
      Objects.requireNonNull(key, "key");
      Objects.requireNonNull(value, "value");
    }
  }

  /**
   * The result of a bulk build: the root page and a {@link PageReference} that already has the
   * root swizzled in via {@link PageReference#setPage}. Every page in the subtree is reachable
   * from {@code rootReference} and has a page key drawn from the caller's allocator.
   *
   * @param rootReference a reference whose in-memory page is {@link #rootPage}
   * @param rootPage      the root of the built HOT subtree (a {@link HOTLeafPage} when the key
   *                      set fits a single page, otherwise a {@link HOTIndirectPage})
   * @param leafCount     number of leaf pages created
   * @param indirectCount number of indirect pages created
   */
  public record BuildResult(PageReference rootReference, Page rootPage, int leafCount,
                             int indirectCount) {}

  /**
   * Build a HOT subtree from sorted, distinct entries.
   *
   * @param sortedEntries     entries sorted strictly ascending by unsigned big-endian key; must
   *                          contain no duplicate keys
   * @param revision          the revision number stamped onto every created page
   * @param indexType         the index type ({@code PATH} / {@code CAS} / {@code NAME})
   * @param pageKeyAllocator  supplier of fresh persistent page keys; called once per created
   *                          page (leaf and indirect)
   * @return the build result; {@code rootPage} is {@code null}-free
   * @throws NullPointerException     if any argument is {@code null}
   * @throws IllegalArgumentException if {@code sortedEntries} is empty, not strictly ascending,
   *                                  contains a duplicate key, or contains a single entry whose
   *                                  bytes cannot fit a leaf page
   */
  public static BuildResult build(final java.util.List<Entry> sortedEntries, final int revision,
      final IndexType indexType, final LongSupplier pageKeyAllocator) {
    Objects.requireNonNull(sortedEntries, "sortedEntries");
    Objects.requireNonNull(indexType, "indexType");
    Objects.requireNonNull(pageKeyAllocator, "pageKeyAllocator");
    final int n = sortedEntries.size();
    if (n == 0) {
      throw new IllegalArgumentException("sortedEntries must be non-empty");
    }

    // Snapshot into primitive-friendly arrays; validate strict ascending order + distinctness.
    final byte[][] keys = new byte[n][];
    final byte[][] values = new byte[n][];
    for (int i = 0; i < n; i++) {
      final Entry e = sortedEntries.get(i);
      keys[i] = e.key();
      values[i] = e.value();
      if (i > 0) {
        final int cmp = Arrays.compareUnsigned(keys[i - 1], keys[i]);
        if (cmp == 0) {
          throw new IllegalArgumentException(
              "duplicate key at index " + i + ": " + hex(keys[i]));
        }
        if (cmp > 0) {
          throw new IllegalArgumentException("entries not sorted ascending at index " + i
              + ": " + hex(keys[i - 1]) + " > " + hex(keys[i]));
        }
      }
    }

    final BulkContext ctx = new BulkContext(keys, values, revision, indexType, pageKeyAllocator);
    final Page root = ctx.bulk(buildR(keys, 0, n - 1));
    final PageReference rootRef = new PageReference();
    rootRef.setPage(root);
    return new BuildResult(rootRef, root, ctx.leafCount, ctx.indirectCount);
  }

  // ======================================================================
  // R(S) — the binary Patricia trie (foundation §3).
  // ======================================================================

  /** A node of {@code R(S)}: either a leaf (a contiguous key group) or a branch. */
  private sealed interface RNode permits RLeaf, RBranch {
    int lo();

    int hi();
  }

  /** A leaf of {@code R(S)}: the contiguous key group {@code keys[lo..hi]}. */
  private record RLeaf(int lo, int hi) implements RNode {}

  /**
   * A branch of {@code R(S)}: splits {@code keys[lo..hi]} on the discriminative bit
   * {@code beta} (an absolute MSB-first bit position). All keys under {@code left} have
   * {@code key[beta] == 0}, all under {@code right} have {@code key[beta] == 1}.
   */
  private record RBranch(int lo, int hi, int beta, RNode left, RNode right) implements RNode {}

  private static int rSize(final RNode r) {
    return r.hi() - r.lo() + 1;
  }

  /**
   * Build {@code R(keys[lo..hi])} by the MSDB recursion. {@code keys} is sorted strictly
   * ascending unsigned; both halves of every branch are non-empty because {@code beta} is a
   * genuinely differing bit.
   */
  private static RNode buildR(final byte[][] keys, final int lo, final int hi) {
    if (lo == hi) {
      return new RLeaf(lo, hi);
    }
    final int beta = msdb(keys[lo], keys[hi]);
    // keys sorted + all agree above beta => a clean 0 -> 1 transition at beta exists in (lo,hi].
    int m = lo + 1;
    while (m <= hi && !bitAt(keys[m], beta)) {
      m++;
    }
    return new RBranch(lo, hi, beta, buildR(keys, lo, m - 1), buildR(keys, m, hi));
  }

  /**
   * Most significant differing bit position (absolute, MSB-first; bit 0 = MSB of byte 0) of two
   * distinct variable-length keys compared as unsigned big-endian. Missing trailing bytes are
   * treated as {@code 0x00}, so {@code "AB"} and {@code "AB\0..."} would be equal — the caller
   * guarantees distinct keys, so a differing bit always exists.
   */
  static int msdb(final byte[] a, final byte[] b) {
    final int max = Math.max(a.length, b.length);
    for (int i = 0; i < max; i++) {
      final int av = i < a.length ? (a[i] & 0xFF) : 0;
      final int bv = i < b.length ? (b[i] & 0xFF) : 0;
      final int x = av ^ bv;
      if (x != 0) {
        // Most significant differing bit within the byte: 0 = MSB ... 7 = LSB.
        final int bitInByte = Integer.numberOfLeadingZeros(x) - 24;
        return i * 8 + bitInByte;
      }
    }
    throw new IllegalArgumentException("msdb of equal keys " + hex(a));
  }

  /**
   * MSB-first absolute bit lookup: bit at absolute position {@code pos} is byte {@code pos/8},
   * bit-within-byte {@code 7 - pos%8}. Positions past the key length are implicit {@code 0}.
   */
  static boolean bitAt(final byte[] key, final int pos) {
    final int bytePos = pos >>> 3;
    if (bytePos >= key.length) {
      return false;
    }
    return (key[bytePos] & (1 << (7 - (pos & 7)))) != 0;
  }

  // ======================================================================
  // bulk — compression of R(S) into HOT pages (foundation §3.1 / §3.2).
  // ======================================================================

  /**
   * Mutable per-build state. Holds the key/value arrays and accumulates page-creation counts;
   * threading it explicitly keeps {@link HOTBulkBuilder} stateless and re-entrant.
   */
  private static final class BulkContext {
    private final byte[][] keys;
    private final byte[][] values;
    private final int revision;
    private final IndexType indexType;
    private final LongSupplier pageKeyAllocator;
    private int leafCount;
    private int indirectCount;

    BulkContext(final byte[][] keys, final byte[][] values, final int revision,
        final IndexType indexType, final LongSupplier pageKeyAllocator) {
      this.keys = keys;
      this.values = values;
      this.revision = revision;
      this.indexType = indexType;
      this.pageKeyAllocator = pageKeyAllocator;
    }

    /**
     * Compress the {@code R(S)} node {@code r} into a HOT page. A node becomes a leaf page when
     * its key group fits a page; otherwise it becomes an indirect page whose block is grown
     * greedily to {@code MAX_FANOUT} children (any frontier is invariant-correct by Theorem 1).
     */
    Page bulk(final RNode r) {
      final int size = rSize(r);
      // Try to cut a leaf here. A single-entry RLeaf must fit (a key set is materializable);
      // a larger group is attempted speculatively and, on overflow, recursed (it is an
      // RBranch because size > 1).
      if (size <= MAX_LEAF_ENTRIES) {
        final HOTLeafPage leaf = tryBuildLeaf(r.lo(), r.hi());
        if (leaf != null) {
          return leaf;
        }
        if (r instanceof RLeaf) {
          throw new IllegalArgumentException(
              "single entry exceeds leaf page capacity: key " + hex(keys[r.lo()]));
        }
      }
      // r has > MAX_LEAF_ENTRIES keys, or its group did not fit a page: it is an RBranch.
      return buildIndirect((RBranch) r);
    }

    /**
     * Speculatively build a leaf page holding {@code keys[lo..hi]}. Returns {@code null} if the
     * entries do not fit a page (entry-count or byte-capacity overflow signalled by
     * {@link HOTLeafPage#put} returning {@code false} on a genuine — non-duplicate — insert).
     * The keys are distinct, so a {@code false} return is unambiguously overflow. A discarded
     * speculative page is {@link HOTLeafPage#close closed} so its off-heap segment is released.
     */
    private HOTLeafPage tryBuildLeaf(final int lo, final int hi) {
      final int count = hi - lo + 1;
      if (count > MAX_LEAF_ENTRIES) {
        return null;
      }
      final HOTLeafPage leaf = new HOTLeafPage(pageKeyAllocator.getAsLong(), revision, indexType);
      for (int i = lo; i <= hi; i++) {
        if (!leaf.put(keys[i], values[i])) {
          // Byte-capacity overflow (entry-count overflow is excluded by the guard above, and
          // duplicates are excluded by build()'s validation). Release the speculative page.
          leaf.close();
          return null;
        }
      }
      leafCount++;
      return leaf;
    }

    /**
     * Compress one {@code R(S)} branch into a HOT indirect compound node. Mirrors
     * {@code HOTFormalModelTest.bulk}: greedily expand the block frontier (always splitting the
     * largest still-expandable frontier node) until the block has {@code MAX_FANOUT} children
     * or no frontier node can be split; then derive the discriminative-bit set, the
     * sparse-path partials, and recurse into each child.
     */
    private HOTIndirectPage buildIndirect(final RBranch root) {
      // Frontier: the block's exit points, each with the block-internal path that reaches it.
      // A path step is {beta, side} with side 0 = left, 1 = right.
      final RNode[] fnodes = new RNode[MAX_FANOUT];
      final int[][][] fpaths = new int[MAX_FANOUT][][];
      int fcount = 0;
      fnodes[fcount] = root.left();
      fpaths[fcount] = new int[][] {{root.beta(), 0}};
      fcount++;
      fnodes[fcount] = root.right();
      fpaths[fcount] = new int[][] {{root.beta(), 1}};
      fcount++;

      while (fcount < MAX_FANOUT) {
        // Pick the largest expandable (RBranch) frontier node.
        int idx = -1;
        int best = -1;
        for (int i = 0; i < fcount; i++) {
          if (fnodes[i] instanceof RBranch rb) {
            final int s = rSize(rb);
            if (s > best) {
              best = s;
              idx = i;
            }
          }
        }
        if (idx < 0) {
          break; // no frontier node can be expanded
        }
        final RBranch rb = (RBranch) fnodes[idx];
        final int[][] base = fpaths[idx];
        final int[][] leftPath = appendStep(base, rb.beta(), 0);
        final int[][] rightPath = appendStep(base, rb.beta(), 1);
        // Replace fnodes[idx] by rb.left(); insert rb.right() at idx+1, shifting the tail.
        System.arraycopy(fnodes, idx + 1, fnodes, idx + 2, fcount - (idx + 1));
        System.arraycopy(fpaths, idx + 1, fpaths, idx + 2, fcount - (idx + 1));
        fnodes[idx] = rb.left();
        fpaths[idx] = leftPath;
        fnodes[idx + 1] = rb.right();
        fpaths[idx + 1] = rightPath;
        fcount++;
      }

      // discBits = sorted unique absolute bit positions of the block's BiNodes.
      final TreeSet<Integer> bitSet = new TreeSet<>();
      for (int i = 0; i < fcount; i++) {
        for (final int[] step : fpaths[i]) {
          bitSet.add(step[0]);
        }
      }
      final int m = bitSet.size();
      final int[] discBits = new int[m];
      int di = 0;
      for (final int b : bitSet) {
        discBits[di++] = b;
      }

      // partial[i] = sparse-path encoding. densePK packs discBits[0] (most significant absolute
      // position) at the highest partial bit, so discBits[j] carries weight 1 << (m-1-j).
      final int[] partials = new int[fcount];
      final PageReference[] children = new PageReference[fcount];
      for (int i = 0; i < fcount; i++) {
        int p = 0;
        for (final int[] step : fpaths[i]) {
          if (step[1] == 1) {
            final int j = Arrays.binarySearch(discBits, step[0]);
            p |= 1 << (m - 1 - j);
          }
        }
        partials[i] = p;
        final Page childPage = bulk(fnodes[i]);
        final PageReference ref = new PageReference();
        ref.setPage(childPage);
        children[i] = ref;
      }

      // Height = 1 + max child height (a leaf page counts as height 0), so an indirect whose
      // children are all leaf pages has height 1 — the convention the writer relies on.
      int maxChildHeight = 0;
      for (final PageReference child : children) {
        final Page cp = child.getPage();
        final int h = cp instanceof HOTIndirectPage hi ? hi.getHeight() : 0;
        if (h > maxChildHeight) {
          maxChildHeight = h;
        }
      }
      indirectCount++;
      return assembleIndirect(discBits, partials, children, maxChildHeight + 1, revision,
          pageKeyAllocator);
    }
  }

  /** Append a {@code {beta, side}} step to a block-internal path, producing a fresh array. */
  private static int[][] appendStep(final int[][] base, final int beta, final int side) {
    final int[][] out = Arrays.copyOf(base, base.length + 1);
    out[base.length] = new int[] {beta, side};
    return out;
  }

  /**
   * Assemble a {@link HOTIndirectPage} from the discriminative-bit set, the sparse-path
   * partials, and the child references. Children must already be in strictly ascending
   * partial-key order (foundation §4, I7) — {@code R(S)} left-to-right order for a bulk build,
   * a contiguous re-encoded sub-block for an incremental split — so no re-sort is needed. Picks
   * the SingleMask layout when the bits fit an 8-byte window and the MultiMask layout otherwise;
   * both decode {@code partials} identically.
   *
   * <p>Package-private: shared with {@link HOTIncrementalInsert}'s {@code splitIndirect} and
   * {@code addEntry}, which re-encode a node's sub-block exactly as a bulk build encodes an
   * {@code R(S)} branch.
   *
   * @param discBits         discriminative bits as sorted ascending absolute (MSB-first)
   *                         positions; {@code discBits[0]} is the assembled node's MSB
   * @param partials         per-child sparse-path partial keys, parallel to {@code children}
   * @param children         child references in ascending partial-key order
   * @param height           the assembled node's height ({@code 1 + max child height})
   * @param revision         the revision stamped onto the created page
   * @param pageKeyAllocator supplier of a fresh persistent page key
   * @return a freshly allocated compound node
   */
  static HOTIndirectPage assembleIndirect(final int[] discBits, final int[] partials,
      final PageReference[] children, final int height, final int revision,
      final LongSupplier pageKeyAllocator) {
    final int numChildren = children.length;
    final int firstByte = discBits[0] >>> 3;
    final int lastByte = discBits[discBits.length - 1] >>> 3;
    final long pageKey = pageKeyAllocator.getAsLong();

    if (lastByte - firstByte < 8) {
      // SingleMask: an 8-byte window covers every discriminative bit.
      // Absolute bit (i*8 + b) within the window maps to long bit 63 - (i*8 + b).
      long bitMask = 0L;
      for (final int absBit : discBits) {
        final int bitInWindow = (absBit >>> 3) - firstByte;
        final int absBitInWindow = bitInWindow * 8 + (absBit & 7);
        bitMask |= 1L << (63 - absBitInWindow);
      }
      return numChildren <= 16
          ? HOTIndirectPage.createSpanNode(pageKey, revision, firstByte, bitMask, partials,
              children, height)
          : HOTIndirectPage.createMultiNode(pageKey, revision, firstByte, bitMask, partials,
              children, height);
    }

    // MultiMask: discriminative bits span more than 8 bytes. Group them by key byte.
    final java.util.TreeMap<Integer, Integer> maskByByte = new java.util.TreeMap<>();
    for (final int absBit : discBits) {
      final int bytePos = absBit >>> 3;
      final int maskBit = 1 << (7 - (absBit & 7));
      maskByByte.merge(bytePos, maskBit, (x, y) -> x | y);
    }
    final int numExtractionBytes = maskByByte.size();
    final byte[] extractionPositions = new byte[numExtractionBytes];
    final long[] extractionMasks = new long[(numExtractionBytes + 7) / 8];
    short msbIndex = Short.MAX_VALUE;
    int idx = 0;
    for (final var entry : maskByByte.entrySet()) {
      final int bytePos = entry.getKey();
      final int byteMaskBits = entry.getValue();
      extractionPositions[idx] = (byte) bytePos;
      // BE chunk packing: extraction byte at chunk-offset o occupies long bits (7-o)*8..+7.
      extractionMasks[idx / 8] |= ((long) (byteMaskBits & 0xFF)) << ((7 - idx % 8) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(byteMaskBits & 0xFF);
      final int absBitPos = bytePos * 8 + (7 - highBit);
      if (absBitPos < msbIndex) {
        msbIndex = (short) absBitPos;
      }
      idx++;
    }
    return numChildren <= 16
        ? HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision, extractionPositions,
            extractionMasks, numExtractionBytes, partials, children, height, msbIndex)
        : HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision, extractionPositions,
            extractionMasks, numExtractionBytes, partials, children, height, msbIndex);
  }

  private static String hex(final byte[] b) {
    return b == null ? "null" : java.util.HexFormat.of().formatHex(b);
  }
}
