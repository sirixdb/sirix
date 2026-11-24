/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.name;

import io.brackit.query.atomic.QNm;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * The exact re-check a NAME read owes a name longer than {@link NameKeySerializer#MAX_NAME_KEY_BYTES}
 * (FMT-305).
 *
 * <p>
 * Such a name is stored under a prefix of itself, so several names can share one key and its
 * postings are candidates. Turning them back into an exact answer means asking each node what its
 * name actually is, which costs one record read per candidate. Nothing here runs for a name inside
 * the bound: {@link #RECHECK_RECORD_READS} stays at zero for every ordinary name, which is what the
 * work budget beside this package asserts.
 */
public final class NameIndexRecheck {

  /**
   * Record reads the exact re-check performed. Always counted — the re-check is a cold path by
   * construction, so there is nothing to gate, and a budget needs the figure to be live.
   */
  public static final AtomicLong RECHECK_RECORD_READS = new AtomicLong();

  private NameIndexRecheck() {
    throw new AssertionError("no instances");
  }

  /** Record reads performed by the exact re-check since this process started. */
  public static long recheckRecordReads() {
    return RECHECK_RECORD_READS.get();
  }

  /**
   * The postings of {@code candidates} that really carry {@code name}.
   *
   * @param reader the reader the postings were read through
   * @param candidates the postings stored under {@code name}'s stored key
   * @param name the name the lookup asked for
   * @return {@code candidates} unchanged when the name is inside the bound, the exact subset
   *         otherwise, or {@code null} when no posting survives
   */
  public static @Nullable NodeReferences exactPostings(final StorageEngineReader reader,
      final NodeReferences candidates, final QNm name) {
    requireNonNull(reader, "reader");
    requireNonNull(candidates, "candidates");
    requireNonNull(name, "name");
    if (!NameKeySerializer.isTruncated(name)) {
      return candidates; // the stored key is the whole name: it already answers exactly
    }
    return retain(reader, candidates, resolved -> name.equals(resolved));
  }

  /**
   * The postings of {@code candidates} whose own name passes the filter. Used where a stored key may
   * be a prefix shared by several names, so the entry's own key cannot decide inclusion.
   *
   * @return the surviving postings, or {@code null} when none do
   */
  public static @Nullable NodeReferences filteredPostings(final StorageEngineReader reader,
      final NodeReferences candidates, final Set<QNm> includes, final Set<QNm> excludes) {
    requireNonNull(reader, "reader");
    requireNonNull(candidates, "candidates");
    requireNonNull(includes, "includes");
    requireNonNull(excludes, "excludes");
    return retain(reader, candidates,
        resolved -> (includes.isEmpty() || includes.contains(resolved)) && !excludes.contains(resolved));
  }

  /** Whether this entry's stored key may stand for more than one name, so its postings need a re-check. */
  public static boolean needsRecheck(final QNm entryName) {
    return NameKeySerializer.mayBeTruncated(entryName);
  }

  private static @Nullable NodeReferences retain(final StorageEngineReader reader, final NodeReferences candidates,
      final NamePredicate keep) {
    final Roaring64Bitmap surviving = new Roaring64Bitmap();
    final LongIterator postings = candidates.getNodeKeys().getLongIterator();
    while (postings.hasNext()) {
      final long nodeKey = postings.next();
      final QNm resolved = nameOf(reader, nodeKey);
      if (resolved != null && keep.test(resolved)) {
        surviving.addLong(nodeKey);
      }
    }
    return surviving.isEmpty()
        ? null
        : new NodeReferences(surviving);
  }

  /**
   * The name the addressed node actually carries, resolved from the dictionary rather than from any
   * cached name on a page-backed view — the same resolution the index builders use.
   *
   * @return the node's expanded name, or {@code null} when the node is gone or carries no name
   */
  private static @Nullable QNm nameOf(final StorageEngineReader reader, final long nodeKey) {
    RECHECK_RECORD_READS.incrementAndGet();
    final DataRecord record = reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    if (!(record instanceof NameNode nameNode) || !(record instanceof Node node)) {
      return null;
    }
    final int localNameKey = nameNode.getLocalNameKey();
    if (localNameKey == -1) {
      return null;
    }
    final String localName = reader.getName(localNameKey, node.getKind());
    if (localName == null) {
      return null;
    }
    final int uriKey = nameNode.getURIKey();
    final String uri = uriKey == -1
        ? ""
        : reader.getName(uriKey, NodeKind.NAMESPACE);
    return new QNm(uri == null
        ? ""
        : uri, "", localName);
  }

  @FunctionalInterface
  private interface NamePredicate {
    boolean test(QNm name);
  }
}
