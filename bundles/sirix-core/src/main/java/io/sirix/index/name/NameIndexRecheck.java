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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * The exact re-check a NAME read owes a name longer than
 * {@link NameKeySerializer#MAX_NAME_KEY_BYTES} (FMT-305).
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
   * @param truncated the marker decision from the serialized lookup key
   * @return {@code candidates} unchanged when the name is inside the bound, the exact subset
   *         otherwise, or {@code null} when no posting survives
   */
  public static @Nullable NodeReferences exactPostings(final StorageEngineReader reader,
      final NodeReferences candidates, final QNm name, final boolean truncated) {
    requireNonNull(reader, "reader");
    requireNonNull(candidates, "candidates");
    requireNonNull(name, "name");
    if (!truncated) {
      return candidates; // the stored key is the whole name: it already answers exactly
    }
    final String namespaceURI = name.getNamespaceURI();
    final String localName = name.getLocalName();
    return filteredPostings(reader, candidates, (uri, local) -> namespaceURI.equals(uri) && localName.equals(local));
  }

  static NamePredicate filter(final Set<QNm> includes, final Set<QNm> excludes) {
    requireNonNull(includes, "includes");
    requireNonNull(excludes, "excludes");
    final Map<String, Set<String>> included = namesByURI(includes);
    final Map<String, Set<String>> excluded = namesByURI(excludes);
    return (uri, localName) -> (included.isEmpty() || contains(included, uri, localName))
        && !contains(excluded, uri, localName);
  }

  private static Map<String, Set<String>> namesByURI(final Set<QNm> names) {
    if (names.isEmpty()) {
      return Map.of();
    }
    final Map<String, Set<String>> byURI = HashMap.newHashMap(names.size());
    for (final QNm name : names) {
      byURI.computeIfAbsent(name.getNamespaceURI(), ignored -> new HashSet<>()).add(name.getLocalName());
    }
    return byURI;
  }

  private static boolean contains(final Map<String, Set<String>> names, final String uri, final String localName) {
    final Set<String> localNames = names.get(uri);
    return localNames != null && localNames.contains(localName);
  }

  static @Nullable NodeReferences filteredPostings(final StorageEngineReader reader, final NodeReferences candidates,
      final NamePredicate keep) {
    requireNonNull(reader, "reader");
    requireNonNull(candidates, "candidates");
    requireNonNull(keep, "keep");
    final Roaring64Bitmap surviving = new Roaring64Bitmap();
    final LongIterator postings = candidates.getNodeKeys().getLongIterator();
    while (postings.hasNext()) {
      final long nodeKey = postings.next();
      RECHECK_RECORD_READS.incrementAndGet();
      final DataRecord record = reader.getRecord(nodeKey, IndexType.DOCUMENT, -1);
      if (!(record instanceof NameNode nameNode) || !(record instanceof Node node)) {
        continue;
      }
      final int localNameKey = nameNode.getLocalNameKey();
      final int uriKey = nameNode.getURIKey();
      final NodeKind nodeKind = node.getKind();
      if (localNameKey == -1) {
        continue;
      }
      final String localName = reader.getName(localNameKey, nodeKind);
      if (localName == null) {
        continue;
      }
      final String uri = uriKey == -1
          ? ""
          : reader.getName(uriKey, NodeKind.NAMESPACE);
      if (keep.test(uri == null
          ? ""
          : uri, localName)) {
        surviving.addLong(nodeKey);
      }
    }
    return surviving.isEmpty()
        ? null
        : new NodeReferences(surviving);
  }

  @FunctionalInterface
  interface NamePredicate {
    boolean test(String uri, String localName);
  }
}
