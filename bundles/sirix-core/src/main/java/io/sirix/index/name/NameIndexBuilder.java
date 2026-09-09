package io.sirix.index.name;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTBulkIndexLoader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/** Builder for the canonical HOT-backed NAME index. */
public final class NameIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(NameIndexBuilder.class));

  private final Set<QNm> includes;
  private final Set<QNm> excludes;
  private final HOTIndexWriter<QNm> hotWriter;
  public final StorageEngineReader storageEngineReader;

  /**
   * Bulk loader for the HOT index, non-{@code null} exactly when this builder starts against an empty
   * index tree — the normal "create an index over an already-shredded revision" case. Every entry is
   * collected and the trie is materialised once in {@link #finish()}.
   */
  private final @Nullable HOTBulkIndexLoader<QNm> bulkLoader;

  public NameIndexBuilder(final Set<QNm> includes, final Set<QNm> excludes, final HOTIndexWriter<QNm> hotWriter,
      final StorageEngineReader storageEngineReader) {
    this.includes = requireNonNull(includes);
    this.excludes = requireNonNull(excludes);
    this.hotWriter = requireNonNull(hotWriter);
    this.storageEngineReader = requireNonNull(storageEngineReader);
    // Bulk-load only into a virgin tree: the loader replaces the root instead of merging into it,
    // so an index that already holds entries keeps the incremental path.
    this.bulkLoader = hotWriter.isEmptyTree()
        ? hotWriter.createBulkLoader()
        : null;
  }

  public VisitResultType build(final QNm name, final ImmutableNode node) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return VisitResultType.CONTINUE;
    }

    try {
      addPosting(name, node.getNodeKey());
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return VisitResultType.CONTINUE;
  }

  /**
   * Primitive entry for feeders that hold no node object — the parallel bulk importer's coordinator
   * drain. Same include/exclude filter and the same bulk-vs-incremental arm as {@link #build}.
   */
  public void add(final QNm name, final long nodeKey) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));
    if (!included || excluded) {
      return;
    }
    try {
      addPosting(name, nodeKey);
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  /**
   * Add {@code nodeKey} to {@code name}'s posting list in the HOT index.
   *
   * <p>
   * A HOT slot write is an OR-merge of the incoming bitmap into the stored one, so adding one
   * reference needs neither a read-back of the stored references nor a re-insert of them. Doing so
   * made building an index quadratic in how many nodes share a name — which, for a name index, is the
   * point of the index.
   * </p>
   */
  private void addPosting(final QNm name, final long nodeKey) {
    if (bulkLoader != null) {
      bulkLoader.add(name, nodeKey);
    } else {
      hotWriter.indexNodeKey(name, nodeKey);
    }
  }

  /**
   * Materialise everything the traversal collected. Must be called exactly once, after the document
   * traversal that feeds {@link #build} has finished; a no-op unless this builder is bulk-loading.
   */
  public void finish() {
    if (bulkLoader != null) {
      bulkLoader.flush();
    }
  }
}
