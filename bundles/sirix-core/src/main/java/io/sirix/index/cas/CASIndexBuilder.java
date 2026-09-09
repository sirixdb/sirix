package io.sirix.index.cas;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.hot.HOTBulkIndexLoader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.settings.Constants;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.index.path.summary.PathSummaryReader;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Builder for CAS indexes.
 */
public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final HOTIndexWriter<CASValue> indexWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;

  /** Path-class records covered by {@link #paths}, resolved lazily on the first indexed node. */
  private @Nullable LongSet resolvedPCRs;

  /**
   * Bulk loader, non-{@code null} exactly when this builder starts against an empty index tree — the
   * normal "create an index over an already-shredded revision" case. Every entry is collected and the
   * trie is materialised once in {@link #finish()}; see {@link HOTBulkIndexLoader} for why that is
   * not the same cost as n incremental inserts.
   */
  private final @Nullable HOTBulkIndexLoader<CASValue> bulkLoader;

  public CASIndexBuilder(final HOTIndexWriter<CASValue> indexWriter, final PathSummaryReader pathSummaryReader,
      final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.indexWriter = indexWriter;
    this.type = type;
    // Bulk-load only into a virgin tree: the loader replaces the root instead of merging into
    // it, so an index that already holds entries (a rebuild over a populated definition) keeps
    // the incremental path.
    this.bulkLoader = indexWriter.isEmptyTree()
        ? indexWriter.createBulkLoader()
        : null;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      if (matchesIndexedPath(pathNodeKey)) {
        final Str strValue = switch (node) {
          case ImmutableValueNode immutableValueNode -> new Str(immutableValueNode.getValue());
          case ImmutableNumberNode immutableNumberNode -> new Str(String.valueOf(immutableNumberNode.getValue()));
          case ImmutableBooleanNode immutableBooleanNode -> new Str(String.valueOf(immutableBooleanNode.getValue()));
          // Fused kinds carry primitive values inline.
          case ObjectNamedNumberNode namedNum -> new Str(String.valueOf(namedNum.getValue()));
          case ObjectNamedBooleanNode namedBool -> new Str(String.valueOf(namedBool.getValue()));
          case ObjectNamedStringNode namedStr ->
            new Str(new String(namedStr.getRawValue(), Constants.DEFAULT_ENCODING));
          case null, default -> throw new IllegalStateException("Value not supported.");
        };

        // KEEP the conversion, do not merely validate with it. Storing the raw Str while the query
        // side casts its probe to the content type gave the serializer TWO shapes for one logical
        // value, and every reconciliation it grew for that was a bug: xs:boolean read a Str through
        // effective-boolean-value and mapped true and false onto one key, xs:float parsed the Str as
        // a double while the probe narrowed through float, and an out-of-range xs:integer saturated
        // on this side while the probe wrapped. One shape per type makes that class of defect
        // unrepresentable rather than fixed case by case.
        Atomic typedValue = strValue;
        boolean isOfType = false;
        try {
          if (type != Type.STR) {
            typedValue = AtomicUtil.toType(strValue, type);
          }
          isOfType = true;
        } catch (final SirixRuntimeException e) {
          LOGGER.debug("Value '{}' is not of type {}, skipping CAS index entry for node {}", strValue, type,
              node.getNodeKey(), e);
        }

        if (isOfType) {
          final CASValue value = new CASValue(typedValue, type, pathNodeKey);
          indexNode(node.getNodeKey(), value);
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  /**
   * Invalidate the resolved path-class cache. Sound for this builder's own frozen-summary traversal,
   * stale for an import-time feeder whose summary grows between drains — a class first minted after
   * the previous drain would stay invisible. Called once per drained chunk.
   */
  public void refreshIndexedPaths() {
    resolvedPCRs = null;
  }

  /**
   * Primitive entry for feeders that hold no node object — the parallel bulk importer's coordinator
   * drain, which stringifies each value from the same primitives the write path carried. Same path
   * filter, the same KEEP-the-conversion typing discipline and skip-on-conversion-failure as
   * {@link #process}, and the same bulk-vs-incremental arm.
   */
  public void add(final Str strValue, final long pathNodeKey, final long nodeKey) {
    try {
      if (!matchesIndexedPath(pathNodeKey)) {
        return;
      }
      Atomic typedValue = strValue;
      try {
        if (type != Type.STR) {
          typedValue = AtomicUtil.toType(strValue, type);
        }
      } catch (final SirixRuntimeException e) {
        LOGGER.debug("Value '{}' is not of type {}, skipping CAS index entry for node {}", strValue, type, nodeKey, e);
        return;
      }
      final CASValue value = new CASValue(typedValue, type, pathNodeKey);
      if (bulkLoader != null) {
        bulkLoader.add(value, nodeKey);
      } else {
        indexWriter.indexNodeKey(value, nodeKey);
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  /**
   * Whether {@code pathNodeKey} is one of the path-class records this index covers. An empty path
   * configuration means "index every path".
   *
   * <p>
   * The resolved PCR set is computed once and reused: the builder runs a single traversal of an
   * already-shredded revision, so the path summary cannot gain nodes underneath it, and
   * {@link PathSummaryReader#getPCRsForPaths(java.util.Collection)} allocates and fills a fresh
   * {@code LongOpenHashSet} on every call — once per value node, on the build hot path.
   * </p>
   */
  private boolean matchesIndexedPath(final long pathNodeKey) {
    if (paths.isEmpty()) {
      return true;
    }
    LongSet pcrs = resolvedPCRs;
    if (pcrs == null) {
      pcrs = pathSummaryReader.getPCRsForPaths(paths);
      resolvedPCRs = pcrs;
    }
    return pcrs.contains(pathNodeKey);
  }

  /**
   * Add {@code nodeKey} to {@code value}'s posting list.
   *
   * <p>
   * A HOT slot write is an OR-merge of the incoming bitmap into the stored one
   * ({@code HOTLeafPage#mergeWithNodeRefs}), so adding one reference needs neither a read-back of the
   * stored references nor a re-insert of them. Doing so made building an index quadratic in the
   * number of nodes sharing a value: the n-th occurrence of a value range-scanned that value's chunks
   * and then re-inserted all n-1 node keys already stored, each through a full trie descent — on a
   * corpus where a value repeats k times, k(k+1)/2 slot writes instead of k.
   * </p>
   */
  private void indexNode(final long nodeKey, final CASValue value) throws SirixIOException {
    if (bulkLoader != null) {
      bulkLoader.add(value, nodeKey);
    } else {
      indexWriter.indexNodeKey(value, nodeKey);
    }
  }

  /**
   * Materialise everything the traversal collected. Must be called exactly once, after the document
   * traversal that feeds {@link #process} has finished; a no-op unless this builder is bulk-loading.
   */
  public void finish() {
    if (bulkLoader != null) {
      bulkLoader.flush();
    }
  }

}
