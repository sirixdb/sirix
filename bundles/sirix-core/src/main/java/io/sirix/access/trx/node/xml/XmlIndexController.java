package io.sirix.access.trx.node.xml;

import io.sirix.access.trx.node.AbstractIndexController;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.index.IndexBuilder;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.cas.xml.XmlCASIndexImpl;
import io.sirix.index.name.xml.XmlNameIndexImpl;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.xml.XmlPCRCollector;
import io.sirix.index.path.xml.XmlPathIndexImpl;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;

import java.util.HashSet;
import java.util.Set;

/**
 * Index controller, used to control the handling of indexes.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlIndexController extends AbstractIndexController<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /**
   * Constructor.
   */
  public XmlIndexController() {
    super(new Indexes(), new HashSet<>(), new XmlPathIndexImpl(), new XmlCASIndexImpl(), new XmlNameIndexImpl(), null);
  }

  @Override
  protected Path<QNm> parsePath(String path) {
    return Path.parse(path, PathParser.Type.XML);
  }

  @Override
  public XmlIndexController createIndexes(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    // Validate before createIndexBuilders catalogues any definition or the shared traversal writes a
    // single index page. Unsupported lifecycle mixes therefore fail atomically at the API boundary.
    validateNewIndexDefinitions(indexDefs, nodeWriteTrx);
    validateProjectionDefinitions(indexDefs, nodeWriteTrx);

    // Build the indexes.
    IndexBuilder.build(nodeWriteTrx, createIndexBuilders(indexDefs, nodeWriteTrx));

    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isProjectionIndex()) {
        createProjectionIndex(indexDef, nodeWriteTrx);
      }
    }

    // Create index listeners for upcoming changes.
    createIndexListeners(indexDefs, nodeWriteTrx);

    return this;
  }

  public XmlIndexController createProjectionIndexesAtLoadStart(final Set<IndexDef> indexDefs,
      final XmlNodeTrx nodeWriteTrx, final long expectedRows) {
    armProjectionIndexesAtLoadStart(indexDefs, nodeWriteTrx, expectedRows);
    return this;
  }

  public ProjectionBulkLoad createProjectionIndexAtLoadStart(final IndexDef indexDef, final XmlNodeTrx nodeWriteTrx,
      final long expectedRows) {
    return armProjectionIndexesAtLoadStart(Set.of(indexDef), nodeWriteTrx, expectedRows)[0];
  }

  private ProjectionBulkLoad[] armProjectionIndexesAtLoadStart(final Set<IndexDef> indexDefs,
      final XmlNodeTrx nodeWriteTrx, final long expectedRows) {
    final String resourceKey = nodeWriteTrx.getResourceSession().getResourceConfig().getResource().toString();
    for (final IndexDef indexDef : indexDefs) {
      if (!indexDef.isProjectionIndex()) {
        throw new IllegalArgumentException(
            "createProjectionIndexesAtLoadStart accepts PROJECTION definitions only; got " + indexDef.getType());
      }
      final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
      if (!pathSummary.getPCRsForPaths(Set.of(indexDef.getProjectionRootPath())).isEmpty()) {
        throw new IllegalStateException("Projection root path '" + indexDef.getProjectionRootPath()
            + "' already has records; load-time projection construction requires an empty record set");
      }
    }
    validateNewIndexDefinitions(indexDefs, nodeWriteTrx);
    nodeWriteTrx.awaitPendingAsyncCommit();
    final ProjectionBulkLoad[] ownedLoads = new ProjectionBulkLoad[indexDefs.size()];
    final IndexDef[] publishedDefs = new IndexDef[indexDefs.size()];
    int ownedCount = 0;
    int publishedCount = 0;
    try {
      for (final IndexDef indexDef : indexDefs) {
        if (indexes.getIndexDef(indexDef.getID(), indexDef.getType()) == null) {
          indexes.add(indexDef);
          publishedDefs[publishedCount] = indexDef;
          publishedCount++;
        }
        final ProjectionBulkLoad ownedLoad = ProjectionBulkLoad.begin(indexDef, resourceKey, nodeWriteTrx,
            nodeWriteTrx.getPathSummary(), nodeWriteTrx.getStorageEngineWriter(), expectedRows);
        ownedLoads[ownedCount] = ownedLoad;
        ownedCount++;
      }
      createIndexListeners(indexDefs, nodeWriteTrx);
      return ownedLoads;
    } catch (final Throwable armFailure) {
      for (int index = ownedCount - 1; index >= 0; index--) {
        try {
          ownedLoads[index].abort();
        } catch (final Throwable cleanupFailure) {
          if (cleanupFailure != armFailure) {
            armFailure.addSuppressed(cleanupFailure);
          }
        }
      }
      for (int index = publishedCount - 1; index >= 0; index--) {
        try {
          indexes.removeIndex(publishedDefs[index]);
        } catch (final Throwable cleanupFailure) {
          if (cleanupFailure != armFailure) {
            armFailure.addSuppressed(cleanupFailure);
          }
        }
      }
      throw XmlIndexController.<RuntimeException>rethrowUnchecked(armFailure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T rethrowUnchecked(final Throwable failure) throws T {
    throw (T) failure;
  }

  private void createProjectionIndex(final IndexDef indexDef, final XmlNodeTrx nodeWriteTrx) {
    final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
    nodeWriteTrx.awaitPendingAsyncCommit();
    ProjectionIndexBuilder.buildAndPersist(indexDef, pathSummary, nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(),
        false);
  }

  @Override
  protected ChangeListener createProjectionIndexListener(final XmlNodeTrx nodeWriteTrx, final IndexDef indexDef) {
    final PathSummaryReader pathSummary = requireProjectionPathSummary(nodeWriteTrx, indexDef);
    return new ProjectionIndexChangeListener(nodeWriteTrx.getStorageEngineWriter(), pathSummary, indexDef,
        nodeWriteTrx);
  }

  /**
   * Validate projection prerequisites before createIndexBuilders can publish a definition or page.
   */
  private static void validateProjectionDefinitions(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isProjectionIndex()) {
        requireProjectionPathSummary(nodeWriteTrx, indexDef);
      }
    }
  }

  private static PathSummaryReader requireProjectionPathSummary(final XmlNodeTrx nodeWriteTrx,
      final IndexDef indexDef) {
    if (!nodeWriteTrx.getResourceSession().getResourceConfig().withPathSummary) {
      throw new IllegalStateException("Cannot bind incremental maintenance for projection index " + indexDef.getID()
          + ": the resource has no path summary. Projection indexes require buildPathSummary=true.");
    }
    final PathSummaryReader pathSummary = nodeWriteTrx.getPathSummary();
    if (pathSummary == null) {
      throw new IllegalStateException("Cannot bind incremental maintenance for projection index " + indexDef.getID()
          + ": the resource's path summary is unavailable.");
    }
    return pathSummary;
  }

  /**
   * Create index builders.
   *
   * @param indexDefs the {@link IndexDef}s
   * @param nodeWriteTrx the {@link XmlNodeTrx}
   * @return the created index builder instances
   */
  Set<XmlNodeVisitor> createIndexBuilders(final Set<IndexDef> indexDefs, final XmlNodeTrx nodeWriteTrx) {
    // Index builders for all index definitions.
    final var indexBuilders = new HashSet<XmlNodeVisitor>(indexDefs.size());
    for (final IndexDef indexDef : indexDefs) {
      indexes.add(indexDef);
      switch (indexDef.getType()) {
        case PATH:
          indexBuilders.add(
              createPathIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case CAS:
          indexBuilders.add(createCASIndexBuilder(nodeWriteTrx, nodeWriteTrx.getStorageEngineWriter(),
              nodeWriteTrx.getPathSummary(), indexDef));
          break;
        case NAME:
          indexBuilders.add(createNameIndexBuilder(nodeWriteTrx.getStorageEngineWriter(), indexDef));
          break;
        case PROJECTION:
          break;
        default:
          break;
      }
    }
    return indexBuilders;
  }

  @Override
  public PathFilter createPathFilter(final Set<String> stringPaths, final XmlNodeReadOnlyTrx rtx) {
    final Set<Path<QNm>> paths = new HashSet<>(stringPaths.size());
    for (final String path : stringPaths) {
      paths.add(Path.parse(path, PathParser.Type.XML));
    }
    return new PathFilter(paths, new XmlPCRCollector(rtx));
  }

  private XmlNodeVisitor createPathIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return (XmlNodeVisitor) pathIndex.createBuilder(storageEngineWriter, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createCASIndexBuilder(final XmlNodeReadOnlyTrx nodeReadTrx,
      final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return (XmlNodeVisitor) casIndex.createBuilder(nodeReadTrx, storageEngineWriter, pathSummaryReader, indexDef);
  }

  private XmlNodeVisitor createNameIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final IndexDef indexDef) {
    return (XmlNodeVisitor) nameIndex.createBuilder(storageEngineWriter, indexDef);
  }
}
