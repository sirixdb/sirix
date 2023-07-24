package org.sirix.cache;

import org.sirix.index.name.Names;
import org.sirix.node.interfaces.Node;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public interface BufferManager extends AutoCloseable {
  Cache<PageReference, Page> getRecordPageCache();

  Cache<PageReference, Page> getPageCache();

  Cache<Integer, RevisionRootPage> getRevisionRootPageCache();

  Cache<RBIndexKey, Node> getIndexCache();

  Cache<NamesCacheKey, Names> getNamesCache();

  Cache<Integer, PathSummaryData> getPathSummaryCache();

  void clearAllCaches();
}
