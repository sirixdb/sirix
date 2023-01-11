package org.sirix.cache;

import org.sirix.index.name.Names;
import org.sirix.index.redblacktree.RBNode;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.interfaces.Page;

public interface BufferManager extends AutoCloseable {
  Cache<PageReference, Page> getRecordPageCache();

  Cache<PageReference, Page> getPageCache();

  Cache<Integer, RevisionRootPage> getRevisionRootPageCache();

  Cache<RBIndexKey, RBNode<?, ?>> getIndexCache();

  Cache<NamesCacheKey, Names> getNamesCache();

  void clearAllCaches();
}
