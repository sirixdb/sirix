package io.sirix.cache;

import io.sirix.index.name.Names;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.node.interfaces.Node;
import io.sirix.page.interfaces.Page;

public interface BufferManager extends AutoCloseable {
	Cache<PageReference, KeyValueLeafPage> getRecordPageCache();

	Cache<PageReference, Page> getPageCache();

	Cache<Integer, RevisionRootPage> getRevisionRootPageCache();

	Cache<RBIndexKey, Node> getIndexCache();

	Cache<NamesCacheKey, Names> getNamesCache();

	Cache<Integer, PathSummaryData> getPathSummaryCache();

	void clearAllCaches();
}
