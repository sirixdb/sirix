package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

public interface BufferManager {
	Cache<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> getRecordPageCache();

	Cache<PageReference, Page> getPageCache();
}
