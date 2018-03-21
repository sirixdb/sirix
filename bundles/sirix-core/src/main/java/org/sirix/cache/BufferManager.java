package org.sirix.cache;

import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

public interface BufferManager {
  Cache<PageReference, PageContainer> getRecordPageCache();

  Cache<PageReference, Page> getPageCache();
}
