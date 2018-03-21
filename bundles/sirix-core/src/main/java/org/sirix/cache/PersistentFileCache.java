package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Map;
import org.sirix.api.PageReadTrx;
import org.sirix.io.Writer;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import com.google.common.collect.ImmutableMap;

public final class PersistentFileCache implements Cache<PageReference, PageContainer> {
  /** Write to a persistent file. */
  private final Writer mWriter;

  /** The page read transaction. */
  private PageReadTrx mPageReadTrx;

  public PersistentFileCache(final Writer writer, final PageReadTrx pageReadTrx) {
    mWriter = checkNotNull(writer);
    mPageReadTrx = checkNotNull(pageReadTrx);
  }

  @Override
  public void clear() {}

  @Override
  public PageContainer get(PageReference reference) {
    if (reference.getPersistentLogKey() < 0)
      return PageContainer.emptyInstance();

    final Page modifiedPage = mWriter.read(reference, mPageReadTrx);
    final Page completePage;

    if (modifiedPage instanceof KeyValuePage) {
      final long peristKey = reference.getPersistentLogKey();
      reference.setPersistentLogKey(peristKey + reference.getLength());
      completePage = mWriter.read(reference, mPageReadTrx);
      reference.setPersistentLogKey(peristKey);
    } else {
      completePage = modifiedPage;
    }

    return new PageContainer(completePage, modifiedPage);
  }

  @Override
  public void put(PageReference reference, PageContainer container) {
    reference.setPage(container.getModified());
    mWriter.write(reference);

    if (container.getModified() instanceof KeyValuePage) {
      final long offset = reference.getPersistentLogKey();
      int length = reference.getLength();
      reference.setPage(container.getComplete());
      mWriter.write(reference);
      length += reference.getLength();
      reference.setPersistentLogKey(offset);
      reference.setLength(length);
    }
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends PageContainer> map) {
    map.forEach((key, value) -> {
      put(key, value);
    });
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException("No secondary cache available.");
  }

  @Override
  public ImmutableMap<PageReference, PageContainer> getAll(Iterable<? extends PageReference> keys) {
    throw new UnsupportedOperationException("Can not read all entries.");
  }

  @Override
  public void remove(PageReference key) {}

  @Override
  public void close() {
    mWriter.close();
  }
}
