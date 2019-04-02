package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Writer;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

public final class PersistentFileCache implements AutoCloseable {
  /** Write to a persistent file. */
  private final Writer mWriter;

  public PersistentFileCache(final Writer writer) {
    mWriter = checkNotNull(writer);
  }

  public PageContainer get(PageReference reference, final PageReadOnlyTrx pageReadTrx) {
    checkNotNull(pageReadTrx);

    if (reference.getPersistentLogKey() < 0)
      return PageContainer.emptyInstance();

    final Page modifiedPage = mWriter.read(reference, pageReadTrx);
    final Page completePage;

    if (modifiedPage instanceof KeyValuePage) {
      final long peristKey = reference.getPersistentLogKey();
      reference.setPersistentLogKey(peristKey + reference.getLength());
      completePage = mWriter.read(reference, pageReadTrx);
      reference.setPersistentLogKey(peristKey);
    } else {
      completePage = modifiedPage;
    }

    return PageContainer.getInstance(completePage, modifiedPage);
  }

  public PersistentFileCache put(PageReference reference, PageContainer container) {
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

    return this;
  }

  public PersistentFileCache truncate() {
    mWriter.truncate();
    return this;
  }

  @Override
  public void close() {
    mWriter.close();
  }
}
