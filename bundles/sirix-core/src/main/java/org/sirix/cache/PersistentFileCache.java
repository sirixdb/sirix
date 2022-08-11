package org.sirix.cache;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Writer;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

import static com.google.common.base.Preconditions.checkNotNull;

public final class PersistentFileCache implements AutoCloseable {
  /** Write to a persistent file. */
  private final Writer writer;

  public PersistentFileCache(final Writer writer) {
    this.writer = checkNotNull(writer);
  }

  public PageContainer get(final PageReadOnlyTrx pageReadTrx, PageReference reference) {
    checkNotNull(pageReadTrx);

    if (reference.getPersistentLogKey() < 0)
      return PageContainer.emptyInstance();

    final Page modifiedPage = writer.read(reference, pageReadTrx);
    final Page completePage;

    if (modifiedPage instanceof KeyValuePage) {
      final long peristKey = reference.getPersistentLogKey();
//      reference.setPersistentLogKey(peristKey + reference.getLength());
      completePage = writer.read(reference, pageReadTrx);
      reference.setPersistentLogKey(peristKey);
    } else {
      completePage = modifiedPage;
    }

    return PageContainer.getInstance(completePage, modifiedPage);
  }

  public PersistentFileCache put(final PageReadOnlyTrx pageReadTrx, PageReference reference, PageContainer container) {
    reference.setPage(container.getModified());
    writer.write(pageReadTrx, reference, null);

    if (container.getModified() instanceof KeyValuePage) {
      final long offset = reference.getPersistentLogKey();
//      int length = reference.getLength();
      reference.setPage(container.getComplete());
      writer.write(pageReadTrx, reference, null);
//      length += reference.getLength();
      reference.setPersistentLogKey(offset);
//      reference.setLength(length);
    }

    reference.setPage(null);

    return this;
  }

  public PersistentFileCache truncate() {
    writer.truncate();
    return this;
  }

  @Override
  public void close() {
    writer.close();
  }
}
