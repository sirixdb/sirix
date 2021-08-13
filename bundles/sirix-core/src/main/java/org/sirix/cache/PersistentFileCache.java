package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.io.Writer;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

public final class PersistentFileCache implements AutoCloseable {
  /** Write to a persistent file. */
  private final Writer writer;

  public PersistentFileCache(final Writer writer) {
    this.writer = checkNotNull(writer);
  }

  public PageContainer get(PageReference reference, final PageReadOnlyTrx pageReadTrx) {
    checkNotNull(pageReadTrx);

    if (reference.getPersistentLogKey() < 0)
      return null;

    final Page modifiedPage = writer.read(reference, pageReadTrx);
    final Page completePage;

    if (modifiedPage instanceof KeyValuePage) {
      final long peristKey = reference.getPersistentLogKey();
      completePage = writer.read(reference, pageReadTrx);
      reference.setPersistentLogKey(peristKey);
    } else {
      completePage = modifiedPage;
    }

    return PageContainer.getInstance(completePage, modifiedPage);
  }

  public PersistentFileCache put(PageReference reference, PageContainer container) {
    reference.setPage(container.getModified());
    writer.write(reference);

    if (container.getModified() instanceof KeyValuePage) {
      final long offset = reference.getPersistentLogKey();
//      int length = reference.getLength();
      reference.setPage(container.getComplete());
      writer.write(reference);
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
