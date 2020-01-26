package org.sirix.access.trx.page;

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.page.NamePage;
import org.sirix.page.RevisionRootPage;

public final class RevisionRootPageReader {
  public RevisionRootPage loadRevisionRootPage(PageReadOnlyTrx rtx, int revisionNumber) {
    return rtx.loadRevRoot(revisionNumber);
  }

  public NamePage getNamePage(PageReadOnlyTrx rtx, RevisionRootPage revisionRootPage) {
    return rtx.getNamePage(revisionRootPage);
  }
}
