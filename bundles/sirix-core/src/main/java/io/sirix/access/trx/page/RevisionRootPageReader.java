package io.sirix.access.trx.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.page.NamePage;
import io.sirix.page.RevisionRootPage;

public final class RevisionRootPageReader {
  public RevisionRootPage loadRevisionRootPage(StorageEngineReader rtx, int revisionNumber) {
    return rtx.loadRevRoot(revisionNumber);
  }

  public NamePage getNamePage(StorageEngineReader rtx, RevisionRootPage revisionRootPage) {
    return rtx.getNamePage(revisionRootPage);
  }
}
