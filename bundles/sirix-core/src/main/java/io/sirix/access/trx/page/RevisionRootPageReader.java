package io.sirix.access.trx.page;

import io.sirix.api.PageReadOnlyTrx;
import io.sirix.page.NamePage;
import io.sirix.page.RevisionRootPage;

public final class RevisionRootPageReader {
	public RevisionRootPage loadRevisionRootPage(PageReadOnlyTrx rtx, int revisionNumber) {
		return rtx.loadRevRoot(revisionNumber);
	}

	public NamePage getNamePage(PageReadOnlyTrx rtx, RevisionRootPage revisionRootPage) {
		return rtx.getNamePage(revisionRootPage);
	}
}
