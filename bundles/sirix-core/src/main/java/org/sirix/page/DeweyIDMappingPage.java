package org.sirix.page;

import org.sirix.page.interfaces.Page;

/**
 * Page which is a secondary index to map
 * 
 * @author Johannes Lichtenberger
 *
 */
public class DeweyIDMappingPage extends AbstractForwardingPage {

	@Override
	public Page setDirty(boolean pDirty) {
		return null;
	}

	@Override
	protected Page delegate() {
		return null;
	}

}
