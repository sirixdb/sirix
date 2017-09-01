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
	protected Page delegate() {
		return null;
	}

}
