package org.sirix.xquery.node;

import org.brackit.xquery.node.AxisTest;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Store;
import org.junit.After;

/**
 * Node axis test.
 * 
 * @author Johannes Lichtenberger
 *
 */
public class NodeAxisTest extends AxisTest {
	@Override
	protected Store createStore() throws Exception {
		return new DBStore();
	}
	
	@After
	public void tearDown() throws DocumentException {
		((DBStore) store).close();
	}
}
