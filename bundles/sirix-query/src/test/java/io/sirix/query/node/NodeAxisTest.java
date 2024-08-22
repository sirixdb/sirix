package io.sirix.query.node;

import io.brackit.query.jdm.node.NodeStore;
import io.brackit.query.node.AxisTest;
import org.junit.After;

/**
 * Node axis test.
 *
 * @author Johannes Lichtenberger
 *
 */
public class NodeAxisTest extends AxisTest {
	@Override
	protected NodeStore createStore() {
		return BasicXmlDBStore.newBuilder().build();
	}

	@After
	public void tearDown() {
		((BasicXmlDBStore) store).close();
	}
}
