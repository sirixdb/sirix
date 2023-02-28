package org.sirix.xquery.node;

import org.brackit.xquery.jdm.node.NodeStore;
import org.brackit.xquery.node.AxisTest;
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
