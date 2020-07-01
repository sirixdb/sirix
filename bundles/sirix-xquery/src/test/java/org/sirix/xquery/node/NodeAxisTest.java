package org.sirix.xquery.node;

import org.brackit.xquery.node.AxisTest;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.node.NodeStore;
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
