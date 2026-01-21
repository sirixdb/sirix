package io.sirix.query.node;

import io.sirix.access.Databases;
import io.brackit.query.jdm.node.NodeStore;
import io.brackit.query.node.AxisTest;
import org.junit.After;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Node axis test.
 *
 * @author Johannes Lichtenberger
 *
 */
public class NodeAxisTest extends AxisTest {

  private Path xmlTestDir;

  @Override
  protected NodeStore createStore() {
    try {
      xmlTestDir = Files.createTempDirectory("sirix-xml-axis-test");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return BasicXmlDBStore.newBuilder().location(xmlTestDir).build();
  }

  @After
  public void tearDown() {
    ((BasicXmlDBStore) store).close();
    if (xmlTestDir != null) {
      Databases.removeDatabase(xmlTestDir);
    }
  }
}
