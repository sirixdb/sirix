package io.sirix.access.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixIOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test case for de-/serialization of {@link DatabaseConfiguration}s.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DatabaseConfigurationTest {

  @BeforeEach
  public void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    XmlTestHelper.deleteEverything();
  }

  /**
   * Test method for {@link DatabaseConfiguration#serialize(DatabaseConfiguration)} and
   * {@link DatabaseConfiguration#deserialize(java.nio.file.Path)} .
   * 
   * @throws SirixIOException if an I/O exception occurs
   */
  @Test
  public void testDeSerialize() {
    DatabaseConfiguration conf = new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile());
    assertTrue(Databases.createXmlDatabase(conf));
    DatabaseConfiguration serializedConf = DatabaseConfiguration.deserialize(XmlTestHelper.PATHS.PATH1.getFile());
    assertEquals(conf.toString(), serializedConf.toString());
  }
}
