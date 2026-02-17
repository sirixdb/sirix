package io.sirix.access.conf;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixIOException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test case for de-/serialization of {@link DatabaseConfiguration}s.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class DatabaseConfigurationTest {

  @BeforeMethod
  public void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @AfterMethod
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
