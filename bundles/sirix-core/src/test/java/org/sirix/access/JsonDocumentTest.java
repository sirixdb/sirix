package org.sirix.access;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.exception.SirixException;

public final class JsonDocumentTest {
  @Before
  public void setUp() throws SirixException {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() throws SirixException {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void testJsonDocument() {
    // JsonTestHelper.createTestDocument();
  }
}
