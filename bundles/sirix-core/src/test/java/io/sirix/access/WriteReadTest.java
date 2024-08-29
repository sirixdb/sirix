package io.sirix.access;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.service.xml.serialize.XmlSerializerTest;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public final class WriteReadTest {
  private Holder helper;

  @BeforeEach
  public void setUp() {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    helper = Holder.generateWtx();
    var wtx = helper.getXmlNodeTrx();
    XmlDocumentCreator.create(wtx);
  }

  @AfterEach
  public void tearDown() {
    helper.close();
    XmlTestHelper.closeEverything();
  }

  // Test methods
  public void test() {
    var rtx = helper.getXmlNodeReadTrx();
  }
}
