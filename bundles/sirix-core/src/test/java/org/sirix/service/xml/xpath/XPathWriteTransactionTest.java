/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.xpath;

import static org.junit.Assert.fail;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.shredder.XMLShredder;

/**
 * Testcase for working with XPath and WriteTransactions
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class XPathWriteTransactionTest {

  private static final Path XML =
      Paths.get("src", "test", "resources", "enwiki-revisions-test.xml");

  private static final String RESOURCE = "bla";

  private ResourceManager manager;

  private XdmNodeWriteTrx wtx;

  private Database database;

  @Before
  public void setUp() throws Exception {
    TestHelper.deleteEverything();
    // Build simple test tree.
    XMLShredder.main(
        XML.toAbsolutePath().toString(), PATHS.PATH1.getFile().toAbsolutePath().toString());

    // Verify.
    database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    database.createResource(
        new ResourceConfiguration.Builder(RESOURCE, PATHS.PATH1.getConfig()).build());
    manager = database.getResourceManager(TestHelper.RESOURCE);
    wtx = manager.beginNodeWriteTrx();
  }

  @Test
  public void test() throws SirixXPathException {
    wtx.moveToDocumentRoot();
    // final XPathAxis xpa =
    // new XPathAxis(wtx, "//revision[./parent::page/title/text() = '"
    // + "AmericanSamoa"
    // + "']");
    final XPathAxis xpa = new XPathAxis(wtx, "//revision");
    if (!xpa.hasNext()) {
      fail();
    }

  }

  @After
  public void tearDown() throws SirixException {
    // wtx.abort();
    wtx.close();
    manager.close();
    TestHelper.closeEverything();
  }

}
