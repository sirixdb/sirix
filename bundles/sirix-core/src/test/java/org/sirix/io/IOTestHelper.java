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

package org.sirix.io;

import static org.junit.Assert.assertEquals;
import org.sirix.XmlTestHelper;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Helper class for testing the I/O interfaces.
 *
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class IOTestHelper {

  /** Private constructor. */
  private IOTestHelper() {}

  /**
   * Static method to get {@link ResourceConfiguration}
   *
   * @param type for the the {@link ResourceConfiguration} should be generated
   * @return a suitable {@link ResourceConfiguration}
   * @throws SirixUsageException
   */
  public static ResourceConfiguration registerIO(final StorageType type) throws SirixException {
    final ResourceConfiguration.Builder resourceConfig = new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE);
    resourceConfig.storageType(type);
    return resourceConfig.build();
  }

  /**
   * Tear down for all tests related to the io layer.
   */
  public static void clean() throws SirixException {
    XmlTestHelper.deleteEverything();
  }

  /**
   * Test reading/writing the first reference.
   *
   * @param resourceConf {@link ResourceConfiguration} reference
   * @throws SirixException if something went wrong
   */
  public static void testReadWriteFirstRef(final ResourceConfiguration resourceConf) throws SirixException {
    final IOStorage fac = StorageType.getStorage(resourceConf);
    final PageReference pageRef1 = new PageReference();
    final UberPage page1 = new UberPage();
    pageRef1.setPage(page1);

    // same instance check
    final Writer writer = fac.createWriter();
    writer.writeUberPageReference(pageRef1);
    final PageReference pageRef2 = writer.readUberPageReference();
    assertEquals(((UberPage) pageRef1.getPage()).getRevisionCount(),
        ((UberPage) pageRef2.getPage()).getRevisionCount());
    writer.close();

    // new instance check
    final Reader reader = fac.createReader();
    final PageReference pageRef3 = reader.readUberPageReference();
    assertEquals(((UberPage) pageRef1.getPage()).getRevisionCount(),
        ((UberPage) pageRef3.getPage()).getRevisionCount());
    reader.close();
    fac.close();
  }

}
