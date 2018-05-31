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

import static org.testng.AssertJUnit.assertEquals;
import java.io.IOException;
import java.nio.file.Files;
import org.sirix.TestHelper;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.file.FileStorage;
import org.sirix.io.ram.RAMStorage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Storage test.
 */
public final class StorageTest {

  /** {@link ResourceConfiguration} reference. */
  private ResourceConfiguration mResourceConfig;

  @BeforeClass
  public void setUp() throws SirixException, IOException {
    TestHelper.closeEverything();
    TestHelper.deleteEverything();
    Files.createDirectories(TestHelper.PATHS.PATH1.getFile());
    Files.createDirectories(
        TestHelper.PATHS.PATH1.getFile()
                              .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()));
    Files.createFile(
        TestHelper.PATHS.PATH1.getFile()
                              .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                              .resolve("data.sirix"));
    mResourceConfig = new ResourceConfiguration.Builder("shredded",
        new DatabaseConfiguration(TestHelper.PATHS.PATH1.getFile())).build();
  }

  @AfterClass
  public void tearDown() throws SirixException {
    TestHelper.closeEverything();
    TestHelper.deleteEverything();
  }

  /**
   * Test method for {@link org.ByteHandler.io.bytepipe.IByteHandler#deserialize(byte[])} and for
   * {@link org.ByteHandler.io.bytepipe.IByteHandler#serialize(byte[])}.
   *
   * @throws SirixIOException
   */
  @Test(dataProvider = "instantiateStorages")
  public void testFirstRef(final Class<Storage> clazz, final Storage[] storages)
      throws SirixException {
    for (final Storage handler : storages) {
      try {
        final PageReference pageRef1 = new PageReference();
        final UberPage page1 = new UberPage();
        pageRef1.setPage(page1);

        // same instance check
        final PageReference pageRef2;
        try (final Writer writer = handler.createWriter()) {
          pageRef2 = writer.writeUberPageReference(pageRef1).readUberPageReference();
          assertEquals(
              new StringBuilder("Check for ").append(handler.getClass())
                                             .append(" failed.")
                                             .toString(),
              ((UberPage) pageRef1.getPage()).getRevisionCount(),
              ((UberPage) pageRef2.getPage()).getRevisionCount());
        }

        // new instance check
        try (final Reader reader = handler.createReader()) {
          final PageReference pageRef3 = reader.readUberPageReference();
          assertEquals(
              new StringBuilder("Check for ").append(handler.getClass())
                                             .append(" failed.")
                                             .toString(),
              pageRef2.getKey(), pageRef3.getKey());
          assertEquals(
              new StringBuilder("Check for ").append(handler.getClass())
                                             .append(" failed.")
                                             .toString(),
              ((UberPage) pageRef2.getPage()).getRevisionCount(),
              ((UberPage) pageRef3.getPage()).getRevisionCount());
        }
      } finally {
        handler.close();
      }
    }
  }

  /**
   * Providing different implementations of the {@link ByteHandler} as Dataprovider to the test
   * class.
   *
   * @return different classes of the {@link ByteHandler}
   * @throws SirixIOException if an I/O error occurs
   */
  @DataProvider(name = "instantiateStorages")
  public Object[][] instantiateStorages() throws SirixIOException {
    Object[][] returnVal = {{Storage.class,
        new Storage[] {new FileStorage(mResourceConfig), new RAMStorage(mResourceConfig)}}};
    return returnVal;
  }

}
