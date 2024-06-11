/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.io.filechannel.FileChannelStorage;
import io.sirix.io.memorymapped.MMStorage;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import net.openhft.chronicle.bytes.Bytes;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.file.FileStorage;
import io.sirix.io.ram.RAMStorage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.testng.AssertJUnit.*;

/**
 * Storage test.
 */
public final class StorageTest {

  /**
   * {@link ResourceConfiguration} reference.
   */
  private ResourceConfiguration resourceConfig;

  @BeforeClass
  public void setUp() throws SirixException, IOException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    Files.createDirectories(XmlTestHelper.PATHS.PATH1.getFile());
    Files.createDirectories(XmlTestHelper.PATHS.PATH1.getFile()
                                                     .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()));
    Files.createFile(XmlTestHelper.PATHS.PATH1.getFile()
                                              .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                              .resolve("data.sirix"));
    resourceConfig = new ResourceConfiguration.Builder("shredded").build();
  }

  @AfterClass
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @Test(dataProvider = "instantiateStorages")
  public void testExists(final Class<IOStorage> clazz, final IOStorage[] storages) throws SirixException {
    for (final IOStorage handler : storages) {
      assertFalse("empty storage should not return true on exists", handler.exists());

      final Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer();

      try (final Writer writer = handler.createWriter()) {
        var ref = new PageReference();
        var uberPage = new UberPage();
        writer.writeUberPageReference(null, ref, uberPage, bytes);
      }

      assertTrue("writing a single page should mark the Storage as existing", handler.exists());
      try (final Writer writer = handler.createWriter()) {
        writer.truncate();
      }

      assertFalse("truncating the file to length 0 should mark the Storage as non-existing", handler.exists());
    }
  }

  @Test(dataProvider = "instantiateStorages")
  public void testFirstRef(final Class<IOStorage> clazz, final IOStorage[] storages) throws SirixException {
    for (final IOStorage handler : storages) {
      try {
        final PageReference pageRef1 = new PageReference();
        final UberPage page1 = new UberPage();
        pageRef1.setPage(page1);

        final Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer();

        // same instance check
        final PageReference pageRef2;
        try (final Writer writer = handler.createWriter()) {
          pageRef2 = writer.writeUberPageReference(null, pageRef1, page1, bytes).readUberPageReference();
          assertEquals("Check for " + handler.getClass() + " failed.",
                       ((UberPage) pageRef1.getPage()).getRevisionCount(),
                       ((UberPage) pageRef2.getPage()).getRevisionCount());
        }

        // new instance check
        try (final Reader reader = handler.createReader()) {
          final PageReference pageRef3 = reader.readUberPageReference();
          assertEquals("Check for " + handler.getClass() + " failed.", pageRef2.getKey(), pageRef3.getKey());
          assertEquals("Check for " + handler.getClass() + " failed.",
                       ((UberPage) pageRef2.getPage()).getRevisionCount(),
                       ((UberPage) pageRef3.getPage()).getRevisionCount());
        }
      } finally {
        handler.close();
      }
    }
  }

  /**
   * Providing different implementations of the {@link ByteHandler} as Dataprovider to the test class.
   *
   * @return different classes of the {@link ByteHandler}
   */
  @DataProvider(name = "instantiateStorages")
  public Object[][] instantiateStorages() {
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile());
    return new Object[][] { { IOStorage.class, new IOStorage[] {
        new FileChannelStorage(resourceConfig.setDatabaseConfiguration(dbConfig), Caffeine.newBuilder().buildAsync()),
        new FileStorage(resourceConfig.setDatabaseConfiguration(dbConfig), Caffeine.newBuilder().buildAsync()),
        new MMStorage(resourceConfig.setDatabaseConfiguration(dbConfig), Caffeine.newBuilder().buildAsync()),
        //     new IOUringStorage(resourceConfig.setDatabaseConfiguration(dbConfig), Caffeine.newBuilder().buildAsync()),
        new RAMStorage(resourceConfig.setDatabaseConfiguration(dbConfig)), } } };
  }

}
