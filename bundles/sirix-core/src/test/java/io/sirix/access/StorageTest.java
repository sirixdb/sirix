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
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.ram.RAMStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage test.
 *
 * <p>Converted from TestNG to JUnit 5 (2026-06-11): TestNG classes never ran under the gradle
 * {@code useJUnitPlatform()} config, which is how a real NPE here went unnoticed. Method order is
 * pinned to method names because {@code testFirstRef} leaves the storage file non-empty, which
 * {@code testExists} (alphabetically first, matching the previous TestNG order) must not see.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
public final class StorageTest {

  /**
   * {@link ResourceConfiguration} reference.
   */
  private ResourceConfiguration resourceConfig;

  @BeforeAll
  public void setUp() throws SirixException, IOException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
    Files.createDirectories(XmlTestHelper.PATHS.PATH1.getFile());
    Files.createDirectories(
        XmlTestHelper.PATHS.PATH1.getFile().resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()));
    Files.createFile(XmlTestHelper.PATHS.PATH1.getFile()
                                              .resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                              .resolve("data.sirix"));
    resourceConfig = new ResourceConfiguration.Builder("shredded").build();
  }

  @AfterAll
  public void tearDown() throws SirixException {
    XmlTestHelper.closeEverything();
    XmlTestHelper.deleteEverything();
  }

  @ParameterizedTest
  @MethodSource("instantiateStorages")
  public void testExists(final Class<IOStorage> clazz, final IOStorage[] storages) throws SirixException {
    for (final IOStorage handler : storages) {
      assertFalse(handler.exists(), "empty storage should not return true on exists");

      final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

      try (final Writer writer = handler.createWriter()) {
        var ref = new PageReference();
        var uberPage = new UberPage();
        // The writer serializes through the resource's byte-handle pipeline — a null config
        // NPEs since the beacon slots are written via the regular page-write path.
        writer.writeUberPageReference(resourceConfig, ref, uberPage, bytes);
      }

      assertTrue(handler.exists(), "writing a single page should mark the Storage as existing");
      try (final Writer writer = handler.createWriter()) {
        writer.truncate();
      }

      assertFalse(handler.exists(), "truncating the file to length 0 should mark the Storage as non-existing");
    }
  }

  @ParameterizedTest
  @MethodSource("instantiateStorages")
  public void testFirstRef(final Class<IOStorage> clazz, final IOStorage[] storages) throws SirixException {
    for (final IOStorage handler : storages) {
      try {
        final PageReference pageRef1 = new PageReference();
        final UberPage page1 = new UberPage();
        pageRef1.setPage(page1);

        final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

        // same instance check
        final PageReference pageRef2;
        try (final Writer writer = handler.createWriter()) {
          pageRef2 = writer.writeUberPageReference(resourceConfig, pageRef1, page1, bytes).readUberPageReference();
          assertEquals(((UberPage) pageRef1.getPage()).getRevisionCount(),
              ((UberPage) pageRef2.getPage()).getRevisionCount(), "Check for " + handler.getClass() + " failed.");
        }

        // new instance check
        try (final Reader reader = handler.createReader()) {
          final PageReference pageRef3 = reader.readUberPageReference();
          assertEquals(pageRef2.getKey(), pageRef3.getKey(), "Check for " + handler.getClass() + " failed.");
          assertEquals(((UberPage) pageRef2.getPage()).getRevisionCount(),
              ((UberPage) pageRef3.getPage()).getRevisionCount(), "Check for " + handler.getClass() + " failed.");
        }
      } finally {
        handler.close();
      }
    }
  }

  /**
   * Providing different implementations of the {@link ByteHandler} as method source to the test class.
   *
   * @return different classes of the {@link ByteHandler}
   */
  public Stream<Arguments> instantiateStorages() {
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile());
    return Stream.of(Arguments.of(IOStorage.class,
        new IOStorage[] {
            new FileChannelStorage(resourceConfig.setDatabaseConfiguration(dbConfig),
                Caffeine.newBuilder().buildAsync()),
            new MMStorage(resourceConfig.setDatabaseConfiguration(dbConfig), Caffeine.newBuilder().buildAsync()),
            // new IOUringStorage(resourceConfig.setDatabaseConfiguration(dbConfig),
            // Caffeine.newBuilder().buildAsync()),
            new RAMStorage(resourceConfig.setDatabaseConfiguration(dbConfig)),}));
  }

}
