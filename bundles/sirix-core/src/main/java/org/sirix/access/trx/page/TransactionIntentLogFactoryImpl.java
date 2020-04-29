/**
 * Copyright (c) 2018, Sirix
 * <p>
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the <organization> nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.access.trx.page;

import org.sirix.access.ResourceConfiguration;
import org.sirix.cache.PersistentFileCache;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.file.FileWriter;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 *
 */
final class TransactionIntentLogFactoryImpl implements TransactionIntentLogFactory {

  /**
   * Package private constructor.
   */
  public TransactionIntentLogFactoryImpl() {
  }

  @Override
  public TransactionIntentLog createTrxIntentLog(final ResourceConfiguration resourceConfig) {
    final Path logFile = resourceConfig.getResource()
                                       .resolve(ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath())
                                       .resolve("intent-log");

    try {
      if (Files.exists(logFile)) {
        Files.delete(logFile);
        Files.createFile(logFile);
      }

      final RandomAccessFile file = new RandomAccessFile(logFile.toFile(), "rw");

      final FileWriter fileWriter = new FileWriter(file, null,
          new ByteHandlePipeline(resourceConfig.byteHandlePipeline), SerializationType.TRANSACTION_INTENT_LOG,
          new PagePersister());

      final PersistentFileCache persistentFileCache = new PersistentFileCache(fileWriter);

      return new TransactionIntentLog(persistentFileCache, 1 << 11);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
