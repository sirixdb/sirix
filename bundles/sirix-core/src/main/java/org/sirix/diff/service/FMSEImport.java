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
package org.sirix.diff.service;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import javax.xml.stream.XMLStreamException;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

/**
 * Import using the FMSE algorithm.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class FMSEImport {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(FMSEImport.class));

  /**
   * Shredder new revision as temporal resource.
   *
   * @param resNewRev {@link File} reference for new revision (XML resource)
   * @param newRev {@link File} reference for shreddered new revision (sirix resource)
   * @return XdmNodeWriteTrx the wr
   * @throws SirixException if sirix fails to shredder the file
   * @throws IOException if file couldn't be read
   * @throws XMLStreamException if XML document isn't well formed
   * @throws NullPointerException if {@code resNewRev} or {@code newRev} is {@code null}
   */
  public static void shredder(final Path resNewRev, @Nonnull final Path newRev)
      throws SirixException, IOException, XMLStreamException {
    assert resNewRev != null;
    assert newRev != null;
    final var conf = new DatabaseConfiguration(newRev);
    Databases.removeDatabase(newRev);
    Databases.createXdmDatabase(conf);

    try (final var db = Databases.openXdmDatabase(newRev)) {
      db.createResource(new ResourceConfiguration.Builder("shredded", conf).build());
      try (final var resMgr = db.getResourceManager("shredded");
          final var wtx = resMgr.beginNodeTrx();
          final var fis = new FileInputStream(resNewRev.toFile())) {
        final var fileReader = XMLShredder.createFileReader(fis);
        final var shredder = new XMLShredder.Builder(wtx, fileReader, Insert.ASFIRSTCHILD).commitAfterwards().build();
        shredder.call();
      }
    }
  }

  /**
   * Import the data.
   *
   * @param resOldRev {@link File} for old revision (sirix resource)
   * @param resNewRev {@link File} for new revision (XML resource)
   */
  private static void xmlDataImport(final Path resOldRev, @Nonnull final Path resNewRev) {
    try {
      final var newRevTarget = Files.createTempDirectory(resNewRev.getFileName().toString());
      if (Files.exists(newRevTarget)) {
        SirixFiles.recursiveRemove(newRevTarget);
      }
      shredder(checkNotNull(resNewRev), newRevTarget);

      try (final var databaseOld = Databases.openXdmDatabase(resOldRev);
          final var resMgrOld = databaseOld.getResourceManager("shredded");
          final var wtx = resMgrOld.beginNodeTrx();
          final var databaseNew = Databases.openXdmDatabase(newRevTarget);
          final var resourceNew = databaseNew.getResourceManager("shredded");
          final var rtx = resourceNew.beginNodeReadOnlyTrx();
          final var fmes = new FMSE()) {
        fmes.diff(wtx, rtx);
      }
    } catch (final SirixException | IOException | XMLStreamException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Main entry point.
   *
   * @param args
   *        <p>
   *        arguments:
   *        </p>
   *        <ul>
   *        <li>args[0] - path to resource to update</li>
   *        <li>args[1] - path to new XML document</li>
   *        </ul>
   */
  public static void main(final String[] args) {
    if (args.length < 2 || args.length > 4) {
      throw new IllegalArgumentException("Usage: FSME oldResource newXMLDocument [startNodeKeyOld] [startNodeKeyNew]");
    }

    final var resOldRev = Paths.get(args[0]);
    final var resNewRev = Paths.get(args[1]);

    FMSEImport.xmlDataImport(resOldRev, resNewRev);
  }
}
