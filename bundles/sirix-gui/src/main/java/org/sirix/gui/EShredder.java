/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
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

package org.sirix.gui;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.ISession;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.exception.TTUsageException;
import org.sirix.service.xml.shredder.EShredderCommit;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.IShredder;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.shredder.XMLUpdateShredder;

/**
 * Determines how to shred.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
enum EShredder {

  /** Determines normal shredding. */
  NORMAL {
    @Override
    boolean shred(final File pSource, final File pTarget) {
      return shredder(checkNotNull(pSource), checkNotNull(pTarget), EType.NORMAL);
    }
  },

  /** Determines update only shredding. */
  UPDATEONLY {
    @Override
    boolean shred(final File pSource, final File pTarget) {
      return shredder(checkNotNull(pSource), checkNotNull(pTarget), EType.UPDATE);
    }
  };

  /** Logger. */
  private static final Logger LOGWRAPPER = LoggerFactory.getLogger(EShredder.class);

  /**
   * Shred XML file.
   * 
   * @param pSource
   *          source XML file
   * @param pTarget
   *          target folder
   * @return true if successfully shreddered, false otherwise
   */
  abstract boolean shred(final File pSource, final File pTarget);

  /** Kind of shredder. */
  private enum EType {
    /** Normal shredder. */
    NORMAL {
      @Override
      Callable<Long> newInstance(final File pSource, final INodeWriteTrx pWtx) throws IOException,
        XMLStreamException, TTUsageException {
        final XMLEventReader reader = XMLShredder.createFileReader(pSource);
        return new XMLShredder(pWtx, reader, EInsert.ASFIRSTCHILD);
      }
    },

    /** Update shredder. */
    UPDATE {
      @Override
      Callable<Long> newInstance(final File pSource, final INodeWriteTrx pWtx) throws IOException,
        XMLStreamException, TTUsageException {
        final XMLEventReader reader = XMLShredder.createFileReader(pSource);
        try {
          return new XMLUpdateShredder(pWtx, reader, EInsert.ASFIRSTCHILD, pSource,
            EShredderCommit.COMMIT);
        } catch (final TTIOException e) {
          throw new IOException(e);
        }
      }
    };

    /**
     * Get new instance of the appropriate shredder.
     * 
     * @param pSource
     *          source to shredder
     * @param pWtx
     *          sirix {@link INodeWriteTrx}
     * @return instance of appropriate {@link IShredder} implementation
     * @throws IOException
     *           if {@code pSource} can't be read
     * @throws XMLStreamException
     *           if parser encounters an error
     * @throws TTUsageException
     *           if the shredder isn't used properly
     */
    abstract Callable<Long> newInstance(final File pSource, final INodeWriteTrx pWtx) throws IOException,
      XMLStreamException, TTUsageException;
  }

  /**
   * Do the shredding.
   * 
   * @param pSource
   *          the source file to shredder
   * @param pTarget
   *          the database to create/open
   */
  private static boolean shredder(final File pSource, final File pTarget, final EType pType) {
    assert pSource != null;
    assert pTarget != null;
    assert pType != null;
    boolean retVal = true;
    try {
      final IDatabase database = setupDatabase(pTarget);
      try (final ISession session = database.getSession(new SessionConfiguration.Builder("shredded").build());
      final INodeWriteTrx wtx = session.beginNodeWriteTrx();) {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(pType.newInstance(pSource, wtx));
        executor.shutdown();
        executor.awaitTermination(5 * 60, TimeUnit.SECONDS);
      }
    } catch (final IOException | XMLStreamException | InterruptedException | AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
      retVal = false;
    }
    return retVal;

  }

  /**
   * Setup a new {@code database/resource}.
   * 
   * @param pTarget
   *          the database to create/open
   * @return {@link IDatabase} implementation
   * @throws AbsTTException
   *           if something went wrong
   */
  private static IDatabase setupDatabase(final File pTarget) throws AbsTTException {
    assert pTarget != null;
    final DatabaseConfiguration config = new DatabaseConfiguration(pTarget);
    Database.truncateDatabase(config);
    Database.createDatabase(config);
    final IDatabase db = Database.openDatabase(pTarget);
    db.createResource(new ResourceConfiguration.Builder("shredded", config).build());
    return db;
  }
}
