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
package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.sirix.access.DatabaseImpl;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Axis;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

/**
 * <p>
 * Watches a directory in a filesystem recursively for changes and updates a sirix database accordingly.
 * </p>
 * 
 * <p>
 * Note that it uses an {@code FSML} representation from <em>Alexander Holupirek</em> for the creation of the
 * database. Subsequent notifications currently don't use any extractors.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class FileSystemWatcher implements AutoCloseable {

  /**
   * Pool to handle transactional time, that is to commit changes after a specific time interval. One
   * thread in the pool is sufficient.
   */
  public final ScheduledExecutorService mPool = Executors.newSingleThreadScheduledExecutor();

  /**
   * Mapping of {@code {@link Path}/{@link Database} to {@link FileSystemWatcher} shared among all
   * instances.
   */
  private static final ConcurrentMap<PathDBContainer, FileSystemWatcher> INSTANCES =
    new ConcurrentHashMap<>();

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(FileSystemWatcher.class));

  /** {@link Path} to watch for modifications. */
  private final Path mPath;

  /** sirix {@link Database}. */
  private final Database mDatabase;

  /** sirix {@link Session}. */
  private final Session mSession;

  /** Determines the state. */
  private EState mState;

  /** sirix {@link NodeWriteTrx}. */
  private NodeWriteTrx mWtx;

  /** Possible states. */
  public enum EState {
    /** Loops and waits for events. */
    LOOP,

    /** Stops waiting for events. */
    NOLOOP
  }

  /**
   * Private constructor invoked from factory.
   * 
   * @param pPath
   *          {@link Path} reference which denotes the {@code path/directory} to watch for changes.
   * @param pDatabase
   *          {@link Database} to use for importing changed data into sirix
   */
  private FileSystemWatcher(@Nonnull final Path pPath, @Nonnull final Database pDatabase)
    throws SirixException {
    mPath = checkNotNull(pPath);
    mDatabase = checkNotNull(pDatabase);
    mSession = mDatabase.getSession(new SessionConfiguration.Builder("shredded").build());
    mWtx = mSession.beginNodeWriteTrx();
    mState = EState.LOOP;

    mPool.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          mWtx.commit();
        } catch (final SirixException e) {
          LOGWRAPPER.error(e.getMessage(), e);
        }
      }
    }, 60, 60, TimeUnit.SECONDS);
  }

  /**
   * Get an instance of {@link FileSystemWatcher}. If an instance with the specified {@code {@link Path}/
   * {@link Database} already exists this instance is returned.
   * 
   * @param pPath
   *          {@link Path} reference which denotes the {@code path/directory} to watch for changes.
   * @return a new {@link FileSystemWatcher} instance
   * @throws NullPointerException
   *           if any of the arguments are {@code null}
   * @throws SirixException
   *           if anything while setting up sirix failes
   */
  public static synchronized FileSystemWatcher getInstance(@Nonnull final Path pPath,
    @Nonnull final Database pDatabase) throws SirixException {
    final PathDBContainer container = new PathDBContainer(pPath, pDatabase);
    FileSystemWatcher watcher = INSTANCES.putIfAbsent(container, new FileSystemWatcher(pPath, pDatabase));
    if (watcher == null) {
      watcher = INSTANCES.get(container);
    }
    return watcher;
  }

  /**
   * Watch the directory for changes.
   * 
   * @param pVisitor
   *          optional visitor
   * @param pIndex
   *          an index of the directory paths to watch
   * @throws IOException
   *           if an I/O error occurs
   * @throws NullPointerException
   *           if {@code pIndex} is {@code null}
   */
  public void watch(@Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor,
    @Nonnull final Map<Path, org.sirix.fs.Path> pIndex) throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    final WatchRecursivelyVisitor fileVisitor = WatchRecursivelyVisitor.getInstance(watcher);
    Files.walkFileTree(mPath, fileVisitor);
    final Map<Path, org.sirix.fs.Path> index = checkNotNull(pIndex);

    for (; mState == EState.LOOP;) {
      // Wait for key to be signaled.
      WatchKey key;
      try {
        key = watcher.take();
      } catch (InterruptedException x) {
        return;
      }

      final Map<WatchKey, Path> keys = fileVisitor.getKeys();
      final Path dir = keys.get(key);
      if (dir == null) {
        LOGWRAPPER.error("WatchKey not recognized!!");
        continue;
      }

      for (WatchEvent<?> event : key.pollEvents()) {
        final WatchEvent.Kind<?> kind = event.kind();

        /*
         * This key is registered only for ENTRY_CREATE events,
         * but an OVERFLOW event can occur regardless if events are
         * lost or discarded.
         */
        if (kind == OVERFLOW) {
          continue;
        }

        /*
         * The filename is the context of the event. Cast is safe because
         * we registered a path instance.
         */
        WatchEvent<?> ev = event;

        for (int i = 0; i < ev.count(); i++) {
          final Path name = (Path)ev.context();
          final Path child = dir.resolve(name);

          if (kind == ENTRY_CREATE && Files.isDirectory(child, NOFOLLOW_LINKS)) {
            Files.walkFileTree(child, new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult preVisitDirectory(final Path pDir, final BasicFileAttributes pAttrs)
                throws IOException {
                checkNotNull(pDir);
                checkNotNull(pAttrs);
                final WatchKey key = pDir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
                keys.put(key, pDir);
                entryCreated(pVisitor, index, pDir);
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFile(final Path pFile, final BasicFileAttributes pAttrs)
                throws IOException {
                checkNotNull(pFile);
                checkNotNull(pAttrs);
                entryCreated(pVisitor, index, pFile);
                return FileVisitResult.CONTINUE;
              }
            });
          } else {
            processEvent(ev, pVisitor, index, watcher, child);
          }
        }
      }

      /*
       * Reset the key -- this step is critical if you want to receive
       * further watch events. If the key is no longer valid, the directory
       * is inaccessible so exit the loop.
       */
      final boolean valid = key.reset();
      if (!valid) {
        keys.remove(key);

        // All directories are inaccessible.
        if (keys.isEmpty()) {
          mState = EState.NOLOOP;
        }
      }
    }

    watcher.close();
  }

  /**
   * Set state.
   * 
   * @param pState
   *          {@link EState} value
   * @throws NullPointerException
   *           if {@code pState} is {@code null}
   */
  public void setState(final EState pState) {
    mState = checkNotNull(pState);
  }

  /**
   * Process an event if the {@link Path} context is available.
   * 
   * @param pEvent
   *          {@link WatchEvent<Path>} reference
   * @param pVisitor
   *          optional visitor
   * @param pIndex
   *          simple path index
   * @throws IOException
   *           if an I/O error occurs
   */
  private void processEvent(@Nonnull final WatchEvent<?> pEvent,
    @Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor, @Nonnull final Map<Path, org.sirix.fs.Path> pIndex,
    @Nonnull final WatchService pWatcher, @Nonnull final Path pPath) throws IOException {
    assert pEvent != null;
    final Kind<?> type = pEvent.kind();

    Optional<?> optional = Optional.fromNullable(pEvent.context());
    if (optional.isPresent()) {
      if (type == ENTRY_CREATE) {
        entryCreated(pVisitor, pIndex, pPath);
      } else if (type == ENTRY_DELETE && !Files.exists(pPath)) {
        entryDeletes(pVisitor, pIndex, pPath);
      } else if (type == ENTRY_MODIFY) {
        entryModified(pVisitor, pIndex, pPath);
      }
    } else {
      LOGWRAPPER.info("no path associated with the context!");
    }
  }

  /**
   * Find node corresponding to the path. The {@link NodeWriteTrx} globally used is moved to the found
   * node.
   * 
   * @param pXPath
   *          xpath expression
   * @throws SirixXPathException
   *           if expression isn't valid
   * @throws NullPointerException
   *           if {@code pXPath} is {@code null}
   */
  private void findNode(@Nonnull final String pXPath) throws SirixXPathException {
    final Axis axis = new XPathAxis(mWtx, checkNotNull(pXPath));
    int countResults = 0;
    long resultNodeKey = (Long)Fixed.NULL_NODE_KEY.getStandardProperty();
    while (axis.hasNext()) {
      resultNodeKey = axis.next();
      countResults++;
      assert countResults == 1 : "At maximum one item should be found!";
    }
    mWtx.moveTo(resultNodeKey);
  }

  /** Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_MODIFY ENTRY_MODIFY} event. */
  private void entryModified(@Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor,
    @Nonnull final Map<Path, org.sirix.fs.Path> pIndex, @Nonnull final Path pPath) {
    try {
      execute(OperationType.UPDATE, pVisitor, pIndex, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /** Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_DELETE ENTRY_DELETE} event. */
  private void entryDeletes(@Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor,
    @Nonnull final Map<Path, org.sirix.fs.Path> pIndex, @Nonnull final Path pPath) {
    try {
      execute(OperationType.DELETE, pVisitor, pIndex, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /** Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_CREATE ENTRY_CREATE} event. */
  private void entryCreated(@Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor,
    @Nonnull final Map<Path, org.sirix.fs.Path> pIndex, @Nonnull final Path pPath) {
    try {
      execute(OperationType.INSERT, pVisitor, pIndex, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Execute a command on the node denoted by the event path.
   * 
   * @param pOperation
   *          {@link OperationType} value
   * @param pVisitor
   *          optional visitor
   * @param pIndex
   *          simple path index
   * @throws SirixException
   *           if operation in sirix fails
   */
  private void execute(@Nonnull final Operation<NodeWriteTrx> pOperation,
    @Nonnull final Optional<Visitor<NodeWriteTrx>> pVisitor, @Nonnull final Map<Path, org.sirix.fs.Path> pIndex,
    @Nonnull final Path pPath) throws SirixException {
    assert pOperation != null;
    assert pIndex != null;
    Path path =
      pOperation == OperationType.INSERT ? mPath.resolve(pPath).getParent().normalize() : mPath.resolve(pPath)
        .normalize();
    if (path != null) {
      final StringBuilder queryBuilder = new StringBuilder("/fsml/");
      queryBuilder.append("dir[@name=\"").append(XMLToken.escapeAttribute(mPath.getFileName().toString())).append(
        "\"]");
      final Path relativized = mPath.relativize(path);
      Path buildPath = mPath;
      for (final Path element : relativized) {
        if (!element.getFileName().toString().isEmpty()) {
          if (Files.exists(buildPath.resolve(element))) {
            if (Files.isDirectory(buildPath.resolve(element))) {
              queryBuilder.append("/dir");
            } else if (Files.isSymbolicLink(buildPath.resolve(element))
              || Files.isRegularFile(buildPath.resolve(element))) {
              queryBuilder.append("/file");
            }
          } else {
            // DELETED.
            LOGWRAPPER.debug("path: " + path);
            final org.sirix.fs.Path kind = pIndex.remove(path);
            assert kind != null;
            kind.append(queryBuilder);
          }
          queryBuilder.append("[@name=\"").append(XMLToken.escapeAttribute(element.getFileName().toString())).append(
            "\"]");
          buildPath = buildPath.resolve(element);
        }
      }
      final String query = queryBuilder.toString();
      LOGWRAPPER.debug("[execute] path: " + query);
      findNode(query);
      pOperation.execute(mWtx, pVisitor, pIndex, pPath);
    }
  }

  @Override
  public void close() throws SirixException {
    mWtx.commit();
    mWtx.close();
    mSession.close();
    mPool.shutdown();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", mPath).add("database", mDatabase)
      .add("transaction", mWtx).toString();
  }

  /**
   * Main entry point.
   * 
   * @param pArgs
   *          first argument speficies the path/directory to watch for changes, the second argument
   *          specifies the database to which to append incoming events; <strong>note that any existing
   *          database is truncated first if an optional third argument is set to {@code true}</strong>
   * @throws SirixException
   *           if sirix encounters any error
   * @throws IOException
   *           if any I/O error occurs
   */
  public static void main(final String[] pArgs) throws SirixException, IOException {
    if (pArgs.length < 2 || pArgs.length > 3) {
      LOGWRAPPER.info("Usage: FileSystemWatcher pathToWatch pathToDatabase [true|false]");
    }

    if (Boolean.parseBoolean(pArgs[2])) {
      DatabaseImpl.truncateDatabase(new DatabaseConfiguration(new File(pArgs[1])));
    }
    final File databasePath = new File(pArgs[1]);
    final DatabaseConfiguration conf = new DatabaseConfiguration(databasePath);
    Map<Path, org.sirix.fs.Path> index = null;
    if (Files.exists(Paths.get(pArgs[1]))) {
      DatabaseImpl.truncateDatabase(conf);
    }

    DatabaseImpl.createDatabase(conf);
    final Database database = DatabaseImpl.openDatabase(databasePath);
    database.createResource(new ResourceConfiguration.Builder("shredded", conf).build());
    index =
      FileHierarchyWalker.parseDir(Paths.get(pArgs[0]), database, Optional
        .<Visitor<NodeWriteTrx>> of(new ProcessFileSystemAttributes()));
    assert index != null;

    final FileSystemWatcher watcher =
      FileSystemWatcher.getInstance(Paths.get(pArgs[0]), DatabaseImpl.openDatabase(databasePath));
    watcher.watch(Optional.<Visitor<NodeWriteTrx>> absent(), index);
    watcher.close();
  }
}
