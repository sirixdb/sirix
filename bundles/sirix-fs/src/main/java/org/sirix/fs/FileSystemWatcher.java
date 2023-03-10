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
package org.sirix.fs;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
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
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Axis;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.XMLToken;
import org.slf4j.LoggerFactory;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;

/**
 * <p>
 * Watches a directory in a filesystem recursively for changes and updates a sirix database
 * accordingly.
 * </p>
 *
 * <p>
 * Note that it uses an {@code FSML} representation from <em>Alexander Holupirek</em> for the
 * creation of the database. Subsequent notifications currently don't use any extractors.
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
  private static final ConcurrentMap<PathDBContainer, FileSystemWatcher> INSTANCES = new ConcurrentHashMap<>();

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(FileSystemWatcher.class));

  /** {@link Path} to watch for modifications. */
  private final Path mPath;

  /** Sirix {@link Database}. */
  private final Database<XmlResourceManager> mDatabase;

  /** Sirix {@link XmlResourceManager}. */
  private final XmlResourceManager mResource;

  /** Determines the state. */
  private State mState;

  /** Sirix {@link XmlNodeTrx}. */
  private XmlNodeTrx mWtx;

  /** Possible states. */
  public enum State {
    /** Loops and waits for events. */
    LOOP,

    /** Stops waiting for events. */
    NOLOOP
  }

  /**
   * Private constructor invoked f rom factory.
   *
   * @param path {@link Path} reference which denotes the {@code path/directory} to watch for changes.
   * @param database {@link Database} to use for importing changed data into sirix
   */
  private FileSystemWatcher(final Path path, final Database<XmlResourceManager> database) throws SirixException {
    mPath = requireNonNull(path);
    mDatabase = requireNonNull(database);
    mResource = mDatabase.openResourceManager("shredded");
    mWtx = mResource.beginNodeTrx();
    mState = State.LOOP;
    mPool.scheduleAtFixedRate(() -> mWtx.commit(), 60, 60, TimeUnit.SECONDS);
  }

  /**
   * Get an instance of {@link FileSystemWatcher}. If an instance with the specified {@code {@link
   * Path}/ {@link Database} already exists this instance is returned.
   *
   * @param pPath {@link Path} reference which denotes the {@code path/directory} to watch for
   * changes.
   *
   * @return a new {@link FileSystemWatcher} instance
   * @throws NullPointerException if any of the arguments are {@code null}
   * @throws SirixException if anything while setting up sirix failes
   */
  public static synchronized FileSystemWatcher getInstance(final Path path, final Database<XmlResourceManager> database)
      throws SirixException {
    final PathDBContainer container = new PathDBContainer(path, database);
    FileSystemWatcher watcher = INSTANCES.putIfAbsent(container, new FileSystemWatcher(path, database));
    if (watcher == null) {
      watcher = INSTANCES.get(container);
    }
    return watcher;
  }

  /**
   * Watch the directory for changes.
   *
   * @param visitor optional visitor
   * @param index an index of the directory paths to watch
   * @throws IOException if an I/O error occurs
   * @throws NullPointerException if {@code pIndex} is {@code null}
   */
  public void watch(final Visitor<XmlNodeTrx> visitor, final Map<Path, FileSystemPath> index) throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    final WatchRecursivelyVisitor fileVisitor = WatchRecursivelyVisitor.getInstance(watcher);
    Files.walkFileTree(mPath, fileVisitor);
    requireNonNull(index);

    for (; mState == State.LOOP;) {
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
         * This key is registered only for ENTRY_CREATE events, but an OVERFLOW event can occur regardless
         * if events are lost or discarded.
         */
        if (kind == OVERFLOW) {
          continue;
        }

        /*
         * The filename is the context of the event. Cast is safe because we registered a path instance.
         */
        WatchEvent<?> ev = event;

        for (int i = 0; i < ev.count(); i++) {
          final Path name = (Path) ev.context();
          final Path child = dir.resolve(name);

          if (kind == ENTRY_CREATE && Files.isDirectory(child, NOFOLLOW_LINKS)) {
            Files.walkFileTree(child, new SimpleFileVisitor<Path>() {
              @Override
              public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                  throws IOException {
                requireNonNull(dir);
                requireNonNull(attrs);
                final WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
                keys.put(key, dir);
                entryCreated(visitor, index, dir);
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFile(final Path pFile, final BasicFileAttributes pAttrs) throws IOException {
                requireNonNull(pFile);
                requireNonNull(pAttrs);
                entryCreated(visitor, index, pFile);
                return FileVisitResult.CONTINUE;
              }
            });
          } else {
            processEvent(ev, visitor, index, watcher, child);
          }
        }
      }

      /*
       * Reset the key -- this step is critical if you want to receive further watch events. If the key is
       * no longer valid, the directory is inaccessible so exit the loop.
       */
      final boolean valid = key.reset();
      if (!valid) {
        keys.remove(key);

        // All directories are inaccessible.
        if (keys.isEmpty()) {
          mState = State.NOLOOP;
        }
      }
    }

    watcher.close();
  }

  /**
   * Set state.
   *
   * @param pState {@link State} value
   * @throws NullPointerException if {@code pState} is {@code null}
   */
  public void setState(final State pState) {
    mState = requireNonNull(pState);
  }

  /**
   * Process an event if the {@link Path} context is available.
   *
   * @param event {@link WatchEvent<Path>} reference
   * @param visitor optional visitor
   * @param index simple path index
   * @throws IOException if an I/O error occurs
   */
  private void processEvent(final WatchEvent<?> event, final Visitor<XmlNodeTrx> visitor,
      final Map<Path, FileSystemPath> index, final WatchService watcher, final Path path) throws IOException {
    assert event != null;
    final Kind<?> type = event.kind();

    Optional<?> optional = Optional.fromNullable(event.context());
    if (optional.isPresent()) {
      if (type == ENTRY_CREATE) {
        entryCreated(visitor, index, path);
      } else if (type == ENTRY_DELETE && !Files.exists(path)) {
        entryDeletes(visitor, index, path);
      } else if (type == ENTRY_MODIFY) {
        entryModified(visitor, index, path);
      }
    } else {
      LOGWRAPPER.info("no path associated with the context!");
    }
  }

  /**
   * Find node corresponding to the path. The {@link XmlNodeTrx} globally used is moved to the found
   * node.
   *
   * @param query xpath expression
   * @throws SirixXPathException if expression isn't valid
   * @throws NullPointerException if {@code pXPath} is {@code null}
   */
  private void findNode(final String query) throws SirixXPathException {
    final Axis axis = new XPathAxis(mWtx, requireNonNull(query));
    int countResults = 0;
    long resultNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    while (axis.hasNext()) {
      resultNodeKey = axis.next();
      countResults++;
      assert countResults == 1 : "At maximum one item should be found!";
    }
    mWtx.moveTo(resultNodeKey);
  }

  /**
   * Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_MODIFY ENTRY_MODIFY} event.
   */
  private void entryModified(final Visitor<XmlNodeTrx> visitor, final Map<Path, FileSystemPath> pIndex,
      final Path pPath) {
    try {
      execute(OperationType.UPDATE, visitor, pIndex, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_DELETE ENTRY_DELETE} event.
   */
  private void entryDeletes(final Visitor<XmlNodeTrx> visitor, final Map<Path, FileSystemPath> index,
      final Path pPath) {
    try {
      execute(OperationType.DELETE, visitor, index, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Process an {@link java.nio.file.StandardWatchEventKinds#ENTRY_CREATE ENTRY_CREATE} event.
   */
  private void entryCreated(final Visitor<XmlNodeTrx> visitor, final Map<Path, FileSystemPath> pIndex,
      final Path pPath) {
    try {
      execute(OperationType.INSERT, visitor, pIndex, pPath);
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  /**
   * Execute a command on the node denoted by the event path.
   *
   * @param operation {@link OperationType} value
   * @param visitor optional visitor
   * @param index simple path index
   * @throws SirixException if operation in sirix fails
   */
  private void execute(final Operation<XmlNodeTrx> operation, final Visitor<XmlNodeTrx> visitor,
      final Map<Path, FileSystemPath> index, final Path pathToWatch) throws SirixException {
    assert operation != null;
    assert index != null;

    final Path path = operation == OperationType.INSERT
        ? mPath.resolve(pathToWatch).getParent().normalize()
        : mPath.resolve(pathToWatch).normalize();

    if (path != null) {
      final StringBuilder queryBuilder = new StringBuilder("/fsml/");
      queryBuilder.append("dir[@name=\"")
                  .append(XMLToken.escapeAttribute(mPath.getFileName().toString()))
                  .append("\"]");
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
            final FileSystemPath kind = index.remove(path);
            assert kind != null;
            kind.append(queryBuilder);
          }
          queryBuilder.append("[@name=\"")
                      .append(XMLToken.escapeAttribute(element.getFileName().toString()))
                      .append("\"]");
          buildPath = buildPath.resolve(element);
        }
      }

      final String query = queryBuilder.toString();
      LOGWRAPPER.debug("[execute] path: " + query);
      findNode(query);
      operation.execute(mWtx, visitor, index, path);
    }
  }

  @Override
  public void close() throws SirixException {
    mWtx.commit();
    mWtx.close();
    mResource.close();
    mPool.shutdown();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("path", mPath)
                      .add("database", mDatabase)
                      .add("transaction", mWtx)
                      .toString();
  }

  /**
   * Main entry point.
   *
   * @param args first argument speficies the path/directory to watch for changes, the second argument
   *        specifies the database to which to append incoming events; <strong>note that any existing
   *        database is truncated first if an optional third argument is set to {@code true}</strong>
   * @throws SirixException if sirix encounters any error
   * @throws IOException if any I/O error occurs
   */
  public static void main(final String[] args) throws SirixException, IOException {
    if (args.length < 2 || args.length > 3) {
      LOGWRAPPER.info("Usage: FileSystemWatcher pathToWatch pathToDatabase [true|false]");
    }

    if (Boolean.parseBoolean(args[2])) {
      Databases.removeDatabase(Paths.get(args[2]));
    }
    final Path databasePath = Paths.get(args[1]);
    final DatabaseConfiguration conf = new DatabaseConfiguration(databasePath);
    Map<Path, FileSystemPath> index = null;
    if (Files.exists(Paths.get(args[1]))) {
      Databases.removeDatabase(databasePath);
    }

    Databases.createXmlDatabase(conf);
    try (final var database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(new ResourceConfiguration.Builder("shredded").build());
      index =
          FileHierarchyWalker.parseDir(Paths.get(args[0]), database, Optional.of(new ProcessFileSystemAttributes()));
      assert index != null;

      try (final FileSystemWatcher watcher =
          FileSystemWatcher.getInstance(Paths.get(args[0]), Databases.openXmlDatabase(databasePath))) {
        watcher.watch(null, index);
      }
    }
  }
}
