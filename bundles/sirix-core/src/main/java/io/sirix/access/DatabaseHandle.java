package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.Transaction;
import io.sirix.exception.SirixThreadedException;

import java.nio.file.Path;
import java.util.List;

/** An independently closeable reference to the process's shared database instance. */
final class DatabaseHandle<T extends ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements Database<T> {
  private final Databases.OpenDatabase<T> owner;
  private final ResourceStore<T> resourceStore;
  private volatile boolean closed;
  private volatile boolean closing;

  DatabaseHandle(final Databases.OpenDatabase<T> owner, final User user) {
    this.owner = owner;
    resourceStore = owner.localDatabase.newUserResourceStore(user);
  }

  private Database<T> database() {
    if (closed || closing || !owner.database.isOpen()) {
      throw new IllegalStateException("Database handle is closing or already closed.");
    }
    return owner.database;
  }

  @Override
  public boolean isOpen() {
    return !closed && owner.database.isOpen();
  }

  @Override
  public synchronized boolean createResource(final ResourceConfiguration config) {
    database();
    return owner.localDatabase.createResource(config, resourceStore);
  }

  @Override
  public boolean existsResource(final String resourceName) {
    return database().existsResource(resourceName);
  }

  @Override
  public List<Path> listResources() {
    return database().listResources();
  }

  @Override
  public synchronized T beginResourceSession(final String resourceName) {
    database();
    return owner.localDatabase.beginUserResourceSession(resourceName, resourceStore);
  }

  @Override
  public Database<T> removeResource(final String resourceName) {
    database().removeResource(resourceName);
    return this;
  }

  @Override
  public void close() {
    synchronized (this) {
      while (closing) {
        try {
          wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SirixThreadedException(e);
        }
      }
      if (closed) {
        return;
      }
      closing = true;
    }
    try {
      resourceStore.close();
      Databases.releaseDatabase(owner, this);
      closed = true;
    } finally {
      synchronized (this) {
        closing = false;
        notifyAll();
      }
    }
  }

  @Override
  public DatabaseConfiguration getDatabaseConfig() {
    return database().getDatabaseConfig();
  }

  @Override
  public Transaction beginTransaction() {
    return database().beginTransaction();
  }

  @Override
  public String getResourceName(final long id) {
    return database().getResourceName(id);
  }

  @Override
  public long getResourceID(final String name) {
    return database().getResourceID(name);
  }

  @Override
  public String getName() {
    return database().getName();
  }
}
