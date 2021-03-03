package org.sirix.access;

import org.sirix.access.trx.page.PageTrxFactory;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.io.Reader;
import org.sirix.io.IOStorage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractResourceStore<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource managers.
   */
  protected final ConcurrentMap<Path, R> resourceManagers;

  protected final PathBasedPool<ResourceManager<?, ?>> allResourceManagers;

  /**
   * The user, which interacts with SirixDB.
   */
  protected final User user;
  protected final String databaseName;
  protected final PageTrxFactory pageTrxFactory;

  public AbstractResourceStore(final ConcurrentMap<Path, R> resourceManagers,
                              final PathBasedPool<ResourceManager<?, ?>> allResourceManagers,
                               final User user,
                               final String databaseName,
                               final PageTrxFactory pageTrxFactory) {
    this.resourceManagers = resourceManagers;
    this.allResourceManagers = allResourceManagers;
    this.user = user;
    this.databaseName = databaseName;
    this.pageTrxFactory = pageTrxFactory;
  }

  protected UberPage getUberPage(final IOStorage storage) {
    final UberPage uberPage;
    if (storage.exists()) {
      try (final Reader reader = storage.createReader()) {
        final PageReference firstRef = reader.readUberPageReference();
        if (firstRef.getPage() == null) {
          uberPage = (UberPage) reader.read(firstRef, null);
        } else {
          uberPage = (UberPage) firstRef.getPage();
        }
      }
    } else {
      // Bootstrap uber page and make sure there already is a root node.
      uberPage = new UberPage();
    }
    return uberPage;
  }

  @Override
  public boolean hasOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceManagers.containsKey(resourceFile);
  }

  @Override
  public R getOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return resourceManagers.get(resourceFile);
  }

  @Override
  public void close() {
    resourceManagers.forEach((resourceName, resourceMgr) -> resourceMgr.close());
  }

  @Override
  public boolean closeResourceManager(final Path resourceFile) {
    final R manager = resourceManagers.remove(resourceFile);
    this.allResourceManagers.removeObject(resourceFile, manager);
    return manager != null;
  }
}
