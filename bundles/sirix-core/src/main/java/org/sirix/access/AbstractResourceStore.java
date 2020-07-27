package org.sirix.access;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.io.Reader;
import org.sirix.io.IOStorage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractResourceStore<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /**
   * Central repository of all open resource managers.
   */
  protected final ConcurrentMap<Path, R> resourceManagers;

  /**
   * The user, which interacts with SirixDB.
   */
  protected final User user;

  public AbstractResourceStore(final ConcurrentMap<Path, R> resourceManagers, final User user) {
    this.resourceManagers = resourceManagers;
    this.user = user;
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
    DatabasesInternals.removeResourceManager(resourceFile, manager);
    return manager != null;
  }
}
