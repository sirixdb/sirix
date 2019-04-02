package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.io.Reader;
import org.sirix.io.Storage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

public abstract class AbstractResourceStore<R extends ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx>>
    implements ResourceStore<R> {

  /** Central repository of all open resource managers. */
  protected final ConcurrentMap<Path, R> mResourceManagers;

  public AbstractResourceStore(final ConcurrentMap<Path, R> resourceManagers) {
    mResourceManagers = resourceManagers;
  }

  protected UberPage getUberPage(final Storage storage) {
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
    return mResourceManagers.containsKey(resourceFile);
  }

  @Override
  public R getOpenResourceManager(final Path resourceFile) {
    checkNotNull(resourceFile);
    return mResourceManagers.get(resourceFile);
  }

  @Override
  public void close() {
    mResourceManagers.forEach((resourceName, resourceMgr) -> resourceMgr.close());
  }

  @Override
  public boolean closeResource(final Path resourceFile) {
    final R manager = mResourceManagers.remove(resourceFile);
    Databases.removeResourceManager(resourceFile, manager);
    return manager != null;
  }

}
