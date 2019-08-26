package org.sirix.page;

import javax.annotation.Nonnull;
import org.sirix.access.DatabaseType;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.SirixDeweyID;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

/**
 * Page utilities.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PageUtils {

  /**
   * Private constructor to prevent instantiation.
   */
  private PageUtils() {
    throw new AssertionError("May never be instantiated!");
  }

  /**
   * Create the initial tree structure.
   *
   * @param reference reference from revision root
   * @param pageKind the page kind
   */
  public static void createTree(@Nonnull PageReference reference, final PageKind pageKind, final int index,
      final PageReadOnlyTrx pageReadTrx, final TransactionIntentLog log) {
    final Page page = new IndirectPage();
    log.put(reference, PageContainer.getInstance(page, page));
    reference = page.getReference(0);

    // Create new record page.
    final UnorderedKeyValuePage ndp = new UnorderedKeyValuePage(Fixed.ROOT_PAGE_KEY.getStandardProperty(), pageKind,
        Constants.NULL_ID_LONG, pageReadTrx);

    final ResourceConfiguration resourceConfiguration = pageReadTrx.getResourceManager().getResourceConfig();

    // Create a {@link DocumentRootNode}.
    final SirixDeweyID id = resourceConfiguration.areDeweyIDsStored
        ? SirixDeweyID.newRootID()
        : null;

    // TODO: Should be passed from the method... chaining up.
    final DatabaseType dbType = pageReadTrx.getResourceManager().getDatabase().getDatabaseConfig().getDatabaseType();

    ndp.setEntry(0L, dbType.getDocumentNode(id));

    log.put(reference, PageContainer.getInstance(ndp, ndp));
  }
}
