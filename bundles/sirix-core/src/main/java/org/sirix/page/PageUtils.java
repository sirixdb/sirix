package org.sirix.page;

import java.util.Optional;
import javax.annotation.Nonnull;
import org.sirix.api.PageReadTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
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
  public static void createTree(@Nonnull PageReference reference, final PageKind pageKind,
      final int index, final PageReadTrx pageReadTrx, final TransactionIntentLog log) {
    Page page = null;

    // Level page count exponent from the configuration.
    final int[] levelPageCountExp = pageReadTrx.getUberPage().getPageCountExp(pageKind);

    // Remaining levels.
    for (int i = 0, l = levelPageCountExp.length; i < l; i++) {
      page = new IndirectPage();
      log.put(reference, PageContainer.getInstance(page, page));
      reference = page.getReference(0);
    }

    // Create new record page.
    final UnorderedKeyValuePage ndp = new UnorderedKeyValuePage(
        Fixed.ROOT_PAGE_KEY.getStandardProperty(), pageKind, Constants.NULL_ID_LONG, pageReadTrx);

    // Create a {@link DocumentRootNode}.
    final Optional<SirixDeweyID> id =
        pageReadTrx.getResourceManager().getResourceConfig().areDeweyIDsStored
            ? Optional.of(SirixDeweyID.newRootID())
            : Optional.empty();
    final NodeDelegate nodeDel = new NodeDelegate(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0,
        id);
    final StructNodeDelegate strucDel = new StructNodeDelegate(nodeDel,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    ndp.setEntry(0L, new DocumentRootNode(nodeDel, strucDel));
    log.put(reference, PageContainer.getInstance(ndp, ndp));
  }
}
