package io.sirix.page;

import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.page.delegates.BitmapReferencesPage;
import net.openhft.chronicle.bytes.BytesIn;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.cache.PageContainer;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

/**
 * Page utilities.
 *
 * @author Johannes Lichtenberger
 */
public final class PageUtils {

  /**
   * Private constructor to prevent instantiation.
   */
  private PageUtils() {
    throw new AssertionError("May never be instantiated!");
  }

  public static Page setReference(Page pageDelegate, int offset, PageReference pageReference) {
    final var hasToGrow = pageDelegate.setOrCreateReference(offset, pageReference);

    if (hasToGrow) {
      if (pageDelegate instanceof ReferencesPage4) {
        pageDelegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) pageDelegate);
      } else {
        assert pageDelegate instanceof BitmapReferencesPage;
        pageDelegate = new FullReferencesPage((BitmapReferencesPage) pageDelegate);
      }
      pageDelegate.setOrCreateReference(offset, pageReference);
    }

    return pageDelegate;
  }

  public static Page createDelegate(BytesIn<?> in, SerializationType type) {
    final byte kind = in.readByte();
    return switch (kind) {
      case 0 -> new ReferencesPage4(in, type);
      case 1 -> new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, in, type);
      case 2 -> new FullReferencesPage(in, type);
      default -> throw new IllegalStateException();
    };
  }

  /**
   * Create the initial tree structure.
   *
   * @param databaseType The type of database.
   * @param reference    reference from revision root
   * @param indexType    the index type
   */
  public static void createTree(final DatabaseType databaseType, @NonNull PageReference reference,
      final IndexType indexType, final PageReadOnlyTrx pageReadTrx, final TransactionIntentLog log) {
    // Create new record page.
    final KeyValueLeafPage recordPage = new KeyValueLeafPage(Fixed.ROOT_PAGE_KEY.getStandardProperty(),
                                                             indexType,
                                                             pageReadTrx.getResourceSession().getResourceConfig(),
                                                             pageReadTrx.getRevisionNumber());

    final ResourceConfiguration resourceConfiguration = pageReadTrx.getResourceSession().getResourceConfig();

    final SirixDeweyID id = resourceConfiguration.areDeweyIDsStored ? SirixDeweyID.newRootID() : null;

    // Create a {@link DocumentRootNode}.
    recordPage.setRecord(databaseType.getDocumentNode(id));

    log.put(reference, PageContainer.getInstance(recordPage, recordPage));
  }
}
