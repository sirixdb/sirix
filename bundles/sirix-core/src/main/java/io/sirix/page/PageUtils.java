package io.sirix.page;

import io.sirix.access.DatabaseType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.utils.OS;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.node.BytesIn;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageContainer;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;

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
      final IndexType indexType, final StorageEngineReader pageReadTrx, final TransactionIntentLog log) {
    // Create new record page.
    final ResourceConfiguration resourceConfiguration = pageReadTrx.getResourceSession().getResourceConfig();
    
    // Direct allocation (no pool)
    final MemorySegmentAllocator allocator = OS.isWindows() 
        ? WindowsMemorySegmentAllocator.getInstance()
        : LinuxMemorySegmentAllocator.getInstance();
    
    final KeyValueLeafPage recordPage = new KeyValueLeafPage(
        Fixed.ROOT_PAGE_KEY.getStandardProperty(),
        indexType,
        resourceConfiguration,
        pageReadTrx.getRevisionNumber(),
        allocator.allocate(SIXTYFOUR_KB),
        resourceConfiguration.areDeweyIDsStored ? allocator.allocate(SIXTYFOUR_KB) : null,
        false  // Memory from allocator - release on close()
    );

    final SirixDeweyID id = resourceConfiguration.areDeweyIDsStored ? SirixDeweyID.newRootID() : null;

    // Create a {@link DocumentRootNode}.
    recordPage.setRecord(databaseType.getDocumentNode(id));

    log.put(reference, PageContainer.getInstance(recordPage, recordPage));
  }

  /**
   * Create the initial HOT (Height Optimized Trie) tree structure.
   *
   * <p>Unlike the traditional tree which uses {@link KeyValueLeafPage},
   * this creates an {@link HOTLeafPage} for cache-friendly secondary indexes.</p>
   *
   * @param reference   reference from revision root
   * @param indexType   the index type (PATH, CAS, or NAME)
   * @param pageReadTrx the storage engine reader
   * @param log         the transaction intent log
   */
  public static void createHOTTree(@NonNull PageReference reference,
      final IndexType indexType, final StorageEngineReader pageReadTrx, final TransactionIntentLog log) {
    
    // Create new HOT leaf page (starts as a leaf, grows into trie on demand)
    final HOTLeafPage hotLeafPage = new HOTLeafPage(
        Fixed.ROOT_PAGE_KEY.getStandardProperty(),
        pageReadTrx.getRevisionNumber(),
        indexType
    );

    // Set page key on reference
    reference.setKey(Fixed.ROOT_PAGE_KEY.getStandardProperty());
    
    log.put(reference, PageContainer.getInstance(hotLeafPage, hotLeafPage));
  }

  /**
   * Fix up PageReferences in a loaded page by setting database and resource IDs.
   * This follows the PostgreSQL pattern where BufferTag components (tablespace_oid, database_oid, 
   * relation_oid) from the read context are combined with on-disk block numbers to create full
   * cache keys. Pages store only page numbers; the database/resource context comes from the caller.
   *
   * @param page the page to fix up
   * @param databaseId the database ID to set
   * @param resourceId the resource ID to set
   */
  public static void fixupPageReferenceIds(Page page, long databaseId, long resourceId) {
    if (page == null) {
      return;
    }
    
    // Some page types (like UberPage) don't have PageReferences to fix up
    try {
      var references = page.getReferences();
      if (references != null) {
        for (PageReference ref : references) {
          if (ref != null) {
            ref.setDatabaseId(databaseId);
            ref.setResourceId(resourceId);
          }
        }
      }
    } catch (UnsupportedOperationException e) {
      // This is expected for page types that don't have references (e.g., UberPage, KeyValueLeafPage)
      // Just skip the fixup for these pages
    }
  }
}
