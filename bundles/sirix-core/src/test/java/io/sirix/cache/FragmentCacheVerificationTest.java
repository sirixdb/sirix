package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Verify that BOTH RecordPageCache AND RecordPageFragmentCache are scanned for pins.
 */
public class FragmentCacheVerificationTest {

  private static final Path JSON_DIRECTORY = Paths.get("src", "test", "resources", "json");
  
  private Database<JsonResourceSession> database;
  
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }
  
  @After
  public void tearDown() {
    if (database != null) {
      database.close();
    }
    JsonTestHelper.closeEverything();
  }
  
  /**
   * Verify that RecordPageFragmentCache is included in pin count scanning.
   */
  @Test
  public void testFragmentCacheIsScanned() {
    System.setProperty("sirix.debug.pin.counts", "true");
    
    try {
      final var databaseFile = JsonTestHelper.PATHS.PATH1.getFile();
      final DatabaseConfiguration dbConf = new DatabaseConfiguration(databaseFile);
      Databases.createJsonDatabase(dbConf);
      database = Databases.openJsonDatabase(databaseFile);
      
      database.createResource(
          ResourceConfiguration.newBuilder("resource")
                              .useDeweyIDs(true)
                              .buildPathSummary(true)
                              .build()
      );
      
      try (final var session = database.beginResourceSession("resource");
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createFileReader(JSON_DIRECTORY.resolve("twitter.json"))
        );
        wtx.commit();
      }
      
      BufferManager bufferManager;
      try (final var session = database.beginResourceSession("resource");
           final var rtx = session.beginNodeReadOnlyTrx()) {
        bufferManager = rtx.getPageTrx().getBufferManager();
      }
      
      System.err.println("\n========================================");
      System.err.println("FRAGMENT CACHE VERIFICATION TEST");
      System.err.println("========================================");
      
      // Scan both caches separately
      var recordCacheReport = PinCountDiagnostics.scanCache(bufferManager.getRecordPageCache());
      var fragmentCacheReport = PinCountDiagnostics.scanCache(bufferManager.getRecordPageFragmentCache());
      var combinedReport = PinCountDiagnostics.scanBufferManager(bufferManager);
      
      System.err.println("\nRecordPageCache:");
      System.err.println("  Total pages: " + recordCacheReport.totalPages);
      System.err.println("  Pinned pages: " + recordCacheReport.pinnedPages);
      System.err.println("  Unpinned pages: " + recordCacheReport.unpinnedPages);
      
      System.err.println("\nRecordPageFragmentCache:");
      System.err.println("  Total pages: " + fragmentCacheReport.totalPages);
      System.err.println("  Pinned pages: " + fragmentCacheReport.pinnedPages);
      System.err.println("  Unpinned pages: " + fragmentCacheReport.unpinnedPages);
      
      System.err.println("\nCombined (scanBufferManager):");
      System.err.println("  Total pages: " + combinedReport.totalPages);
      System.err.println("  Pinned pages: " + combinedReport.pinnedPages);
      System.err.println("  Unpinned pages: " + combinedReport.unpinnedPages);
      
      // Verify math
      int expectedTotal = recordCacheReport.totalPages + fragmentCacheReport.totalPages;
      int expectedPinned = recordCacheReport.pinnedPages + fragmentCacheReport.pinnedPages;
      
      System.err.println("\nVerification:");
      System.err.println("  Expected total: " + expectedTotal + ", Actual: " + combinedReport.totalPages);
      System.err.println("  Expected pinned: " + expectedPinned + ", Actual: " + combinedReport.pinnedPages);
      
      assertEquals("Combined total should equal sum of both caches", expectedTotal, combinedReport.totalPages);
      assertEquals("Combined pinned should equal sum of both caches", expectedPinned, combinedReport.pinnedPages);
      
      if (combinedReport.pinnedPages > 0) {
        System.err.println("\n🔴 PINNED PAGES FOUND!");
        System.err.println("   RecordPageCache pinned: " + recordCacheReport.pinnedPages);
        System.err.println("   FragmentCache pinned: " + fragmentCacheReport.pinnedPages);
        fail("Found " + combinedReport.pinnedPages + " pinned pages");
      }
      
      System.err.println("\n✅ Fragment cache is properly scanned");
      System.err.println("✅ No pinned pages in either cache");
      
    } finally {
      System.clearProperty("sirix.debug.pin.counts");
    }
  }
}





