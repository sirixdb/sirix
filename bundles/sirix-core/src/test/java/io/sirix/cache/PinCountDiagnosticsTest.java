package io.sirix.cache;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Diagnostic test to analyze guard/pin count leaks.
 * Run with: -Dsirix.debug.guard.tracking=true
 */
public class PinCountDiagnosticsTest {

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
   * Test that scans a JSON document multiple times and tracks pin counts.
   * This test helps identify whether leaked pages have pinCount > 0 or pinCount == 0.
   */
  @Test
  public void testPinCountLeakDiagnostics() {
    // Enable guard tracking (successor to pin count debugging)
    System.setProperty("sirix.debug.guard.tracking", "true");
    
    try {
      // Create database with Chicago Twitter data
      final var databaseFile = JsonTestHelper.PATHS.PATH1.getFile();
      final DatabaseConfiguration dbConf = new DatabaseConfiguration(databaseFile);
      Databases.createJsonDatabase(dbConf);
      database = Databases.openJsonDatabase(databaseFile);
      
      database.createResource(
          ResourceConfiguration.newBuilder("resource")
                              .useDeweyIDs(true)
                              .useTextCompression(false)
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
      
      // Get buffer manager for diagnostics
      BufferManager bufferManager;
      try (final var session = database.beginResourceSession("resource");
           final var rtx = session.beginNodeReadOnlyTrx()) {
        bufferManager = rtx.getPageTrx().getBufferManager();
      }
      
      System.err.println("\n========================================");
      System.err.println("INITIAL STATE - Before any reads");
      System.err.println("========================================");
      PinCountDiagnostics.DiagnosticReport initialReport = 
          PinCountDiagnostics.scanBufferManager(bufferManager);
      System.err.println(initialReport);
      
      // Perform multiple iterations of descendant axis scans
      final int iterations = 10;
      
      for (int i = 0; i < iterations; i++) {
        try (final var session = database.beginResourceSession("resource");
             final var rtx = session.beginNodeReadOnlyTrx()) {
          
          // Scan descendant axis - this is known to cause leaks
          rtx.moveToDocumentRoot();
          var axis = new DescendantAxis(rtx);
          int nodeCount = 0;
          while (axis.hasNext()) {
            axis.nextLong();
            nodeCount++;
          }
          
          System.err.println("\nIteration " + (i + 1) + ": Scanned " + nodeCount + " nodes");
        }
        
        // After transaction closes, check pin counts
        if ((i + 1) % 5 == 0) {
          System.err.println("\n========================================");
          System.err.println("AFTER ITERATION " + (i + 1));
          System.err.println("========================================");
          PinCountDiagnostics.DiagnosticReport report = 
              PinCountDiagnostics.scanBufferManager(bufferManager);
          System.err.println(report);
          
          // Analysis: Are leaked pages pinned or unpinned?
          if (report.pinnedPages > 0) {
            System.err.println("\n🔴 CRITICAL: " + report.pinnedPages + " PINNED pages found!");
            System.err.println("   This indicates PINNING BUGS - pages not being unpinned.");
            System.err.println("   Total pinned memory: " + (report.pinnedMemoryBytes / 1024.0 / 1024.0) + " MB");
          }
          
          if (report.unpinnedPages > 100) {
            System.err.println("\n⚠️  WARNING: " + report.unpinnedPages + " UNPINNED pages in cache");
            System.err.println("   This may indicate CACHE EVICTION ISSUES.");
            System.err.println("   Total unpinned memory: " + (report.unpinnedMemoryBytes / 1024.0 / 1024.0) + " MB");
          }
        }
      }
      
      // Final analysis
      System.err.println("\n========================================");
      System.err.println("FINAL STATE - After " + iterations + " iterations");
      System.err.println("========================================");
      PinCountDiagnostics.DiagnosticReport finalReport = 
          PinCountDiagnostics.scanBufferManager(bufferManager);
      System.err.println(finalReport);
      
      // Print verdict
      System.err.println("\n========================================");
      System.err.println("DIAGNOSTIC VERDICT");
      System.err.println("========================================");
      
      if (finalReport.pinnedPages > 0) {
        System.err.println("🔴 ROOT CAUSE: PINNING BUGS");
        System.err.println("   Pages remain pinned when they should be unpinned.");
        System.err.println("   Fix: Audit incrementPinCount/decrementPinCount pairs.");
        System.err.println("   Pinned pages: " + finalReport.pinnedPages);
        System.err.println("   Pinned memory: " + (finalReport.pinnedMemoryBytes / 1024.0 / 1024.0) + " MB");
        
        fail("Pinning bugs detected: " + finalReport.pinnedPages + " pages remain pinned");
      } else {
        System.err.println("✅ No pinned pages found - pinning logic appears correct");
        
        if (finalReport.unpinnedPages > initialReport.unpinnedPages + 50) {
          System.err.println("⚠️  POSSIBLE ISSUE: CACHE NOT EVICTING");
          System.err.println("   Many unpinned pages remain in cache.");
          System.err.println("   This may be normal cache behavior or weight calculation issue.");
          System.err.println("   Unpinned pages: " + finalReport.unpinnedPages);
          System.err.println("   Unpinned memory: " + (finalReport.unpinnedMemoryBytes / 1024.0 / 1024.0) + " MB");
        } else {
          System.err.println("✅ Cache eviction working normally");
        }
      }
      
      System.err.println("\nPin Count Distribution:");
      finalReport.pinCountDistribution.forEach((pinCount, numPages) -> 
          System.err.println("  " + numPages + " pages with pinCount=" + pinCount));
      
    } finally {
      System.clearProperty("sirix.debug.guard.tracking");
    }
  }
  
  /**
   * Test path summary operations which are known to have caching issues.
   */
  @Test
  public void testPathSummaryPinCounts() {
    System.setProperty("sirix.debug.guard.tracking", "true");
    System.setProperty("sirix.debug.path.summary", "true");
    
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
      
      // Create initial data with path summary
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
      System.err.println("PATH SUMMARY PIN COUNT TEST");
      System.err.println("========================================");
      
      // Open path summary reader multiple times
      for (int i = 0; i < 5; i++) {
        try (final var session = database.beginResourceSession("resource");
             final var pathSummary = session.openPathSummary()) {
          
          pathSummary.moveToDocumentRoot();
          var axis = new DescendantAxis(pathSummary);
          int pathNodes = 0;
          while (axis.hasNext()) {
            axis.nextLong();
            pathNodes++;
          }
          
          System.err.println("\nIteration " + (i + 1) + ": " + pathNodes + " path summary nodes");
        }
        
        PinCountDiagnostics.DiagnosticReport report = 
            PinCountDiagnostics.scanBufferManager(bufferManager);
        
        if (report.pinnedPages > 0) {
          System.err.println("⚠️  Iteration " + (i + 1) + ": " + 
                            report.pinnedPages + " pinned pages, " +
                            (report.pinnedMemoryBytes / 1024.0) + " KB");
        }
      }
      
      System.err.println("\n========================================");
      System.err.println("FINAL PATH SUMMARY STATE");
      System.err.println("========================================");
      PinCountDiagnostics.DiagnosticReport finalReport = 
          PinCountDiagnostics.scanBufferManager(bufferManager);
      System.err.println(finalReport);
      
      if (finalReport.pinnedPages > 0) {
        fail("Path summary test: " + finalReport.pinnedPages + " pages remain pinned");
      }
      
    } finally {
      System.clearProperty("sirix.debug.guard.tracking");
      System.clearProperty("sirix.debug.path.summary");
    }
  }
}

