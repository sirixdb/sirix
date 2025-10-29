package io.sirix.cache;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.xml.shredder.XmlShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

/**
 * Specific test to track down the 12 NAME Page 0 finalizer leaks.
 */
public class NamePage0LeakTest {

  private static final String XMLFILE = "10mb.xml";
  private static final Path XML = Paths.get("src", "test", "resources", XMLFILE);
  
  private Holder holder;
  
  @Before
  public void setUp() throws Exception {
    KeyValueLeafPage.PAGES_CREATED.set(0);
    KeyValueLeafPage.PAGES_CLOSED.set(0);
    KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.set(0);
    KeyValueLeafPage.ALL_LIVE_PAGES.clear();
    
    XmlTestHelper.deleteEverything();
    
    System.err.println("\n=== BEFORE XmlShredder.main() ===");
    XmlShredder.main(XML.toAbsolutePath().toString(), XmlTestHelper.PATHS.PATH1.getFile().toAbsolutePath().toString());
    
    long afterShredding = KeyValueLeafPage.PAGES_CREATED.get();
    long closedAfterShredding = KeyValueLeafPage.PAGES_CLOSED.get();
    System.err.println("\n=== AFTER XmlShredder.main() ===");
    System.err.println("Created: " + afterShredding);
    System.err.println("Closed: " + closedAfterShredding);
    System.err.println("Live: " + KeyValueLeafPage.ALL_LIVE_PAGES.size());
    
    // Force GC to see if any are finalized
    for (int i = 0; i < 3; i++) {
      System.gc();
      try { Thread.sleep(50); } catch (InterruptedException e) {}
    }
    
    long finalized1 = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
    System.err.println("Finalized after shredding: " + finalized1);
    
    System.err.println("\n=== Opening Holder (new transaction) ===");
    holder = Holder.generateRtx();
    
    long afterHolder = KeyValueLeafPage.PAGES_CREATED.get();
    long closedAfterHolder = KeyValueLeafPage.PAGES_CLOSED.get();
    System.err.println("Created: " + afterHolder);
    System.err.println("Closed: " + closedAfterHolder);
    System.err.println("Live: " + KeyValueLeafPage.ALL_LIVE_PAGES.size());
  }
  
  @After
  public void tearDown() {
    if (holder != null) {
      holder.close();
    }
    XmlTestHelper.closeEverything();
    
    System.err.println("\n=== AFTER tearDown() ===");
    System.err.println("Created: " + KeyValueLeafPage.PAGES_CREATED.get());
    System.err.println("Closed: " + KeyValueLeafPage.PAGES_CLOSED.get());
    System.err.println("Live: " + KeyValueLeafPage.ALL_LIVE_PAGES.size());
    
    // Check cache contents
    try {
      var bufMgr = holder.getXmlNodeReadTrx().getPageTrx().getBufferManager();
      
      long inRecordCache = KeyValueLeafPage.ALL_LIVE_PAGES.stream()
          .filter(p -> bufMgr.getRecordPageCache().asMap().containsValue(p))
          .count();
      long inFragmentCache = KeyValueLeafPage.ALL_LIVE_PAGES.stream()
          .filter(p -> bufMgr.getRecordPageFragmentCache().asMap().containsValue(p))
          .count();
      long inPageCache = KeyValueLeafPage.ALL_LIVE_PAGES.stream()
          .filter(p -> bufMgr.getPageCache().asMap().containsValue(p))
          .count();
      
      System.err.println("In RecordPageCache: " + inRecordCache);
      System.err.println("In RecordPageFragmentCache: " + inFragmentCache);
      System.err.println("In PageCache: " + inPageCache);
      System.err.println("Not in any cache (swizzled only): " + 
          (KeyValueLeafPage.ALL_LIVE_PAGES.size() - inRecordCache - inFragmentCache - inPageCache));
      
      // Check specifically for NAME Page 0
      long namePage0Count = KeyValueLeafPage.ALL_LIVE_PAGES.stream()
          .filter(p -> p.getPageKey() == 0)
          .filter(p -> p.getIndexType() == io.sirix.index.IndexType.NAME)
          .count();
      
      System.err.println("\nNAME Page 0 instances still live: " + namePage0Count);
      
      KeyValueLeafPage.ALL_LIVE_PAGES.stream()
          .filter(p -> p.getPageKey() == 0)
          .filter(p -> p.getIndexType() == io.sirix.index.IndexType.NAME)
          .forEach(p -> System.err.println("  Page 0 NAME (rev " + p.getRevision() + 
                                          "): pinCount=" + p.getPinCount() +
                                          ", closed=" + p.isClosed() +
                                          ", pinByTrx=" + p.getPinCountByTransaction()));
    } catch (Exception ex) {
      System.err.println("(Cache analysis skipped - transaction closed)");
    }
    
    // Force GC to trigger finalizer
    for (int i = 0; i < 5; i++) {
      System.gc();
      try { Thread.sleep(100); } catch (InterruptedException e) {}
    }
    
    long finalized = KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
    System.err.println("\nFinalized by GC: " + finalized);
  }
  
  @Test
  public void testTrackNamePage0Lifecycle() {
    System.setProperty("sirix.debug.memory.leaks", "true");
    
    try {
      // Just run a simple query
      var axis = new io.sirix.axis.DescendantAxis(holder.getXmlNodeReadTrx());
      int count = 0;
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }
      
      System.err.println("\n=== DURING TEST (scanned " + count + " nodes) ===");
      System.err.println("Created: " + KeyValueLeafPage.PAGES_CREATED.get());
      System.err.println("Closed: " + KeyValueLeafPage.PAGES_CLOSED.get());
      System.err.println("Live: " + KeyValueLeafPage.ALL_LIVE_PAGES.size());
      
    } finally {
      System.clearProperty("sirix.debug.memory.leaks");
    }
  }
}

