package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Diagnostic utility to analyze pin count distribution and memory usage across cached pages.
 * Used to identify memory leaks caused by pinning bugs vs cache eviction bugs.
 */
public class PinCountDiagnostics {

  /**
   * Result of pin count analysis across all cached pages.
   */
  public static class DiagnosticReport {
    public final int totalPages;
    public final int pinnedPages;
    public final int unpinnedPages;
    public final long totalMemoryBytes;
    public final long pinnedMemoryBytes;
    public final long unpinnedMemoryBytes;
    public final Map<Integer, Integer> pinCountDistribution; // pinCount -> number of pages
    public final List<PagePinInfo> pagesWithPins; // Detailed info for pinned pages
    
    public DiagnosticReport(int totalPages, int pinnedPages, int unpinnedPages,
                           long totalMemoryBytes, long pinnedMemoryBytes, long unpinnedMemoryBytes,
                           Map<Integer, Integer> pinCountDistribution,
                           List<PagePinInfo> pagesWithPins) {
      this.totalPages = totalPages;
      this.pinnedPages = pinnedPages;
      this.unpinnedPages = unpinnedPages;
      this.totalMemoryBytes = totalMemoryBytes;
      this.pinnedMemoryBytes = pinnedMemoryBytes;
      this.unpinnedMemoryBytes = unpinnedMemoryBytes;
      this.pinCountDistribution = pinCountDistribution;
      this.pagesWithPins = pagesWithPins;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("=== Pin Count Diagnostic Report ===\n");
      sb.append(String.format("Total Pages: %d\n", totalPages));
      sb.append(String.format("Pinned Pages: %d (%.1f%%)\n", 
          pinnedPages, totalPages > 0 ? 100.0 * pinnedPages / totalPages : 0));
      sb.append(String.format("Unpinned Pages: %d (%.1f%%)\n",
          unpinnedPages, totalPages > 0 ? 100.0 * unpinnedPages / totalPages : 0));
      sb.append(String.format("Total Memory: %.2f MB\n", totalMemoryBytes / (1024.0 * 1024.0)));
      sb.append(String.format("Pinned Memory: %.2f MB (%.1f%%)\n",
          pinnedMemoryBytes / (1024.0 * 1024.0),
          totalMemoryBytes > 0 ? 100.0 * pinnedMemoryBytes / totalMemoryBytes : 0));
      sb.append(String.format("Unpinned Memory: %.2f MB (%.1f%%)\n",
          unpinnedMemoryBytes / (1024.0 * 1024.0),
          totalMemoryBytes > 0 ? 100.0 * unpinnedMemoryBytes / totalMemoryBytes : 0));
      
      sb.append("\nPin Count Distribution:\n");
      pinCountDistribution.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(e -> sb.append(String.format("  Pin Count %d: %d pages\n", e.getKey(), e.getValue())));
      
      if (!pagesWithPins.isEmpty()) {
        sb.append(String.format("\nTop Pinned Pages (showing %d):\n", Math.min(20, pagesWithPins.size())));
        pagesWithPins.stream()
            .sorted(Comparator.comparingInt(p -> -p.totalPinCount))
            .limit(20)
            .forEach(p -> sb.append(String.format("  Page %d (rev %d, type %s): pinCount=%d, memory=%.2f KB, transactions=%s\n",
                p.pageKey, p.revision, p.indexType, p.totalPinCount,
                p.memoryBytes / 1024.0, p.pinnedByTransactions)));
      }
      
      return sb.toString();
    }
  }
  
  /**
   * Information about a single pinned page.
   */
  public static class PagePinInfo {
    public final long pageKey;
    public final int revision;
    public final String indexType;
    public final int totalPinCount;
    public final long memoryBytes;
    public final Map<Integer, Integer> pinnedByTransactions; // trxId -> pinCount
    
    public PagePinInfo(long pageKey, int revision, String indexType, int totalPinCount,
                      long memoryBytes, Map<Integer, Integer> pinnedByTransactions) {
      this.pageKey = pageKey;
      this.revision = revision;
      this.indexType = indexType;
      this.totalPinCount = totalPinCount;
      this.memoryBytes = memoryBytes;
      this.pinnedByTransactions = pinnedByTransactions;
    }
  }
  
  /**
   * Scan a cache and generate diagnostic report.
   */
  public static DiagnosticReport scanCache(Cache<PageReference, ? extends Page> cache) {
    ConcurrentMap<PageReference, ? extends Page> map = cache.asMap();
    
    int totalPages = 0;
    int pinnedPages = 0;
    int unpinnedPages = 0;
    long totalMemoryBytes = 0;
    long pinnedMemoryBytes = 0;
    long unpinnedMemoryBytes = 0;
    Map<Integer, Integer> pinCountDistribution = new HashMap<>();
    List<PagePinInfo> pagesWithPins = new ArrayList<>();
    
    for (Map.Entry<PageReference, ? extends Page> entry : map.entrySet()) {
      Page page = entry.getValue();
      if (!(page instanceof KeyValueLeafPage kvPage)) {
        continue; // Only analyze KeyValueLeafPages
      }
      
      totalPages++;
      int pinCount = kvPage.getPinCount();
      long memoryBytes = kvPage.getActualMemorySize();
      
      totalMemoryBytes += memoryBytes;
      
      if (pinCount > 0) {
        pinnedPages++;
        pinnedMemoryBytes += memoryBytes;
        
        Map<Integer, Integer> pinsByTrx = kvPage.getPinCountByTransaction();
        pagesWithPins.add(new PagePinInfo(
            kvPage.getPageKey(),
            kvPage.getRevision(),
            kvPage.getIndexType() != null ? kvPage.getIndexType().toString() : "UNKNOWN",
            pinCount,
            memoryBytes,
            pinsByTrx
        ));
      } else {
        unpinnedPages++;
        unpinnedMemoryBytes += memoryBytes;
      }
      
      pinCountDistribution.merge(pinCount, 1, Integer::sum);
    }
    
    return new DiagnosticReport(
        totalPages, pinnedPages, unpinnedPages,
        totalMemoryBytes, pinnedMemoryBytes, unpinnedMemoryBytes,
        pinCountDistribution, pagesWithPins
    );
  }
  
  /**
   * Scan buffer manager's record page cache, fragment cache, AND page cache.
   */
  public static DiagnosticReport scanBufferManager(BufferManager bufferManager) {
    DiagnosticReport recordPageReport = scanCache(bufferManager.getRecordPageCache());
    DiagnosticReport fragmentReport = scanCache(bufferManager.getRecordPageFragmentCache());
    DiagnosticReport pageReport = scanCache(bufferManager.getPageCache());
    
    // Combine reports from all 3 caches
    Map<Integer, Integer> combinedDistribution = new HashMap<>(recordPageReport.pinCountDistribution);
    fragmentReport.pinCountDistribution.forEach((k, v) -> 
        combinedDistribution.merge(k, v, Integer::sum));
    pageReport.pinCountDistribution.forEach((k, v) -> 
        combinedDistribution.merge(k, v, Integer::sum));
    
    List<PagePinInfo> combinedPins = new ArrayList<>(recordPageReport.pagesWithPins);
    combinedPins.addAll(fragmentReport.pagesWithPins);
    combinedPins.addAll(pageReport.pagesWithPins);
    
    return new DiagnosticReport(
        recordPageReport.totalPages + fragmentReport.totalPages + pageReport.totalPages,
        recordPageReport.pinnedPages + fragmentReport.pinnedPages + pageReport.pinnedPages,
        recordPageReport.unpinnedPages + fragmentReport.unpinnedPages + pageReport.unpinnedPages,
        recordPageReport.totalMemoryBytes + fragmentReport.totalMemoryBytes + pageReport.totalMemoryBytes,
        recordPageReport.pinnedMemoryBytes + fragmentReport.pinnedMemoryBytes + pageReport.pinnedMemoryBytes,
        recordPageReport.unpinnedMemoryBytes + fragmentReport.unpinnedMemoryBytes + pageReport.unpinnedMemoryBytes,
        combinedDistribution,
        combinedPins
    );
  }
  
  /**
   * Print diagnostic report to System.err if debug flag is enabled.
   */
  public static void printIfEnabled(String label, DiagnosticReport report) {
    if (Boolean.getBoolean("sirix.debug.pin.counts")) {
      System.err.println("\n" + label);
      System.err.println(report.toString());
    }
  }
  
  /**
   * Check for leaked pins when a transaction closes.
   * Returns list of pages that this transaction still has pinned.
   */
  public static List<PagePinInfo> checkTransactionPins(BufferManager bufferManager, int trxId) {
    List<PagePinInfo> leakedPins = new ArrayList<>();
    
    for (Page page : bufferManager.getRecordPageCache().asMap().values()) {
      if (page instanceof KeyValueLeafPage kvPage) {
        Map<Integer, Integer> pinsByTrx = kvPage.getPinCountByTransaction();
        Integer pinCount = pinsByTrx.get(trxId);
        if (pinCount != null && pinCount > 0) {
          leakedPins.add(new PagePinInfo(
              kvPage.getPageKey(),
              kvPage.getRevision(),
              kvPage.getIndexType() != null ? kvPage.getIndexType().toString() : "UNKNOWN",
              pinCount,
              kvPage.getActualMemorySize(),
              Map.of(trxId, pinCount)
          ));
        }
      }
    }
    
    for (Page page : bufferManager.getRecordPageFragmentCache().asMap().values()) {
      if (page instanceof KeyValueLeafPage kvPage) {
        Map<Integer, Integer> pinsByTrx = kvPage.getPinCountByTransaction();
        Integer pinCount = pinsByTrx.get(trxId);
        if (pinCount != null && pinCount > 0) {
          leakedPins.add(new PagePinInfo(
              kvPage.getPageKey(),
              kvPage.getRevision(),
              kvPage.getIndexType() != null ? kvPage.getIndexType().toString() : "FRAGMENT",
              pinCount,
              kvPage.getActualMemorySize(),
              Map.of(trxId, pinCount)
          ));
        }
      }
    }
    
    // CRITICAL: Also check PageCache!
    for (Page page : bufferManager.getPageCache().asMap().values()) {
      if (page instanceof KeyValueLeafPage kvPage) {
        Map<Integer, Integer> pinsByTrx = kvPage.getPinCountByTransaction();
        Integer pinCount = pinsByTrx.get(trxId);
        if (pinCount != null && pinCount > 0) {
          leakedPins.add(new PagePinInfo(
              kvPage.getPageKey(),
              kvPage.getRevision(),
              kvPage.getIndexType() != null ? kvPage.getIndexType().toString() : "PAGE_CACHE",
              pinCount,
              kvPage.getActualMemorySize(),
              Map.of(trxId, pinCount)
          ));
        }
      }
    }
    
    return leakedPins;
  }
  
  /**
   * Warn if transaction is closing with pinned pages.
   */
  public static void warnOnTransactionClose(BufferManager bufferManager, int trxId) {
    if (!Boolean.getBoolean("sirix.debug.pin.counts")) {
      return;
    }
    
    List<PagePinInfo> leakedPins = checkTransactionPins(bufferManager, trxId);
    if (!leakedPins.isEmpty()) {
      System.err.println("\n⚠️  WARNING: Transaction " + trxId + " closing with " + leakedPins.size() + " pinned pages!");
      long totalMemory = leakedPins.stream().mapToLong(p -> p.memoryBytes).sum();
      System.err.println("    Total leaked memory: " + (totalMemory / 1024.0 / 1024.0) + " MB");
      leakedPins.stream()
          .limit(10)
          .forEach(p -> System.err.println(String.format("    - Page %d (rev %d, type %s): pinCount=%d",
              p.pageKey, p.revision, p.indexType, p.totalPinCount)));
      if (leakedPins.size() > 10) {
        System.err.println("    ... and " + (leakedPins.size() - 10) + " more");
      }
    }
  }
}

