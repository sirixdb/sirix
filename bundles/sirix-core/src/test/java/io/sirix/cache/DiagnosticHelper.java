package io.sirix.cache;

import java.util.Map;

/**
 * Helper class to print diagnostic information about memory segment allocation.
 * 
 * All diagnostic output is written to memory-leak-diagnostic.log in the project root.
 * 
 * Usage in your tests:
 * <pre>
 * // Before test
 * DiagnosticHelper.logTestStart("MyTest");
 * DiagnosticHelper.printPoolStatistics();
 * 
 * // Run your test
 * // ...
 * 
 * // After test
 * DiagnosticHelper.printPoolStatistics();
 * DiagnosticHelper.printMemorySegmentPools();
 * DiagnosticHelper.logTestEnd("MyTest");
 * 
 * // View the log
 * System.out.println("Diagnostic log: " + DiagnosticLogger.getLogFilePath());
 * </pre>
 */
public class DiagnosticHelper {
    
    /**
     * Log the start of a test.
     */
    public static void logTestStart(String testName) {
        DiagnosticLogger.separator("TEST START: " + testName);
    }
    
    /**
     * Log the end of a test.
     */
    public static void logTestEnd(String testName) {
        DiagnosticLogger.separator("TEST END: " + testName);
        DiagnosticLogger.log("Log file location: " + DiagnosticLogger.getLogFilePath());
    }
    
    /**
     * Print statistics about page borrowing/returning to the diagnostic log.
     * Shows if pages are being leaked (borrowed but not returned).
     */
    public static void printPoolStatistics() {
        DiagnosticLogger.separator("POOL STATISTICS");
        
        // KeyValueLeafPagePool statistics
        var pagePoolStats = KeyValueLeafPagePool.getInstance().getDetailedStatistics();
        long borrowed = (Long) pagePoolStats.get("totalBorrowedPages");
        long returned = (Long) pagePoolStats.get("totalReturnedPages");
        long leaked = borrowed - returned;
        
        DiagnosticLogger.log("KeyValueLeafPagePool:");
        DiagnosticLogger.log("  Total borrowed pages: " + borrowed);
        DiagnosticLogger.log("  Total returned pages: " + returned);
        DiagnosticLogger.log("  Difference (leaked):  " + leaked + (leaked > 0 ? " ⚠️  LEAK!" : " ✓"));
        
        // LinuxMemorySegmentAllocator statistics
        var allocator = LinuxMemorySegmentAllocator.getInstance();
        DiagnosticLogger.log("LinuxMemorySegmentAllocator:");
        DiagnosticLogger.log("  Max buffer size: " + allocator.getMaxBufferSize() + " bytes");
        DiagnosticLogger.separator("END STATISTICS");
        
        // Also print to console
        System.out.println("Pool statistics written to: " + DiagnosticLogger.getLogFilePath());
        System.out.println("Borrowed: " + borrowed + ", Returned: " + returned + ", Leaked: " + leaked);
    }
    
    /**
     * Print current state of memory segment pools to the diagnostic log.
     * Shows how many segments are available in each pool.
     */
    public static void printMemorySegmentPools() {
        DiagnosticLogger.separator("MEMORY SEGMENT POOLS");
        var allocator = LinuxMemorySegmentAllocator.getInstance();
        int[] poolSizes = allocator.getPoolSizes();
        long[] segmentSizes = {4096, 8192, 16384, 32768, 65536, 131072, 262144};
        
        for (int i = 0; i < poolSizes.length; i++) {
            DiagnosticLogger.log("Pool " + i + " (" + segmentSizes[i] + " bytes): " + 
                             poolSizes[i] + " segments available");
        }
        
        DiagnosticLogger.separator("END POOLS");
        
        // Also print summary to console
        System.out.println("Pool sizes written to: " + DiagnosticLogger.getLogFilePath());
        System.out.println("Pool 4 (65536): " + poolSizes[4] + ", Pool 5 (131072): " + poolSizes[5]);
    }
    
    /**
     * Print diagnostic information using LinuxMemorySegmentAllocator's built-in method.
     */
    public static void printAllocatorDiagnostics() {
        LinuxMemorySegmentAllocator.getInstance().printPoolDiagnostics();
    }
}

