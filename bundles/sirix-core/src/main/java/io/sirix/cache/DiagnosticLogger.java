package io.sirix.cache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Thread-safe diagnostic logger that writes to a file.
 * All diagnostic messages are written to memory-leak-diagnostic.log in the project root.
 */
public class DiagnosticLogger {
    
    // Use absolute path so we know exactly where the file is
    private static final Path LOG_FILE = Paths.get(System.getProperty("user.home"), 
                                                    "IdeaProjects", "sirix", 
                                                    "memory-leak-diagnostic.log");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final Object LOCK = new Object();
    
    static {
        // Clear the log file at startup
        try {
            System.err.println("[DiagnosticLogger] Initializing... LOG_FILE=" + LOG_FILE);
            System.err.println("[DiagnosticLogger] user.home=" + System.getProperty("user.home"));
            
            // Create parent directories if needed
            if (LOG_FILE.getParent() != null) {
                boolean created = LOG_FILE.getParent().toFile().mkdirs();
                System.err.println("[DiagnosticLogger] Parent dirs created/exist: " + created);
            }
            
            try (PrintWriter writer = new PrintWriter(LOG_FILE.toFile())) {
                writer.println("=== Memory Leak Diagnostic Log Started at " + LocalDateTime.now() + " ===");
                writer.println("=== Log file: " + LOG_FILE.toAbsolutePath() + " ===");
                writer.println();
                writer.flush();
            }
            System.err.println("[DiagnosticLogger] SUCCESS: Logging initialized at " + LOG_FILE.toAbsolutePath());
            System.out.println("[DiagnosticLogger] Log file: " + LOG_FILE.toAbsolutePath());
        } catch (Throwable e) {
            System.err.println("[DiagnosticLogger] FAILED to initialize log file: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
    
    /**
     * Log a diagnostic message with timestamp and thread info.
     */
    public static void log(String message) {
        synchronized (LOCK) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE.toFile(), true))) {
                String timestamp = LocalDateTime.now().format(TIME_FORMAT);
                String threadName = Thread.currentThread().getName();
                writer.write(String.format("[%s] [%s] %s%n", timestamp, threadName, message));
            } catch (IOException e) {
                System.err.println("Failed to write to diagnostic log: " + e.getMessage());
            }
        }
    }
    
    /**
     * Log an error with exception details.
     */
    public static void error(String message, Throwable e) {
        synchronized (LOCK) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(LOG_FILE.toFile(), true))) {
                String timestamp = LocalDateTime.now().format(TIME_FORMAT);
                String threadName = Thread.currentThread().getName();
                writer.printf("[%s] [%s] %s%n", timestamp, threadName, message);
                if (e != null) {
                    e.printStackTrace(writer);
                }
            } catch (IOException ioEx) {
                System.err.println("Failed to write error to diagnostic log: " + ioEx.getMessage());
            }
        }
    }
    
    /**
     * Log a separator line for readability.
     */
    public static void separator(String label) {
        log("========== " + label + " ==========");
    }
    
    /**
     * Flush and close the log (optional - files are auto-flushed).
     */
    public static void flush() {
        // Files are auto-flushed with try-with-resources
    }
    
    /**
     * Get the path to the diagnostic log file.
     */
    public static Path getLogFilePath() {
        return LOG_FILE.toAbsolutePath();
    }
}

