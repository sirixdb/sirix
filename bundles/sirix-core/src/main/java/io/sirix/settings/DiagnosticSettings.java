/*
 * Copyright (c) 2024, SirixDB
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *   * Redistributions of source code must retain the above copyright notice, this list of
 *     conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice, this list of
 *     conditions and the following disclaimer in the documentation and/or other materials
 *     provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 */

package io.sirix.settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized diagnostic and debugging settings for SirixDB.
 * <p>
 * All diagnostic flags should be accessed through this class to ensure consistent
 * configuration and logging. Diagnostic features are disabled by default in production
 * and can be enabled via system properties for troubleshooting.
 * <p>
 * <b>Available System Properties:</b>
 * <ul>
 *   <li>{@code sirix.debug.memory.leaks} - Enable memory leak tracking and detection</li>
 *   <li>{@code sirix.debug.path.summary} - Enable path summary cache debugging</li>
 *   <li>{@code sirix.debug.guard.tracking} - Enable guard acquisition/release tracking</li>
 *   <li>{@code sirix.debug.cache.stats} - Enable cache statistics logging</li>
 * </ul>
 * <p>
 * <b>Example Usage:</b>
 * <pre>{@code
 * // Enable memory leak debugging
 * java -Dsirix.debug.memory.leaks=true -jar sirix.jar
 *
 * // In code:
 * if (DiagnosticSettings.isMemoryLeakTrackingEnabled()) {
 *     LOGGER.debug("Tracking page lifecycle: {}", page);
 * }
 * }</pre>
 *
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
public final class DiagnosticSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiagnosticSettings.class);

  // ============ Diagnostic Flags (read from system properties at startup) ============

  /**
   * Enable detailed memory leak tracking and page lifecycle monitoring.
   * <p>
   * When enabled, tracks creation and closure of all KeyValueLeafPage instances,
   * captures stack traces for leak diagnosis, and logs warnings when pages are
   * garbage collected without being properly closed.
   * <p>
   * <b>Performance Impact:</b> Significant overhead due to stack trace capture.
   * Use only for debugging.
   * <p>
   * <b>System Property:</b> {@code sirix.debug.memory.leaks}
   */
  public static final boolean MEMORY_LEAK_TRACKING = 
      Boolean.getBoolean("sirix.debug.memory.leaks");

  /**
   * Enable path summary cache debugging.
   * <p>
   * When enabled, logs detailed information about path summary page lookups,
   * cache hits/misses, and cache bypass decisions for write transactions.
   * <p>
   * <b>Performance Impact:</b> Moderate overhead due to logging.
   * <p>
   * <b>System Property:</b> {@code sirix.debug.path.summary}
   */
  public static final boolean PATH_SUMMARY_DEBUG = 
      Boolean.getBoolean("sirix.debug.path.summary");

  /**
   * Enable guard (page locking) tracking.
   * <p>
   * When enabled, logs detailed information about guard acquisition and release
   * operations, helping diagnose guard leaks and concurrency issues.
   * <p>
   * <b>Performance Impact:</b> Moderate overhead.
   * <p>
   * <b>System Property:</b> {@code sirix.debug.guard.tracking}
   */
  public static final boolean GUARD_TRACKING = 
      Boolean.getBoolean("sirix.debug.guard.tracking");

  /**
   * Enable cache statistics logging.
   * <p>
   * When enabled, periodically logs cache hit/miss ratios, eviction counts,
   * and memory usage for all caches.
   * <p>
   * <b>Performance Impact:</b> Low overhead.
   * <p>
   * <b>System Property:</b> {@code sirix.debug.cache.stats}
   */
  public static final boolean CACHE_STATISTICS = 
      Boolean.getBoolean("sirix.debug.cache.stats");

  static {
    // Log active diagnostic settings on startup
    if (MEMORY_LEAK_TRACKING || PATH_SUMMARY_DEBUG || GUARD_TRACKING || CACHE_STATISTICS) {
      LOGGER.info("SirixDB Diagnostic Settings Active:");
      if (MEMORY_LEAK_TRACKING) {
        LOGGER.info("  - Memory leak tracking ENABLED (sirix.debug.memory.leaks)");
      }
      if (PATH_SUMMARY_DEBUG) {
        LOGGER.info("  - Path summary debugging ENABLED (sirix.debug.path.summary)");
      }
      if (GUARD_TRACKING) {
        LOGGER.info("  - Guard tracking ENABLED (sirix.debug.guard.tracking)");
      }
      if (CACHE_STATISTICS) {
        LOGGER.info("  - Cache statistics ENABLED (sirix.debug.cache.stats)");
      }
      LOGGER.info("Diagnostic features add overhead. Disable for production use.");
    }
  }

  // ============ Convenience Methods ============

  /**
   * Check if memory leak tracking is enabled.
   *
   * @return true if memory leak tracking is active
   */
  public static boolean isMemoryLeakTrackingEnabled() {
    return MEMORY_LEAK_TRACKING;
  }

  /**
   * Check if path summary debugging is enabled.
   *
   * @return true if path summary debugging is active
   */
  public static boolean isPathSummaryDebugEnabled() {
    return PATH_SUMMARY_DEBUG;
  }

  /**
   * Check if guard tracking is enabled.
   *
   * @return true if guard tracking is active
   */
  public static boolean isGuardTrackingEnabled() {
    return GUARD_TRACKING;
  }

  /**
   * Check if cache statistics logging is enabled.
   *
   * @return true if cache statistics are active
   */
  public static boolean isCacheStatisticsEnabled() {
    return CACHE_STATISTICS;
  }

  /**
   * Check if any diagnostic feature is enabled.
   *
   * @return true if any diagnostic feature is active
   */
  public static boolean isAnyDiagnosticEnabled() {
    return MEMORY_LEAK_TRACKING || PATH_SUMMARY_DEBUG || GUARD_TRACKING || CACHE_STATISTICS;
  }

  // Private constructor to prevent instantiation
  private DiagnosticSettings() {
    throw new AssertionError("Utility class - do not instantiate");
  }
}



