/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.api;

/** Read-cache policy only; none of these choices changes persisted pages or versioning. */
public enum HOTReadIntent {
  /** An explicit point lookup may grow a resolved-record mini page. */
  POINT,
  /**
   * Metadata and other non-admitting lookups may resolve one slot and reuse existing mini entries,
   * without growing a mini page on behalf of a possible range scan.
   */
  SELECTIVE,
  /** A scan needs complete leaves, through the ordinary guarded loader. */
  SCAN
}
