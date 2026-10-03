/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.api;

/** Read-cache policy only; none of these choices changes persisted pages or versioning. */
public enum HOTReadIntent {
  /** An explicit point lookup may grow a resolved-record mini page. */
  POINT,
  /** A scan needs complete leaves, through the ordinary guarded loader. */
  SCAN
}
