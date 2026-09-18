/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Borrowed page input is the default on every runtime: an unset option resolves to borrowing, an
 * explicit {@code false} (in any case, surrounding blanks ignored) restores the owned-buffer path,
 * and any other explicit value keeps borrowing. The resolution is the same code whether the reader
 * runs on a JVM or in a native image.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class BorrowedInputDefaultTest {

  private static final String OPTION = "sirix.io.borrowedInputDefaultTest.option";

  @Test
  void anUnsetOptionResolvesToBorrowing() {
    System.clearProperty(OPTION);
    assertTrue(AbstractReader.borrowedInputEnabled(OPTION));
  }

  @Test
  void anExplicitFalseInAnyCaseRestoresOwnedInputAndAnythingElseKeepsBorrowing() {
    try {
      System.setProperty(OPTION, "false");
      assertFalse(AbstractReader.borrowedInputEnabled(OPTION));
      System.setProperty(OPTION, "FALSE");
      assertFalse(AbstractReader.borrowedInputEnabled(OPTION));
      System.setProperty(OPTION, " False ");
      assertFalse(AbstractReader.borrowedInputEnabled(OPTION));
      System.setProperty(OPTION, "true");
      assertTrue(AbstractReader.borrowedInputEnabled(OPTION));
      System.setProperty(OPTION, "");
      assertTrue(AbstractReader.borrowedInputEnabled(OPTION));
    } finally {
      System.clearProperty(OPTION);
    }
  }
}
